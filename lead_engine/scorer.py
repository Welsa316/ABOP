"""
scorer.py — Lead scoring engine.

Assigns a weighted score to each business based on website analysis,
business metadata, and category-specific checks.  Every point that
contributes to the total is recorded in a human-readable breakdown.
"""

import logging
from .config import (
    SCORE_WEIGHTS as W,
    REVIEW_THRESHOLDS,
    RATING_THRESHOLDS,
    CHAIN_KEYWORDS,
    RESTAURANT_KEYWORDS,
    SERVICE_KEYWORDS,
)
from .analyzer import SiteAnalysis
from .utils import normalize_text

logger = logging.getLogger("lead_engine")


def _is_chain(name: str) -> bool:
    """Heuristic: check if a business name looks like a major chain."""
    low = normalize_text(name)
    return any(kw in low for kw in CHAIN_KEYWORDS)


def _is_restaurant(categories: list[str]) -> bool:
    """Does the business look like a restaurant / food place?"""
    cats = " ".join(categories).lower()
    return any(kw in cats for kw in RESTAURANT_KEYWORDS)


def _is_service(categories: list[str]) -> bool:
    """Does the business look like a service provider?"""
    cats = " ".join(categories).lower()
    return any(kw in cats for kw in SERVICE_KEYWORDS)


def score_business(biz: dict, analysis: SiteAnalysis | None) -> dict:
    """
    Score a single business.

    Returns a dict with:
      - total_score (int)
      - breakdown   (list of {"reason": str, "points": int})
      - pitch_angle (str) — suggested outreach angle
    """
    breakdown: list[dict] = []
    pitch_angles: list[str] = []

    def add(reason: str, key: str) -> None:
        pts = W.get(key, 0)
        if pts:
            breakdown.append({"reason": reason, "points": pts})

    has_website = bool(biz.get("website"))
    categories = biz.get("categories", [])
    is_rest = _is_restaurant(categories)
    is_svc = _is_service(categories)

    # ------------------------------------------------------------------
    # 1. Website existence / reachability
    # ------------------------------------------------------------------
    if not has_website:
        add("No website listed", "no_website")
        pitch_angles.append("needs_new_website")
    elif analysis:
        if analysis.is_social_only:
            add("Only a social media page (no real website)", "social_media_only")
            pitch_angles.append("needs_new_website")
        elif not analysis.reachable:
            add("Website is unreachable / broken", "site_unreachable")
            pitch_angles.append("site_broken")

    # ------------------------------------------------------------------
    # 2. Technical quality (only if site was reachable)
    # ------------------------------------------------------------------
    if has_website and analysis and analysis.reachable:
        if not analysis.has_ssl:
            add("No HTTPS / SSL", "no_ssl")
            pitch_angles.append("security_upgrade")

        if not analysis.has_viewport:
            add("No mobile viewport meta tag", "no_viewport")
            pitch_angles.append("mobile_improvement")

        if analysis.is_slow:
            add(f"Slow response ({analysis.response_time}s)", "slow_response")
            pitch_angles.append("speed_improvement")

        if analysis.is_thin_content:
            add("Very thin / minimal content", "thin_content")
            pitch_angles.append("content_improvement")

        if analysis.is_placeholder:
            add("Looks like a placeholder / parked page", "placeholder_site")
            pitch_angles.append("needs_new_website")

        if analysis.is_outdated_design:
            add("Outdated HTML (tables/frames/old tags)", "outdated_design")
            pitch_angles.append("redesign")

        if not analysis.has_contact_info:
            add("No visible contact info on homepage", "no_contact_info")
            pitch_angles.append("contact_improvement")

        if not analysis.has_cta:
            add("No clear call-to-action", "no_cta")
            pitch_angles.append("cta_improvement")

        # Category-specific UX checks
        if is_rest:
            if not analysis.has_menu_link:
                add("Restaurant without menu link", "no_menu")
                pitch_angles.append("add_menu")
            if not analysis.has_ordering_link:
                add("Restaurant without online ordering", "no_online_ordering")
                pitch_angles.append("add_ordering")

        if is_svc:
            if not analysis.has_booking_link:
                add("Service business without booking option", "no_booking")
                pitch_angles.append("add_booking")

        # Check if website is actually strong — penalise (low priority)
        issue_count = len([b for b in breakdown if b["points"] > 0])
        if issue_count == 0:
            add("Website looks modern and complete", "strong_website_penalty")
            pitch_angles.append("low_priority")

    # ------------------------------------------------------------------
    # 3. Business attractiveness bonuses
    # ------------------------------------------------------------------
    review_count = biz.get("review_count", 0)
    rating = biz.get("rating", 0.0)

    if review_count >= REVIEW_THRESHOLDS["very_high"]:
        add(f"{review_count} reviews (very popular)", "very_high_reviews_bonus")
    elif review_count >= REVIEW_THRESHOLDS["high"]:
        add(f"{review_count} reviews (popular)", "high_reviews_bonus")

    if rating >= RATING_THRESHOLDS["excellent"]:
        add(f"{rating} stars (excellent rating)", "excellent_rating_bonus")
    elif rating >= RATING_THRESHOLDS["good"]:
        add(f"{rating} stars (good rating)", "good_rating_bonus")

    # ------------------------------------------------------------------
    # 4. Chain penalty
    # ------------------------------------------------------------------
    if _is_chain(biz.get("business_name", "")):
        add("Suspected chain / franchise", "chain_penalty")
        pitch_angles.append("skip_chain")

    # ------------------------------------------------------------------
    # Compute total
    # ------------------------------------------------------------------
    total = sum(item["points"] for item in breakdown)
    total = max(total, 0)  # floor at 0

    # Pick primary pitch angle
    # Priority order: needs_new_website > site_broken > redesign > mobile > others
    angle_priority = [
        "needs_new_website", "site_broken", "redesign",
        "mobile_improvement", "speed_improvement", "cta_improvement",
        "contact_improvement", "content_improvement", "security_upgrade",
        "add_menu", "add_ordering", "add_booking", "low_priority",
        "skip_chain",
    ]
    primary_angle = "general_improvement"
    for a in angle_priority:
        if a in pitch_angles:
            primary_angle = a
            break

    return {
        "total_score": total,
        "breakdown": breakdown,
        "pitch_angle": primary_angle,
        "all_pitch_angles": list(dict.fromkeys(pitch_angles)),  # unique, ordered
    }


def score_all(businesses: list[dict],
              analyses: dict[int, SiteAnalysis]) -> list[dict]:
    """
    Score every business and attach results.

    Returns the same list of business dicts, each augmented with:
      lead_score, score_breakdown, pitch_angle, all_pitch_angles
    """
    for i, biz in enumerate(businesses):
        analysis = analyses.get(i)
        result = score_business(biz, analysis)
        biz["lead_score"] = result["total_score"]
        biz["score_breakdown"] = result["breakdown"]
        biz["pitch_angle"] = result["pitch_angle"]
        biz["all_pitch_angles"] = result["all_pitch_angles"]

        # Attach analysis summary too
        if analysis:
            biz["website_status"] = (
                "reachable" if analysis.reachable
                else ("social_only" if analysis.is_social_only
                      else ("unreachable" if biz.get("website") else "none"))
            )
            biz["detected_issues"] = analysis.detected_issues
            biz["site_title"] = analysis.title
            biz["final_url"] = analysis.final_url
            biz["response_time"] = analysis.response_time
        else:
            biz["website_status"] = "none" if not biz.get("website") else "not_checked"
            biz["detected_issues"] = []
            biz["site_title"] = ""
            biz["final_url"] = ""
            biz["response_time"] = 0

    # Sort by lead_score descending
    businesses.sort(key=lambda b: b["lead_score"], reverse=True)
    logger.info("Scoring complete. Top score=%d, Bottom score=%d",
                businesses[0]["lead_score"] if businesses else 0,
                businesses[-1]["lead_score"] if businesses else 0)
    return businesses
