"""
scorer.py — Lead scoring engine.

Assigns a weighted score to each business based on website analysis,
business metadata, and category-specific checks.  Every point that
contributes to the total is recorded in a human-readable breakdown.

Sub-scores provide granular insight:
  - opportunity_score: how big is the digital gap?
  - reputation_score: how strong is the business's public reputation?
  - contactability_score: how easy is it to reach them?
  - digital_weakness_score: how weak is their current online presence?
  - priority_score: combined urgency indicator (opportunity * contactability factor)
"""

import logging
from .config import (
    SCORE_WEIGHTS as W,
    REVIEW_THRESHOLDS,
    RATING_THRESHOLDS,
    CHAIN_KEYWORDS,
    RESTAURANT_KEYWORDS,
    SERVICE_KEYWORDS,
    SUB_SCORE_CATEGORIES,
    PITCH_ANGLE_LABELS,
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


def _compute_sub_scores(breakdown: list[dict]) -> dict:
    """
    Compute sub-scores from the breakdown.
    Each sub-score sums points whose reason-key belongs to that category.
    """
    # Build a reverse map: reason_key -> points
    reason_points = {}
    for item in breakdown:
        reason_points[item.get("key", "")] = item["points"]

    sub = {}
    for score_name, keys in SUB_SCORE_CATEGORIES.items():
        total = sum(reason_points.get(k, 0) for k in keys)
        sub[score_name] = max(total, 0)

    # Priority score = opportunity * contactability factor
    opp = sub.get("opportunity_score", 0)
    contact = sub.get("contactability_score", 0)
    factor = 1.0 + (contact / 100.0) if contact > 0 else 0.5
    sub["priority_score"] = round(opp * factor)

    return sub


def _build_score_summary(breakdown: list[dict], pitch_angle: str,
                         sub_scores: dict) -> str:
    """Build a plain-English summary of the score."""
    if not breakdown:
        return "No scoring signals detected."

    parts = []
    positives = [b for b in breakdown if b["points"] > 0]
    negatives = [b for b in breakdown if b["points"] < 0]

    if positives:
        top = sorted(positives, key=lambda x: x["points"], reverse=True)[:3]
        reasons = [f"{b['reason']} (+{b['points']})" for b in top]
        parts.append("Key signals: " + "; ".join(reasons))

    if negatives:
        negs = [f"{b['reason']} ({b['points']})" for b in negatives]
        parts.append("Negatives: " + "; ".join(negs))

    label = PITCH_ANGLE_LABELS.get(pitch_angle, "")
    if label:
        parts.append(f"Recommended angle: {label}")

    return ". ".join(parts) + "."


def score_business(biz: dict, analysis: SiteAnalysis | None) -> dict:
    """
    Score a single business.

    Returns a dict with:
      - total_score (int)
      - breakdown   (list of {"reason": str, "points": int, "key": str})
      - pitch_angle (str) — suggested outreach angle
      - sub_scores  (dict) — opportunity, reputation, contactability, etc.
      - score_summary (str) — plain-English breakdown
    """
    breakdown: list[dict] = []
    pitch_angles: list[str] = []

    def add(reason: str, key: str) -> None:
        pts = W.get(key, 0)
        if pts:
            breakdown.append({"reason": reason, "points": pts, "key": key})

    has_website = bool(biz.get("website"))

    # ------------------------------------------------------------------
    # 1. Website existence
    # ------------------------------------------------------------------
    if not has_website:
        add("No website listed", "no_website")
        pitch_angles.append("needs_new_website")
        # Check if they have social media but no website
        if biz.get("instagram") or biz.get("facebook") or biz.get("tiktok"):
            pitch_angles.append("social_media_only")
    elif analysis:
        if analysis.is_social_only:
            add("Only a social media page (no real website)", "social_media_only")
            pitch_angles.append("social_media_only")
        elif not analysis.reachable:
            add("Website is unreachable / broken", "site_unreachable")
            pitch_angles.append("site_broken")
        else:
            # Reachable website — check for weaknesses
            if not analysis.has_ssl:
                add("No HTTPS (insecure)", "no_ssl")
                pitch_angles.append("security_upgrade")
            if not analysis.has_viewport:
                add("Not mobile-friendly (no viewport)", "no_viewport")
                pitch_angles.append("mobile_improvement")
            if analysis.response_time > 5:
                add(f"Slow website ({analysis.response_time:.1f}s)", "slow_response")
                pitch_angles.append("speed_improvement")
            if analysis.is_thin_content:
                add("Very thin content", "thin_content")
                pitch_angles.append("content_improvement")
            if analysis.is_placeholder:
                add("Placeholder / parked page", "placeholder_site")
                pitch_angles.append("needs_new_website")
            if not analysis.has_cta:
                add("No clear call-to-action", "no_cta")
                pitch_angles.append("cta_improvement")
            if not analysis.has_contact_info:
                add("No visible contact info", "no_contact_info")
                pitch_angles.append("contact_improvement")
            if analysis.is_outdated_design:
                add("Outdated / basic design", "outdated_design")
                pitch_angles.append("redesign")

            # Category-specific checks
            categories = biz.get("categories", [])
            if isinstance(categories, str):
                categories = [categories]
            if _is_restaurant(categories):
                if not analysis.has_menu_link:
                    add("Restaurant without online menu", "no_menu")
                    pitch_angles.append("add_menu")
                if not analysis.has_ordering_link:
                    add("Restaurant without online ordering", "no_online_ordering")
                    pitch_angles.append("add_ordering")
            if _is_service(categories):
                if not analysis.has_booking_link:
                    add("No online booking / appointment system", "no_booking")
                    pitch_angles.append("add_booking")

            # If site is actually strong, penalize
            weakness_count = sum(1 for b in breakdown if b["points"] > 0
                                 and b["key"] not in ("high_reviews_bonus",
                                                       "very_high_reviews_bonus",
                                                       "good_rating_bonus",
                                                       "excellent_rating_bonus",
                                                       "instagram_found",
                                                       "facebook_found",
                                                       "tiktok_found",
                                                       "email_found",
                                                       "yelp_found"))
            if weakness_count == 0 and analysis.reachable:
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
    # 5. Contact discovery bonuses
    # ------------------------------------------------------------------
    if biz.get("instagram"):
        add("Instagram profile found", "instagram_found")
    if biz.get("facebook"):
        add("Facebook page found", "facebook_found")
    if biz.get("tiktok"):
        add("TikTok profile found", "tiktok_found")
    if biz.get("email"):
        add("Email address found", "email_found")
    if biz.get("yelp"):
        add("Yelp listing found", "yelp_found")

    # ------------------------------------------------------------------
    # Compute total + sub-scores
    # ------------------------------------------------------------------
    total = sum(item["points"] for item in breakdown)
    total = max(total, 0)  # floor at 0

    sub_scores = _compute_sub_scores(breakdown)

    # Pick primary pitch angle
    angle_priority = [
        "needs_new_website", "site_broken", "social_media_only", "redesign",
        "mobile_improvement", "speed_improvement", "cta_improvement",
        "contact_improvement", "content_improvement", "security_upgrade",
        "add_menu", "add_ordering", "add_booking", "general_improvement",
        "low_priority", "skip_chain",
    ]
    primary_angle = "general_improvement"
    for a in angle_priority:
        if a in pitch_angles:
            primary_angle = a
            break

    score_summary = _build_score_summary(breakdown, primary_angle, sub_scores)

    return {
        "total_score": total,
        "breakdown": breakdown,
        "pitch_angle": primary_angle,
        "all_pitch_angles": list(dict.fromkeys(pitch_angles)),  # unique, ordered
        "sub_scores": sub_scores,
        "score_summary": score_summary,
        "recommended_pitch_label": PITCH_ANGLE_LABELS.get(primary_angle, ""),
    }


def score_all(businesses: list[dict],
              analyses: dict[int, SiteAnalysis]) -> list[dict]:
    """
    Score every business and attach results.

    Returns the same list of business dicts, each augmented with:
      lead_score, score_breakdown, pitch_angle, all_pitch_angles,
      opportunity_score, reputation_score, contactability_score,
      digital_weakness_score, priority_score, score_summary
    """
    for i, biz in enumerate(businesses):
        analysis = analyses.get(i)
        result = score_business(biz, analysis)
        biz["lead_score"] = result["total_score"]
        biz["score_breakdown"] = result["breakdown"]
        biz["pitch_angle"] = result["pitch_angle"]
        biz["all_pitch_angles"] = result["all_pitch_angles"]
        biz["score_summary"] = result["score_summary"]
        biz["recommended_pitch_label"] = result["recommended_pitch_label"]

        # Sub-scores
        for key, value in result["sub_scores"].items():
            biz[key] = value

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

        # CRM defaults
        if "status" not in biz:
            biz["status"] = "new"
        if "notes" not in biz:
            biz["notes"] = ""
        if "contacted" not in biz:
            biz["contacted"] = "No"

    # Sort by lead_score descending
    businesses.sort(key=lambda b: b["lead_score"], reverse=True)
    logger.info("Scoring complete. Top score=%d, Bottom score=%d",
                businesses[0]["lead_score"] if businesses else 0,
                businesses[-1]["lead_score"] if businesses else 0)
    return businesses
