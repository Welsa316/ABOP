"""
scorer.py — Lead scoring engine.

Assigns a weighted score to each business based on:
  - Website status tier (not_found > discovered > listed)
  - Review count (popular businesses are better leads)
  - Rating (high-rated businesses are better leads)
"""

import logging
from .config import (
    SCORE_WEIGHTS as W,
    REVIEW_THRESHOLDS,
    RATING_THRESHOLDS,
)

logger = logging.getLogger("lead_engine")


def score_business(biz: dict) -> int:
    """
    Score a single business. Returns the total score (int).

    Scoring:
      - website_status "not_found": +40
      - website_status "discovered": +25
      - website_status "listed": +0
      - 100+ reviews: +8, 500+ reviews: +12
      - 4.5+ rating: +5, 4.8+ rating: +8
    """
    total = 0

    # Website status tier
    status = biz.get("website_status", "")
    if status == "not_found":
        total += W.get("no_website_found", 0)
    elif status == "discovered":
        total += W.get("unlisted_website", 0)
    elif not status:
        # Fallback for businesses that haven't been through the analyzer:
        # treat missing website as not_found
        if not biz.get("website"):
            total += W.get("no_website_found", 0)

    # Review count bonus
    review_count = biz.get("review_count", 0)
    if review_count >= REVIEW_THRESHOLDS["very_high"]:
        total += W.get("very_high_reviews_bonus", 0)
    elif review_count >= REVIEW_THRESHOLDS["high"]:
        total += W.get("high_reviews_bonus", 0)

    # Rating bonus
    rating = biz.get("rating", 0.0)
    if rating >= RATING_THRESHOLDS["excellent"]:
        total += W.get("excellent_rating_bonus", 0)
    elif rating >= RATING_THRESHOLDS["good"]:
        total += W.get("good_rating_bonus", 0)

    # Website audit signals (set by auditor.py)
    if biz.get("has_contact_form") is False and biz.get("website_status") == "listed":
        total += W.get("no_contact_form", 0)
    if biz.get("has_mobile_viewport") is False and biz.get("website_status") == "listed":
        total += W.get("no_mobile_viewport", 0)

    return max(total, 0)


def score_all(businesses: list[dict], analyses=None) -> list[dict]:
    """
    Score every business and attach results.

    Parameters:
        businesses: list of business dicts
        analyses: optional dict from analyze_websites() (unused directly
                  since analyzer already sets website_status on each biz)

    Returns the same list sorted by lead_score descending.
    Each business dict gets:
      - lead_score (int)
      - has_website (bool) — True if listed or discovered
      - website_status — preserved from analyzer, or set here if missing
    """
    for biz in businesses:
        # Ensure website_status exists (GUI path may skip analyzer)
        if "website_status" not in biz:
            biz["website_status"] = "listed" if biz.get("website") else "not_found"

        biz["lead_score"] = score_business(biz)
        biz["has_website"] = biz["website_status"] != "not_found"

    businesses.sort(key=lambda b: b["lead_score"], reverse=True)

    if businesses:
        logger.info("Scoring complete. Top score=%d, Bottom score=%d",
                    businesses[0]["lead_score"], businesses[-1]["lead_score"])
    return businesses
