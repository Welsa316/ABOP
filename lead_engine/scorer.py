"""
scorer.py — Lead scoring engine.

Assigns a weighted score to each business based on:
  - Whether they have a website (no website = highest opportunity)
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
      - No website: +40
      - 100+ reviews: +8, 500+ reviews: +12
      - 4.5+ rating: +5, 4.8+ rating: +8
    """
    total = 0

    # Website existence
    if not biz.get("website"):
        total += W.get("no_website", 0)

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

    return max(total, 0)


def score_all(businesses: list[dict]) -> list[dict]:
    """
    Score every business and attach results.

    Returns the same list sorted by lead_score descending.
    Each business dict gets:
      - lead_score (int)
      - has_website (bool)
    """
    for biz in businesses:
        biz["lead_score"] = score_business(biz)
        biz["has_website"] = bool(biz.get("website"))

    businesses.sort(key=lambda b: b["lead_score"], reverse=True)

    if businesses:
        logger.info("Scoring complete. Top score=%d, Bottom score=%d",
                    businesses[0]["lead_score"], businesses[-1]["lead_score"])
    return businesses
