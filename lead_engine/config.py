"""
config.py — Central configuration and scoring weights.

Edit the numbers in SCORE_WEIGHTS to change how leads are ranked.
All weights are *added* to a running total; higher = better lead for you.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

# ---------------------------------------------------------------------------
# Scoring weights  (tweak these to taste)
# ---------------------------------------------------------------------------
SCORE_WEIGHTS = {
    # --- Website existence ---
    "no_website":              40,   # no URL at all

    # --- Business attractiveness bonuses ---
    "high_reviews_bonus":       8,   # 100+ reviews
    "very_high_reviews_bonus": 12,   # 500+ reviews
    "good_rating_bonus":        5,   # 4.5+ stars
    "excellent_rating_bonus":   8,   # 4.8+ stars
}

# Review thresholds used by the scorer
REVIEW_THRESHOLDS = {
    "high": 100,
    "very_high": 500,
}
RATING_THRESHOLDS = {
    "good": 4.5,
    "excellent": 4.8,
}

# ---------------------------------------------------------------------------
# Excel output settings
# ---------------------------------------------------------------------------
EXCEL_FILENAME = "lead_tracker.xlsx"
EXCEL_HIGH_SCORE_THRESHOLD = 50   # green highlight threshold
EXCEL_MEDIUM_SCORE_THRESHOLD = 30  # yellow highlight threshold
