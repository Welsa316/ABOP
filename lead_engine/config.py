"""
config.py — Central configuration and scoring weights.

Edit the numbers in SCORE_WEIGHTS to change how leads are ranked.
All weights are *added* to a running total; higher = better lead for you.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ---------------------------------------------------------------------------
# Website analysis
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = 12          # seconds per HTTP request
MAX_CONCURRENT_REQUESTS = 10  # simultaneous website checks
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
MAX_HTML_SIZE = 500_000       # bytes — stop reading after this

# ---------------------------------------------------------------------------
# Scoring weights  (tweak these to taste)
# ---------------------------------------------------------------------------
SCORE_WEIGHTS = {
    # --- Website existence / reachability ---
    "no_website":              40,   # no URL at all
    "site_unreachable":        35,   # URL exists but request failed
    "social_media_only":       30,   # URL points to facebook/instagram/etc.

    # --- Technical quality ---
    "no_ssl":                  10,   # HTTP, not HTTPS
    "no_viewport":             12,   # missing mobile viewport meta
    "slow_response":            8,   # response > 5 s
    "thin_content":            10,   # very little text on page
    "placeholder_site":        15,   # looks like a template/parked page

    # --- UX / business readiness ---
    "no_contact_info":          8,
    "no_cta":                  10,   # no clear call-to-action
    "no_online_ordering":       6,   # restaurant without ordering link
    "no_booking":               6,   # service biz without booking link
    "no_menu":                  5,   # restaurant without menu link
    "outdated_design":         10,   # heuristic: tables, frames, old tags

    # --- Business attractiveness bonuses ---
    "high_reviews_bonus":       8,   # 100+ reviews
    "very_high_reviews_bonus": 12,   # 500+ reviews
    "good_rating_bonus":        5,   # 4.5+ stars
    "excellent_rating_bonus":   8,   # 4.8+ stars

    # --- Contact discovery bonuses ---
    "instagram_found":         15,   # Instagram profile discovered
    "facebook_found":          10,   # Facebook page discovered
    "tiktok_found":             8,   # TikTok profile discovered
    "email_found":             15,   # email address discovered
    "yelp_found":               5,   # Yelp listing discovered

    # --- Penalties ---
    "chain_penalty":          -20,   # suspected chain / franchise
    "strong_website_penalty":  -15,  # modern, complete site detected
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
# Known chains / franchises  (lowercase substrings)
# ---------------------------------------------------------------------------
CHAIN_KEYWORDS = [
    "mcdonald", "burger king", "wendy", "subway", "taco bell",
    "chick-fil-a", "popeyes", "sonic drive", "domino", "papa john",
    "pizza hut", "little caesars", "starbucks", "dunkin", "panda express",
    "chipotle", "five guys", "wingstop", "zaxby", "raising cane",
    "applebee", "chili's", "olive garden", "red lobster", "denny",
    "ihop", "waffle house", "cracker barrel", "outback steakhouse",
    "longhorn steakhouse", "texas roadhouse", "golden corral",
    "buffalo wild wings", "hooters", "tj maxx", "walmart", "target",
    "walgreens", "cvs", "home depot", "lowe's", "best buy",
    "autozone", "o'reilly auto", "jiffy lube", "enterprise rent",
    "great clips", "supercuts", "sport clips",
]

# ---------------------------------------------------------------------------
# Social-media-only URL patterns
# ---------------------------------------------------------------------------
SOCIAL_DOMAINS = [
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "youtube.com", "yelp.com", "linkedin.com",
]

# ---------------------------------------------------------------------------
# Category hints — used to decide which UX checks matter
# ---------------------------------------------------------------------------
RESTAURANT_KEYWORDS = [
    "restaurant", "food", "pizza", "burger", "sushi", "café", "cafe",
    "bakery", "deli", "grill", "seafood", "bbq", "barbecue", "taco",
    "diner", "bistro", "eatery", "kitchen", "steakhouse", "buffet",
    "noodle", "ramen", "pho", "thai", "chinese", "mexican", "italian",
    "japanese", "indian", "korean", "vietnamese", "mediterranean",
    "soul food", "cajun", "creole", "po'boy", "crawfish",
    "ice cream", "frozen yogurt", "smoothie", "juice bar",
    "coffee", "tea house", "donut", "bagel", "breakfast",
    "catering", "bar", "pub", "brewery", "winery", "cocktail",
]

SERVICE_KEYWORDS = [
    "salon", "barber", "spa", "massage", "nail", "beauty",
    "dental", "dentist", "doctor", "clinic", "therapy", "chiropractic",
    "veterinary", "vet", "pet", "grooming",
    "gym", "fitness", "yoga", "pilates", "martial arts",
    "plumber", "electrician", "hvac", "roofing", "contractor",
    "landscaping", "lawn", "cleaning", "maid", "auto repair",
    "mechanic", "tire", "collision", "body shop",
    "accounting", "tax", "insurance", "real estate", "attorney", "lawyer",
    "photography", "wedding", "florist", "tutoring",
]

# ---------------------------------------------------------------------------
# Contact discovery (search-based)
# ---------------------------------------------------------------------------
CONTACT_DISCOVERY_TIMEOUT = 10   # seconds per search request
CONTACT_DISCOVERY_DELAY_MIN = 2.0  # min seconds between search queries
CONTACT_DISCOVERY_DELAY_MAX = 4.0  # max seconds between search queries

# ---------------------------------------------------------------------------
# Targeting mode
# ---------------------------------------------------------------------------
# When True, ONLY businesses with NO website are kept as leads.
# Businesses that have any website URL are filtered out entirely.
# Set to False later if you want to also target businesses with weak sites.
NO_WEBSITE_ONLY = True

# ---------------------------------------------------------------------------
# Message-generation settings
# ---------------------------------------------------------------------------
MESSAGE_SCORE_THRESHOLD = 10   # only generate messages for scores >= this
MAX_MESSAGES_PER_RUN = 0       # 0 = unlimited; set to e.g. 50 to cap costs
