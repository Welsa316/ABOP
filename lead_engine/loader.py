"""
loader.py — Load a CSV of businesses, normalise columns, clean data,
             and remove obvious duplicates.

The column-mapping system uses fuzzy keyword matching so the tool works
even when CSV headers differ slightly between exports.
"""

import csv
import logging
from pathlib import Path

from .utils import clean_url, normalize_text, safe_int, safe_float

logger = logging.getLogger("lead_engine")

# ---------------------------------------------------------------------------
# Column-mapping heuristics
# ---------------------------------------------------------------------------
# Each target field maps to a list of substrings that might appear in the
# raw CSV header.  The first match wins.

COLUMN_HINTS: dict[str, list[str]] = {
    "business_name": ["title", "name", "business_name", "business name",
                      "company", "place"],
    "website":       ["website", "web", "url", "site", "homepage"],
    "phone":         ["phone", "telephone", "tel", "mobile", "contact"],
    "street":        ["street", "address_line", "address line", "addr"],
    "city":          ["city", "town", "locality"],
    "state":         ["state", "region", "province"],
    "country":       ["country", "countrycode", "country_code"],
    "category":      ["category", "categories/0", "type", "business_type",
                      "categoryname", "category_name"],
    "all_categories": [],  # populated from categories/* columns
    "rating":        ["totalscore", "total_score", "rating", "score", "stars"],
    "review_count":  ["reviewscount", "reviews_count", "review_count",
                      "reviewcount", "reviews", "num_reviews"],
    "google_url":    ["url", "google", "maps_url", "place_url", "link"],
}


def _match_column(raw_header: str, hints: list[str]) -> bool:
    """Return True if *raw_header* matches any of the hint substrings."""
    h = raw_header.lower().replace(" ", "").replace("_", "")
    for hint in hints:
        if hint.replace(" ", "").replace("_", "") in h:
            return True
    return False


def _build_column_map(headers: list[str]) -> dict[str, str | None]:
    """
    Map our canonical field names to actual CSV column names.
    Returns {canonical_name: csv_column_name_or_None}.
    """
    col_map: dict[str, str | None] = {k: None for k in COLUMN_HINTS}
    used: set[str] = set()

    for field, hints in COLUMN_HINTS.items():
        if field == "all_categories":
            continue
        for header in headers:
            if header in used:
                continue
            if _match_column(header, hints):
                col_map[field] = header
                used.add(header)
                break

    # Special: gather all "categories/*" columns
    cat_cols = [h for h in headers if h.lower().startswith("categories/")]
    col_map["all_categories"] = cat_cols  # type: ignore[assignment]

    # google_url often collides with "website" because both contain "url".
    # If google_url resolved to the same column as website, try harder.
    if col_map["google_url"] == col_map["website"]:
        for header in headers:
            hl = header.lower()
            if "google" in hl or "maps" in hl or "place" in hl:
                col_map["google_url"] = header
                break
        # If still colliding, prefer the one with "google" in it
        if col_map["google_url"] == col_map["website"]:
            col_map["google_url"] = None
            for header in headers:
                if header not in used and "url" in header.lower():
                    col_map["google_url"] = header
                    break

    return col_map


def _row_to_business(row: dict, col_map: dict) -> dict:
    """Convert a raw CSV row dict into a normalised business dict."""

    def _get(field: str) -> str:
        col = col_map.get(field)
        if col is None:
            return ""
        val = row.get(col, "")
        return str(val).strip() if val else ""

    # Gather all category values
    cat_cols = col_map.get("all_categories", [])
    categories = []
    if isinstance(cat_cols, list):
        for c in cat_cols:
            v = row.get(c, "")
            if v and str(v).strip():
                categories.append(str(v).strip())

    # Also include the single "category" field if not already present
    single_cat = _get("category")
    if single_cat and single_cat not in categories:
        categories.insert(0, single_cat)

    website_raw = _get("website")
    website = clean_url(website_raw)

    return {
        "business_name":      _get("business_name"),
        "website_raw":        website_raw,
        "website":            website,
        "phone":              _get("phone"),
        "street":             _get("street"),
        "city":               _get("city"),
        "state":              _get("state"),
        "country":            _get("country"),
        "categories":         categories,
        "primary_category":   categories[0] if categories else "",
        "rating":             safe_float(_get("rating")),
        "review_count":       safe_int(_get("review_count")),
        "google_url":         _get("google_url"),
        # Keep full original row for reference
        "_raw":               dict(row),
    }


def _dedup_key(biz: dict) -> str:
    """Create a deduplication key from name + city + phone."""
    parts = [
        normalize_text(biz["business_name"]),
        normalize_text(biz["city"]),
        normalize_text(biz["phone"]),
    ]
    return "|".join(parts)


def load_csv(filepath: str | Path) -> list[dict]:
    """
    Load a CSV file, normalise columns, clean data, remove duplicates.

    Returns a list of business dicts ready for scoring.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV not found: {filepath}")

    logger.info("Loading CSV: %s", filepath)

    # Try common encodings
    raw_rows: list[dict] = []
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(filepath, newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                raw_rows = list(reader)
            logger.info("Read %d rows with encoding=%s", len(raw_rows), encoding)
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        raise RuntimeError(f"Could not decode CSV with any known encoding: {filepath}")

    if not headers:
        raise ValueError("CSV has no headers")

    col_map = _build_column_map(headers)
    logger.info("Column mapping: %s",
                {k: v for k, v in col_map.items() if k != "all_categories"})

    # Normalise rows
    businesses: list[dict] = []
    seen_keys: set[str] = set()
    dupes = 0

    for row in raw_rows:
        biz = _row_to_business(row, col_map)
        if not biz["business_name"]:
            continue  # skip unnamed rows

        key = _dedup_key(biz)
        if key in seen_keys:
            dupes += 1
            continue
        seen_keys.add(key)
        businesses.append(biz)

    logger.info("Loaded %d businesses (%d duplicates removed)", len(businesses), dupes)

    return businesses
