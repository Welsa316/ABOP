"""
utils.py — Small helper functions used across modules.
"""

import re
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger("lead_engine")


def setup_logging(verbose: bool = False) -> None:
    """Configure root logger for the project."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")


def clean_url(raw: str) -> str:
    """Normalise a URL: strip whitespace, add scheme if missing."""
    if not raw or not isinstance(raw, str):
        return ""
    url = raw.strip().strip('"').strip("'")
    if not url:
        return ""
    # Remove trailing slashes for consistency
    url = url.rstrip("/")
    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    return url


def extract_domain(url: str) -> str:
    """Return the bare domain from a URL, e.g. 'example.com'."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.lower().removeprefix("www.")
    except Exception:
        return ""


def is_social_media_url(url: str, social_domains: list[str]) -> bool:
    """Check whether a URL points to a social media profile."""
    domain = extract_domain(url)
    return any(sd in domain for sd in social_domains)


def normalize_text(text: str) -> str:
    """Lowercase, collapse whitespace."""
    return re.sub(r"\s+", " ", text.lower().strip())


def safe_int(val, default: int = 0) -> int:
    """Convert to int without crashing."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_float(val, default: float = 0.0) -> float:
    """Convert to float without crashing."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def save_json(data, path: Path) -> None:
    """Write data to a JSON file with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Saved JSON → %s", path)


def save_text(text: str, path: Path) -> None:
    """Write plain text to a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    logger.info("Saved text → %s", path)
