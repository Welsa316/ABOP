"""
utils.py — Small helper functions used across modules.
"""

import re
import logging
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
    url = url.rstrip("/")
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    return url


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
