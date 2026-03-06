"""
analyzer.py — Lightweight website analysis using requests + BeautifulSoup.

Fetches the homepage, follows redirects, and runs heuristic checks to
detect common issues that a freelance web developer can fix.
"""

import re
import time
import logging
import asyncio
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from . import config
from .utils import extract_domain, is_social_media_url

logger = logging.getLogger("lead_engine")


@dataclass
class SiteAnalysis:
    """Results of analysing one website."""
    url: str = ""
    final_url: str = ""
    reachable: bool = False
    status_code: int = 0
    response_time: float = 0.0
    error: str = ""

    # Technical checks
    has_ssl: bool = False
    has_viewport: bool = False
    title: str = ""

    # Content checks
    text_length: int = 0
    has_contact_info: bool = False
    has_cta: bool = False
    has_menu_link: bool = False
    has_booking_link: bool = False
    has_ordering_link: bool = False
    is_placeholder: bool = False
    is_thin_content: bool = False
    is_outdated_design: bool = False

    # Derived flags
    is_social_only: bool = False
    is_slow: bool = False

    detected_issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Heuristic keyword sets
# ---------------------------------------------------------------------------
CTA_KEYWORDS = [
    "book now", "order now", "order online", "schedule", "appointment",
    "get a quote", "contact us", "call now", "sign up", "get started",
    "request", "reserve", "buy now", "shop now", "free estimate",
    "learn more", "join", "subscribe", "download",
]

CONTACT_PATTERNS = [
    r"\(\d{3}\)\s*\d{3}[\-\.\s]\d{4}",   # (504) 345-2878
    r"\d{3}[\-\.]\d{3}[\-\.]\d{4}",       # 504-345-2878
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",  # email
]

MENU_KEYWORDS = ["menu", "our food", "dishes", "specials", "cuisine"]
BOOKING_KEYWORDS = ["book", "appointment", "schedule", "reservation", "reserve"]
ORDERING_KEYWORDS = ["order online", "order now", "doordash", "ubereats",
                     "grubhub", "online order", "pickup", "delivery"]

OUTDATED_TAGS = ["<frameset", "<frame ", "<marquee", "<blink",
                 "<center>", "<font ", "bgcolor="]

PLACEHOLDER_SIGNALS = [
    "coming soon", "under construction", "website coming",
    "parked", "this domain", "buy this domain",
    "wordpress starter", "starter template", "hello world",
    "sample page", "just another wordpress",
]


# ---------------------------------------------------------------------------
# Core analysis function (single site)
# ---------------------------------------------------------------------------
async def _analyze_one(client: httpx.AsyncClient, url: str) -> SiteAnalysis:
    """Fetch and analyse a single URL."""
    result = SiteAnalysis(url=url)

    # --- Social-media-only check ---
    if is_social_media_url(url, config.SOCIAL_DOMAINS):
        result.is_social_only = True
        result.detected_issues.append("social_media_only")
        return result

    # --- Fetch page ---
    try:
        t0 = time.monotonic()
        resp = await client.get(
            url,
            follow_redirects=True,
            timeout=config.REQUEST_TIMEOUT,
        )
        result.response_time = round(time.monotonic() - t0, 2)
        result.status_code = resp.status_code
        result.final_url = str(resp.url)
        result.reachable = resp.status_code < 400
    except httpx.TimeoutException:
        result.error = "timeout"
        result.detected_issues.append("site_unreachable")
        return result
    except Exception as exc:
        result.error = str(exc)[:200]
        result.detected_issues.append("site_unreachable")
        return result

    if not result.reachable:
        result.detected_issues.append("site_unreachable")
        return result

    # --- SSL ---
    result.has_ssl = result.final_url.startswith("https://")
    if not result.has_ssl:
        result.detected_issues.append("no_ssl")

    # --- Slow ---
    if result.response_time > 5.0:
        result.is_slow = True
        result.detected_issues.append("slow_response")

    # --- Parse HTML ---
    html = resp.text[:config.MAX_HTML_SIZE]
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True).lower()
    result.text_length = len(text)

    # Title
    title_tag = soup.find("title")
    result.title = title_tag.get_text(strip=True) if title_tag else ""

    # Viewport
    viewport = soup.find("meta", attrs={"name": "viewport"})
    result.has_viewport = viewport is not None
    if not result.has_viewport:
        result.detected_issues.append("no_viewport")

    # Thin content (< 200 chars of visible text)
    if result.text_length < 200:
        result.is_thin_content = True
        result.detected_issues.append("thin_content")

    # Placeholder / parked
    for signal in PLACEHOLDER_SIGNALS:
        if signal in text:
            result.is_placeholder = True
            result.detected_issues.append("placeholder_site")
            break

    # Contact info
    for pattern in CONTACT_PATTERNS:
        if re.search(pattern, html):
            result.has_contact_info = True
            break
    if not result.has_contact_info:
        result.detected_issues.append("no_contact_info")

    # CTA
    html_lower = html.lower()
    result.has_cta = any(kw in html_lower for kw in CTA_KEYWORDS)
    if not result.has_cta:
        result.detected_issues.append("no_cta")

    # Menu link (relevant for restaurants)
    result.has_menu_link = any(kw in html_lower for kw in MENU_KEYWORDS)

    # Booking link
    result.has_booking_link = any(kw in html_lower for kw in BOOKING_KEYWORDS)

    # Online ordering
    result.has_ordering_link = any(kw in html_lower for kw in ORDERING_KEYWORDS)

    # Outdated design heuristic
    for tag in OUTDATED_TAGS:
        if tag in html_lower:
            result.is_outdated_design = True
            result.detected_issues.append("outdated_design")
            break

    return result


# ---------------------------------------------------------------------------
# Public: analyse a batch of businesses
# ---------------------------------------------------------------------------
async def analyze_websites(businesses: list[dict],
                           max_concurrent: int | None = None) -> dict[int, SiteAnalysis]:
    """
    Analyse websites for all businesses that have a URL.

    Returns {index: SiteAnalysis} keyed by position in the input list.
    """
    sem = asyncio.Semaphore(max_concurrent or config.MAX_CONCURRENT_REQUESTS)

    async def _bounded(idx: int, url: str) -> tuple[int, SiteAnalysis]:
        async with sem:
            return idx, await _analyze_one(client, url)

    results: dict[int, SiteAnalysis] = {}
    headers = {
        "User-Agent": config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    async with httpx.AsyncClient(headers=headers, verify=False) as client:
        tasks = []
        for i, biz in enumerate(businesses):
            url = biz.get("website", "")
            if url:
                tasks.append(_bounded(i, url))
            else:
                results[i] = SiteAnalysis()  # empty analysis for no-URL rows

        if tasks:
            logger.info("Analysing %d websites (concurrency=%d) ...",
                        len(tasks),
                        max_concurrent or config.MAX_CONCURRENT_REQUESTS)
            completed = await asyncio.gather(*tasks, return_exceptions=True)
            for item in completed:
                if isinstance(item, Exception):
                    logger.warning("Analysis task failed: %s", item)
                    continue
                idx, analysis = item
                results[idx] = analysis

    return results
