"""
contact_discovery.py — Find social media profiles and email addresses
for businesses that have no website.

Uses DuckDuckGo HTML search to discover Instagram, Facebook, TikTok,
Yelp pages, and email addresses using business name + city queries.
"""

import re
import time
import random
import logging
from dataclasses import dataclass, field
from urllib.parse import urlparse, unquote

import httpx
from bs4 import BeautifulSoup

from . import config

logger = logging.getLogger("lead_engine")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ContactInfo:
    """Discovered contact information for one business."""
    instagram: str = ""
    facebook: str = ""
    tiktok: str = ""
    yelp: str = ""
    email: str = ""
    contact_methods_found: int = 0

    def count_methods(self) -> int:
        count = 0
        if self.instagram:
            count += 1
        if self.facebook:
            count += 1
        if self.tiktok:
            count += 1
        if self.email:
            count += 1
        if self.yelp:
            count += 1
        self.contact_methods_found = count
        return count


# ---------------------------------------------------------------------------
# URL validation helpers
# ---------------------------------------------------------------------------

# Patterns that indicate a result is NOT a real business profile
_INSTAGRAM_REJECT = re.compile(
    r"/(explore|tags|locations|p/|reel/|stories/|accounts/)",
    re.IGNORECASE,
)
_FACEBOOK_REJECT = re.compile(
    r"/(hashtag|places|events/|marketplace|groups/discover|watch/|login|"
    r"photo\.php|story\.php|share\.php)",
    re.IGNORECASE,
)
_TIKTOK_REJECT = re.compile(
    r"/(tag|discover|music|video/|search)",
    re.IGNORECASE,
)

_EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
)

# Emails to ignore (generic / false positives)
_JUNK_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "squarespace.com",
    "wordpress.com", "godaddy.com", "google.com", "facebook.com",
    "instagram.com", "tiktok.com", "yelp.com", "apple.com",
}

# HTTP headers for search requests
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Reusable httpx client (created lazily to avoid import-time side effects)
_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    """Return a shared httpx client, creating it on first use."""
    global _client
    if _client is None:
        _client = httpx.Client(
            headers=_HEADERS,
            follow_redirects=True,
            verify=False,
            timeout=config.CONTACT_DISCOVERY_TIMEOUT,
        )
    return _client


# ---------------------------------------------------------------------------
# Search helpers
# ---------------------------------------------------------------------------

def _ddg_search(query: str, max_results: int = 8) -> list[dict]:
    """
    Search DuckDuckGo HTML and return a list of result dicts:
      [{"url": "...", "title": "...", "snippet": "..."}, ...]

    Uses the HTML lite version to avoid needing an API key.
    Retries with exponential backoff if DuckDuckGo blocks/rate-limits.
    """
    url = "https://html.duckduckgo.com/html/"

    for attempt in range(3):
        try:
            client = _get_client()
            resp = client.post(url, data={"q": query, "b": ""})
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Search failed for %r: %s", query, exc)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Detect DuckDuckGo CAPTCHA / rate-limit block
        page_text = resp.text.lower()
        if ("please try again" in page_text
                or "blocked" in page_text
                or "unusual traffic" in page_text
                or "robot" in page_text):
            wait = (attempt + 1) * 5  # 5s, 10s, 15s
            logger.warning("DuckDuckGo rate-limited on %r — waiting %ds (attempt %d/3)",
                           query, wait, attempt + 1)
            time.sleep(wait)
            continue

        results = []
        for result_div in soup.select(".result"):
            link_tag = result_div.select_one("a.result__a")
            snippet_tag = result_div.select_one(".result__snippet")
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            # DuckDuckGo wraps URLs in a redirect — extract the real URL
            real_url = _extract_ddg_url(href)
            if not real_url:
                continue

            results.append({
                "url": real_url,
                "title": link_tag.get_text(strip=True),
                "snippet": snippet_tag.get_text(strip=True) if snippet_tag else "",
            })
            if len(results) >= max_results:
                break

        if not results and attempt < 2:
            # Got zero results — might be a soft block, retry once with a delay
            wait = (attempt + 1) * 4
            logger.debug("Zero results for %r — retrying after %ds", query, wait)
            time.sleep(wait)
            continue

        return results

    logger.warning("All retries exhausted for %r", query)
    return []


def _extract_ddg_url(href: str) -> str:
    """Extract the actual destination URL from a DuckDuckGo redirect link."""
    if "uddg=" in href:
        # //duckduckgo.com/l/?uddg=https%3A%2F%2F...&rut=...
        match = re.search(r"uddg=([^&]+)", href)
        if match:
            return unquote(match.group(1))
    if href.startswith("http"):
        return href
    return ""


def _rate_limit() -> None:
    """Sleep a random interval to avoid getting blocked."""
    delay = random.uniform(
        config.CONTACT_DISCOVERY_DELAY_MIN,
        config.CONTACT_DISCOVERY_DELAY_MAX,
    )
    time.sleep(delay)


# ---------------------------------------------------------------------------
# Platform-specific finders
# ---------------------------------------------------------------------------

def _clean_name(name: str) -> str:
    """Simplify business name for matching."""
    # Remove common suffixes, punctuation
    cleaned = re.sub(r"[''`]", "", name.lower())
    cleaned = re.sub(r"\b(llc|inc|corp|ltd|restaurant|bar|grill|cafe|café)\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s]", "", cleaned)
    return cleaned.strip()


def _name_matches(url: str, biz_name: str) -> bool:
    """
    Heuristic: does the URL path likely correspond to this business?
    We check if significant words from the name appear in the URL.
    """
    url_lower = unquote(url).lower().replace("-", "").replace("_", "").replace(".", "")
    name_words = _clean_name(biz_name).split()
    # Require at least one significant word (3+ chars) to match
    significant = [w for w in name_words if len(w) >= 3]
    if not significant:
        return True  # short name, can't filter well
    matches = sum(1 for w in significant if w in url_lower)
    return matches >= 1


def _find_instagram(biz_name: str, city: str) -> str:
    """Search for the business's Instagram profile."""
    queries = [
        f"site:instagram.com {biz_name} {city}",
        f"{biz_name} {city} instagram",
    ]
    for query in queries:
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for r in results:
            url = r["url"]
            parsed = urlparse(url)
            if "instagram.com" not in parsed.netloc:
                continue
            # Must be a profile URL like instagram.com/username
            path = parsed.path.strip("/")
            if not path or "/" in path:
                continue  # sub-page like /p/xxxxx
            if _INSTAGRAM_REJECT.search(url):
                continue
            if _name_matches(url, biz_name):
                clean = f"https://instagram.com/{path}"
                logger.debug("Instagram found: %s → %s", biz_name, clean)
                return clean
    return ""


def _find_facebook(biz_name: str, city: str) -> str:
    """Search for the business's Facebook page."""
    queries = [
        f"site:facebook.com {biz_name} {city}",
        f"{biz_name} {city} facebook",
    ]
    for query in queries:
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for r in results:
            url = r["url"]
            parsed = urlparse(url)
            if "facebook.com" not in parsed.netloc and "fb.com" not in parsed.netloc:
                continue
            if _FACEBOOK_REJECT.search(url):
                continue
            path = parsed.path.strip("/")
            if not path:
                continue
            if _name_matches(url, biz_name):
                clean = f"https://facebook.com/{path}"
                logger.debug("Facebook found: %s → %s", biz_name, clean)
                return clean
    return ""


def _find_tiktok(biz_name: str, city: str) -> str:
    """Search for the business's TikTok profile."""
    query = f"site:tiktok.com {biz_name} {city}"
    results = _ddg_search(query, max_results=5)
    _rate_limit()
    for r in results:
        url = r["url"]
        parsed = urlparse(url)
        if "tiktok.com" not in parsed.netloc:
            continue
        if _TIKTOK_REJECT.search(url):
            continue
        path = parsed.path.strip("/")
        if not path or not path.startswith("@"):
            continue
        if "/" in path:
            continue  # sub-page like /@user/video/123
        if _name_matches(url, biz_name):
            clean = f"https://tiktok.com/{path}"
            logger.debug("TikTok found: %s → %s", biz_name, clean)
            return clean
    return ""


def _find_yelp(biz_name: str, city: str) -> str:
    """Search for the business's Yelp page."""
    query = f"site:yelp.com {biz_name} {city}"
    results = _ddg_search(query, max_results=5)
    _rate_limit()
    for r in results:
        url = r["url"]
        parsed = urlparse(url)
        if "yelp.com" not in parsed.netloc:
            continue
        if "/biz/" not in parsed.path:
            continue  # only want business pages
        if _name_matches(url, biz_name):
            logger.debug("Yelp found: %s → %s", biz_name, url)
            return url
    return ""


def _find_email(biz_name: str, city: str) -> str:
    """Search for the business's email address in search snippets."""
    queries = [
        f"{biz_name} {city} email",
        f"{biz_name} {city} contact email",
    ]
    found_emails: list[str] = []

    for query in queries:
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for r in results:
            text = f"{r['title']} {r['snippet']}"
            emails = _EMAIL_RE.findall(text)
            for email in emails:
                domain = email.split("@")[1].lower()
                if domain in _JUNK_EMAIL_DOMAINS:
                    continue
                # Skip obviously fake/template emails
                local = email.split("@")[0].lower()
                if local in ("info", "noreply", "no-reply", "example", "test"):
                    continue
                found_emails.append(email.lower())

        if found_emails:
            break  # got at least one, stop searching

    if found_emails:
        # Prefer the most commonly found email
        best = max(set(found_emails), key=found_emails.count)
        logger.debug("Email found: %s → %s", biz_name, best)
        return best
    return ""


# ---------------------------------------------------------------------------
# Google Maps profile scraping (optional enrichment)
# ---------------------------------------------------------------------------

def _check_google_listing(google_url: str) -> dict:
    """
    Attempt to extract social links from a Google Maps listing page.
    Returns dict with any found links.
    """
    if not google_url:
        return {}

    try:
        client = _get_client()
        resp = client.get(google_url)
        resp.raise_for_status()
    except httpx.HTTPError:
        return {}

    text = resp.text.lower()
    found = {}

    # Look for social links in the page content
    ig_match = re.search(r'https?://(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)', resp.text)
    if ig_match:
        found["instagram"] = f"https://instagram.com/{ig_match.group(1)}"

    fb_match = re.search(r'https?://(?:www\.)?facebook\.com/([a-zA-Z0-9.\-]+)', resp.text)
    if fb_match:
        path = fb_match.group(1)
        if path not in ("sharer", "dialog", "share", "tr", "flx"):
            found["facebook"] = f"https://facebook.com/{path}"

    email_matches = _EMAIL_RE.findall(resp.text)
    for email in email_matches:
        domain = email.split("@")[1].lower()
        if domain not in _JUNK_EMAIL_DOMAINS:
            found["email"] = email.lower()
            break

    return found


# ---------------------------------------------------------------------------
# Main discovery function
# ---------------------------------------------------------------------------

def discover_contacts(biz: dict) -> ContactInfo:
    """
    Run full contact discovery for a single business.
    Tries Google Maps listing first, then falls back to search.
    """
    name = biz.get("business_name", "")
    city = biz.get("city", "")
    google_url = biz.get("google_url", "")

    if not name:
        return ContactInfo()

    info = ContactInfo()

    # Step 1: Check Google Maps listing for embedded links
    if google_url:
        gmap_links = _check_google_listing(google_url)
        info.instagram = gmap_links.get("instagram", "")
        info.facebook = gmap_links.get("facebook", "")
        info.email = gmap_links.get("email", "")
        _rate_limit()

    # Step 2: Search for anything not already found
    if not info.instagram:
        info.instagram = _find_instagram(name, city)
        _rate_limit()

    if not info.facebook:
        info.facebook = _find_facebook(name, city)
        _rate_limit()

    info.tiktok = _find_tiktok(name, city)
    _rate_limit()

    info.yelp = _find_yelp(name, city)
    _rate_limit()

    if not info.email:
        info.email = _find_email(name, city)

    info.count_methods()
    return info


def discover_all_contacts(
    businesses: list[dict],
    progress_callback=None,
) -> dict[int, ContactInfo]:
    """
    Run contact discovery for every business in the list.

    Returns a dict mapping business index → ContactInfo.
    Includes rate limiting between businesses.

    If *progress_callback* is provided it is called after each business as
    ``progress_callback(current_index, total, business_name, contact_info)``.
    """
    total = len(businesses)
    results: dict[int, ContactInfo] = {}

    for i, biz in enumerate(businesses):
        name = biz.get("business_name", "?")
        logger.info("Contact discovery [%d/%d]: %s", i + 1, total, name)

        try:
            info = discover_contacts(biz)
            results[i] = info
            methods = info.contact_methods_found
            parts = []
            if info.instagram:
                parts.append("IG")
            if info.facebook:
                parts.append("FB")
            if info.tiktok:
                parts.append("TT")
            if info.yelp:
                parts.append("Yelp")
            if info.email:
                parts.append("Email")
            summary = ", ".join(parts) if parts else "none"
            logger.info("  → Found %d contacts: %s", methods, summary)

            if progress_callback:
                progress_callback(i, total, name, info)

        except Exception as exc:
            logger.error("  → Error discovering contacts for %s: %s", name, exc)
            results[i] = ContactInfo()
            if progress_callback:
                progress_callback(i, total, name, results[i])

    found_any = sum(1 for c in results.values() if c.contact_methods_found > 0)
    logger.info("Contact discovery complete: %d/%d businesses have contacts",
                found_any, total)

    return results
