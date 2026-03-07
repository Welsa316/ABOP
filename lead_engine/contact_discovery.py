"""
contact_discovery.py — Find social media profiles and email addresses
for businesses that have no website.

Uses DuckDuckGo search to discover Instagram, Facebook, TikTok,
Yelp pages, and email addresses using business name + city queries.
"""

import re
import time
import random
import logging
from dataclasses import dataclass, field
from urllib.parse import urlparse, unquote

import httpx

from . import config

# Try the dedicated DuckDuckGo search library first (much more reliable)
try:
    from duckduckgo_search import DDGS
    _HAS_DDGS = True
except ImportError:
    _HAS_DDGS = False

try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:
    _HAS_BS4 = False

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
    # Confidence scores: "high", "medium", "low"
    instagram_confidence: str = ""
    facebook_confidence: str = ""
    tiktok_confidence: str = ""
    yelp_confidence: str = ""
    email_confidence: str = ""
    best_contact_channel: str = ""

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
        self._pick_best_channel()
        return count

    def _pick_best_channel(self):
        """Choose the best contact channel based on what was found."""
        # Priority: email > instagram > facebook > tiktok > yelp
        if self.email:
            self.best_contact_channel = "email"
        elif self.instagram:
            self.best_contact_channel = "instagram_dm"
        elif self.facebook:
            self.best_contact_channel = "facebook"
        elif self.tiktok:
            self.best_contact_channel = "tiktok"
        elif self.yelp:
            self.best_contact_channel = "yelp"
        else:
            self.best_contact_channel = "none"


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

# HTTP headers for search requests (used by HTML fallback and Google Maps)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Reusable httpx client (created lazily to avoid import-time side effects)
_client: httpx.Client | None = None

# Reusable DDGS instance
_ddgs: "DDGS | None" = None


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


def _get_ddgs() -> "DDGS":
    """Return a shared DDGS instance, creating it on first use."""
    global _ddgs
    if _ddgs is None:
        _ddgs = DDGS()
    return _ddgs


# ---------------------------------------------------------------------------
# Search helpers
# ---------------------------------------------------------------------------

def _ddg_search(query: str, max_results: int = 8) -> list[dict]:
    """
    Search DuckDuckGo and return a list of result dicts:
      [{"url": "...", "title": "...", "snippet": "..."}, ...]

    Uses the duckduckgo-search library (primary) with HTML fallback.
    """
    # --- Primary: duckduckgo-search library ---
    if _HAS_DDGS:
        return _ddg_search_library(query, max_results)

    # --- Fallback: manual HTML scraping ---
    if _HAS_BS4:
        return _ddg_search_html(query, max_results)

    logger.error("No search backend available — install duckduckgo-search")
    return []


def _ddg_search_library(query: str, max_results: int = 8) -> list[dict]:
    """Search using the duckduckgo-search Python library."""
    for attempt in range(3):
        try:
            ddgs = _get_ddgs()
            raw = ddgs.text(query, max_results=max_results)
            results = []
            for r in raw:
                results.append({
                    "url": r.get("href", ""),
                    "title": r.get("title", ""),
                    "snippet": r.get("body", ""),
                })
            if results:
                logger.debug("DDGS search OK: %r → %d results", query, len(results))
            else:
                logger.warning("DDGS search returned 0 results for %r", query)
            return results
        except Exception as exc:
            wait = (attempt + 1) * 3
            logger.warning("DDGS search error for %r (attempt %d/3): %s — retrying in %ds",
                           query, attempt + 1, exc, wait)
            # Reset the DDGS instance in case it's in a bad state
            global _ddgs
            _ddgs = None
            if attempt < 2:
                time.sleep(wait)
    logger.warning("All search attempts failed for: %s", query)
    return []


def _ddg_search_html(query: str, max_results: int = 8) -> list[dict]:
    """Fallback: scrape DuckDuckGo HTML lite directly."""
    url = "https://html.duckduckgo.com/html/"

    for attempt in range(3):
        try:
            client = _get_client()
            resp = client.post(url, data={"q": query, "b": ""})
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("HTML search failed for %r: %s", query, exc)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Detect DuckDuckGo CAPTCHA / rate-limit block
        page_text = resp.text.lower()
        if ("please try again" in page_text
                or "blocked" in page_text
                or "unusual traffic" in page_text
                or "robot" in page_text):
            wait = (attempt + 1) * 5
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
    cleaned = re.sub(r"\b(llc|inc|corp|ltd|the|and|of|restaurant|bar|grill|cafe|café|"
                      r"sports|kitchen|house|place|shop|store|lounge|club)\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s]", "", cleaned)
    return cleaned.strip()


def _calc_confidence(query_index: int, result_index: int) -> str:
    """Estimate confidence based on which query and result position matched."""
    # First query + first result = highest confidence
    if query_index == 0 and result_index == 0:
        return "high"
    if query_index <= 1 and result_index <= 1:
        return "high"
    if query_index <= 1:
        return "medium"
    return "low"


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
    if matches == 0:
        logger.debug("Name match failed: words=%s not found in URL %s", significant, url_lower)
    return matches >= 1


def _find_instagram(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's Instagram profile. Returns (url, confidence)."""
    queries = [
        f"site:instagram.com {biz_name} {city}",
        f"{biz_name} {city} instagram",
    ]
    # Fallback: try without city if city queries return nothing useful
    if city:
        queries.append(f"site:instagram.com {biz_name}")
        queries.append(f"{biz_name} instagram")
    for qi, query in enumerate(queries):
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
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
                confidence = _calc_confidence(qi, ri)
                logger.info("    ✅ Instagram found (%s): %s", confidence, clean)
                return clean, confidence
            else:
                logger.info("    ⏭️  Instagram candidate rejected (name mismatch): %s", url)
    return "", ""


def _find_facebook(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's Facebook page. Returns (url, confidence)."""
    queries = [
        f"site:facebook.com {biz_name} {city}",
        f"{biz_name} {city} facebook",
    ]
    if city:
        queries.append(f"site:facebook.com {biz_name}")
        queries.append(f"{biz_name} facebook")
    for qi, query in enumerate(queries):
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
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
                confidence = _calc_confidence(qi, ri)
                logger.info("    ✅ Facebook found (%s): %s", confidence, clean)
                return clean, confidence
            else:
                logger.info("    ⏭️  Facebook candidate rejected (name mismatch): %s", url)
    return "", ""


def _find_tiktok(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's TikTok profile. Returns (url, confidence)."""
    queries = [f"site:tiktok.com {biz_name} {city}"]
    if city:
        queries.append(f"site:tiktok.com {biz_name}")
    for qi, query in enumerate(queries):
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
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
                confidence = _calc_confidence(qi, ri)
                logger.info("    ✅ TikTok found (%s): %s", confidence, clean)
                return clean, confidence
    return "", ""


def _find_yelp(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's Yelp page. Returns (url, confidence)."""
    queries = [f"site:yelp.com {biz_name} {city}"]
    if city:
        queries.append(f"site:yelp.com {biz_name}")
    for qi, query in enumerate(queries):
        results = _ddg_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
            url = r["url"]
            parsed = urlparse(url)
            if "yelp.com" not in parsed.netloc:
                continue
            if "/biz/" not in parsed.path:
                continue  # only want business pages
            if _name_matches(url, biz_name):
                confidence = _calc_confidence(qi, ri)
                logger.info("    ✅ Yelp found (%s): %s", confidence, url)
                return url, confidence
    return "", ""


def _find_email(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's email address in search snippets. Returns (email, confidence)."""
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
        # Confidence: high if found multiple times, medium otherwise
        count = found_emails.count(best)
        confidence = "high" if count >= 2 else "medium"
        logger.debug("Email found: %s → %s (%s)", biz_name, best, confidence)
        return best, confidence
    return "", ""


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
        if gmap_links.get("instagram"):
            info.instagram = gmap_links["instagram"]
            info.instagram_confidence = "high"  # from Google listing = reliable
        if gmap_links.get("facebook"):
            info.facebook = gmap_links["facebook"]
            info.facebook_confidence = "high"
        if gmap_links.get("email"):
            info.email = gmap_links["email"]
            info.email_confidence = "high"
        _rate_limit()

    # Step 2: Search for anything not already found
    if not info.instagram:
        info.instagram, info.instagram_confidence = _find_instagram(name, city)
        _rate_limit()

    if not info.facebook:
        info.facebook, info.facebook_confidence = _find_facebook(name, city)
        _rate_limit()

    info.tiktok, info.tiktok_confidence = _find_tiktok(name, city)
    _rate_limit()

    info.yelp, info.yelp_confidence = _find_yelp(name, city)
    _rate_limit()

    if not info.email:
        info.email, info.email_confidence = _find_email(name, city)

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

    # Quick connectivity check — test a simple search before processing all businesses
    if _HAS_DDGS:
        logger.info("Using duckduckgo-search library for contact discovery")
        print("      Search backend: duckduckgo-search library")
    else:
        logger.warning("duckduckgo-search not installed — using HTML fallback (less reliable)")
        print("      ⚠️  duckduckgo-search NOT installed — using HTML scraping fallback")
    test_results = _ddg_search("test", max_results=1)
    if not test_results:
        logger.warning("Search connectivity test failed — results may be limited")
        print("      ⚠️  Search connectivity test FAILED — DuckDuckGo may be blocking requests")
        print("      Contact discovery results will likely be empty.")
    else:
        print("      ✅ Search connectivity test passed")

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
