"""
contact_discovery.py — Find social media profiles and email addresses
for businesses using web search.

Search backends (tried in order):
  1. duckduckgo-search library (if installed)
  2. Google HTML scraping (reliable fallback)
  3. DuckDuckGo HTML scraping (last resort)

Discovers Instagram, Facebook, TikTok, Yelp pages, and email addresses
using business name + city queries.
"""

import re
import time
import random
import logging
from dataclasses import dataclass
from urllib.parse import urlparse, unquote, quote_plus

import httpx

from . import config

# Try the dedicated DuckDuckGo search library first
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

# Track which search backend is working
_search_backend: str = "unknown"  # "ddgs", "bing", "google", "ddg_html", "none"

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

_JUNK_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "squarespace.com",
    "wordpress.com", "godaddy.com", "google.com", "facebook.com",
    "instagram.com", "tiktok.com", "yelp.com", "apple.com",
}

# Rotating user agents to reduce blocking
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Reusable httpx client
_client: httpx.Client | None = None

# Reusable DDGS instance
_ddgs: "DDGS | None" = None


def _get_client() -> httpx.Client:
    """Return a shared httpx client, creating it on first use."""
    global _client
    if _client is None:
        headers = {**_HEADERS, "User-Agent": random.choice(_USER_AGENTS)}
        _client = httpx.Client(
            headers=headers,
            follow_redirects=True,
            verify=False,
            timeout=config.CONTACT_DISCOVERY_TIMEOUT,
        )
    return _client


def _fresh_client() -> httpx.Client:
    """Create a new client with a fresh user agent (for retries after blocks)."""
    global _client
    if _client:
        try:
            _client.close()
        except Exception:
            pass
    headers = {**_HEADERS, "User-Agent": random.choice(_USER_AGENTS)}
    _client = httpx.Client(
        headers=headers,
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
# Search backends
# ---------------------------------------------------------------------------

def _web_search(query: str, max_results: int = 8) -> list[dict]:
    """
    Search the web and return a list of result dicts:
      [{"url": "...", "title": "...", "snippet": "..."}, ...]

    Tries multiple backends in order of reliability.
    """
    global _search_backend

    # If we already know which backend works, use it directly
    if _search_backend == "bing":
        results = _bing_search(query, max_results)
        if results:
            return results
    elif _search_backend == "google":
        results = _google_search(query, max_results)
        if results:
            return results
    elif _search_backend == "ddgs":
        results = _ddg_search_library(query, max_results)
        if results:
            return results
    elif _search_backend == "ddg_html":
        results = _ddg_search_html(query, max_results)
        if results:
            return results

    # If the preferred backend returned nothing, try all others as fallback
    # This handles cases where the backend works sometimes but not always
    backends = []
    if _HAS_DDGS and _search_backend != "ddgs":
        backends.append(("ddgs", _ddg_search_library))
    if _HAS_BS4 and _search_backend != "bing":
        backends.append(("bing", _bing_search))
    if _HAS_BS4 and _search_backend != "google":
        backends.append(("google", _google_search))
    if _HAS_BS4 and _search_backend != "ddg_html":
        backends.append(("ddg_html", _ddg_search_html))

    for name, fn in backends:
        results = fn(query, max_results)
        if results:
            if _search_backend == "unknown":
                _search_backend = name
                logger.info("Auto-selected search backend: %s", name)
            return results

    return []


def _ddg_search_library(query: str, max_results: int = 8) -> list[dict]:
    """Search using the duckduckgo-search Python library."""
    for attempt in range(2):
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
                logger.debug("DDGS library OK: %r → %d results", query, len(results))
            return results
        except Exception as exc:
            logger.debug("DDGS library error for %r (attempt %d): %s", query, attempt + 1, exc)
            global _ddgs
            _ddgs = None
            if attempt < 1:
                time.sleep(3)
    return []


def _google_search(query: str, max_results: int = 8) -> list[dict]:
    """Search Google by scraping the HTML results page."""
    if not _HAS_BS4:
        return []

    encoded_q = quote_plus(query)
    url = f"https://www.google.com/search?q={encoded_q}&num={max_results}&hl=en"

    for attempt in range(3):
        try:
            client = _get_client()
            resp = client.get(url)

            # If blocked, rotate user agent and retry
            if resp.status_code in (429, 503) or resp.status_code >= 400:
                logger.debug("Google returned %d for %r — rotating UA (attempt %d)",
                             resp.status_code, query, attempt + 1)
                _fresh_client()
                time.sleep((attempt + 1) * 3)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            results = []

            # Method 1: Standard Google result divs
            for div in soup.select("div.g"):
                link = div.select_one("a[href]")
                if not link:
                    continue
                href = link.get("href", "")
                if not href.startswith("http"):
                    continue
                # Skip Google's own URLs
                if "google.com" in href or "googleapis.com" in href:
                    continue

                title_el = div.select_one("h3")
                title = title_el.get_text(strip=True) if title_el else ""

                # Try multiple snippet selectors
                snippet = ""
                for sel in [".VwiC3b", "div[data-sncf]", ".IsZvec", "span.st"]:
                    snip_el = div.select_one(sel)
                    if snip_el:
                        snippet = snip_el.get_text(strip=True)
                        break

                results.append({
                    "url": href,
                    "title": title,
                    "snippet": snippet,
                })
                if len(results) >= max_results:
                    break

            # Method 2: If method 1 found nothing, try broader link extraction
            if not results:
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    # Google wraps some results in /url?q=...
                    if href.startswith("/url?q="):
                        actual = href.split("/url?q=")[1].split("&")[0]
                        href = unquote(actual)
                    if not href.startswith("http"):
                        continue
                    if "google.com" in href or "googleapis.com" in href:
                        continue
                    if any(d in href for d in ["instagram.com", "facebook.com",
                                               "tiktok.com", "yelp.com"]):
                        title = a_tag.get_text(strip=True)
                        results.append({
                            "url": href,
                            "title": title,
                            "snippet": "",
                        })
                        if len(results) >= max_results:
                            break

            if results:
                logger.debug("Google search OK: %r → %d results", query, len(results))
                return results

            # Check if we got a CAPTCHA page
            page_text = resp.text.lower()
            if "captcha" in page_text or "unusual traffic" in page_text:
                logger.debug("Google CAPTCHA detected for %r", query)
                _fresh_client()
                time.sleep((attempt + 1) * 5)
                continue

            # Got a valid page but no results extracted
            logger.debug("Google returned page but 0 results parsed for %r", query)
            return []

        except httpx.HTTPError as exc:
            logger.debug("Google HTTP error for %r: %s", query, exc)
            if attempt < 2:
                time.sleep((attempt + 1) * 2)
            continue

    return []


def _bing_search(query: str, max_results: int = 8) -> list[dict]:
    """Search Bing by scraping HTML results — most reliable scraping target."""
    if not _HAS_BS4:
        return []

    encoded_q = quote_plus(query)
    url = f"https://www.bing.com/search?q={encoded_q}&count={max_results}"

    for attempt in range(3):
        try:
            client = _get_client()
            resp = client.get(url)

            if resp.status_code in (429, 503) or resp.status_code >= 400:
                logger.debug("Bing returned %d for %r (attempt %d)",
                             resp.status_code, query, attempt + 1)
                _fresh_client()
                time.sleep((attempt + 1) * 2)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            results = []

            # Method 1: Standard Bing result items
            for li in soup.select("li.b_algo"):
                link = li.select_one("h2 a[href]")
                if not link:
                    link = li.select_one("a[href]")
                if not link:
                    continue
                href = link.get("href", "")
                if not href.startswith("http"):
                    continue
                if "bing.com" in href or "microsoft.com" in href:
                    continue

                title = link.get_text(strip=True)

                snippet = ""
                snip_el = li.select_one(".b_caption p")
                if not snip_el:
                    snip_el = li.select_one("p")
                if snip_el:
                    snippet = snip_el.get_text(strip=True)

                results.append({
                    "url": href,
                    "title": title,
                    "snippet": snippet,
                })
                if len(results) >= max_results:
                    break

            # Method 2: Broader link extraction if method 1 found nothing
            if not results:
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if not href.startswith("http"):
                        continue
                    if "bing.com" in href or "microsoft.com" in href:
                        continue
                    if any(d in href for d in ["instagram.com", "facebook.com",
                                               "tiktok.com", "yelp.com"]):
                        title = a_tag.get_text(strip=True)
                        results.append({
                            "url": href,
                            "title": title,
                            "snippet": "",
                        })
                        if len(results) >= max_results:
                            break

            if results:
                logger.debug("Bing search OK: %r → %d results", query, len(results))
                return results

            # Check for CAPTCHA
            page_text = resp.text.lower()
            if "captcha" in page_text or "unusual traffic" in page_text:
                logger.debug("Bing CAPTCHA detected for %r", query)
                _fresh_client()
                time.sleep((attempt + 1) * 4)
                continue

            logger.debug("Bing returned page but 0 results parsed for %r", query)
            return []

        except httpx.HTTPError as exc:
            logger.debug("Bing HTTP error for %r: %s", query, exc)
            if attempt < 2:
                time.sleep((attempt + 1) * 2)
            continue

    return []


def _ddg_search_html(query: str, max_results: int = 8) -> list[dict]:
    """Fallback: scrape DuckDuckGo HTML lite directly."""
    if not _HAS_BS4:
        return []

    url = "https://html.duckduckgo.com/html/"

    for attempt in range(2):
        try:
            client = _get_client()
            resp = client.post(url, data={"q": query, "b": ""})
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.debug("DDG HTML failed for %r: %s", query, exc)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        page_text = resp.text.lower()
        if ("please try again" in page_text
                or "blocked" in page_text
                or "unusual traffic" in page_text
                or "robot" in page_text):
            logger.debug("DDG HTML rate-limited on %r (attempt %d)", query, attempt + 1)
            time.sleep((attempt + 1) * 5)
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

        if results:
            return results

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
    cleaned = re.sub(r"[''`]", "", name.lower())
    cleaned = re.sub(r"\b(llc|inc|corp|ltd|the|and|of|restaurant|bar|grill|cafe|café|"
                      r"sports|kitchen|house|place|shop|store|lounge|club)\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s]", "", cleaned)
    return cleaned.strip()


def _calc_confidence(query_index: int, result_index: int) -> str:
    """Estimate confidence based on which query and result position matched."""
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
    significant = [w for w in name_words if len(w) >= 3]
    if not significant:
        return True
    matches = sum(1 for w in significant if w in url_lower)
    if matches == 0:
        logger.debug("Name match failed: words=%s not in URL %s", significant, url_lower)
    return matches >= 1


def _find_instagram(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's Instagram profile. Returns (url, confidence)."""
    queries = [
        f"site:instagram.com {biz_name} {city}",
        f"{biz_name} {city} instagram",
    ]
    if city:
        queries.append(f"site:instagram.com {biz_name}")
        queries.append(f"{biz_name} instagram")
    for qi, query in enumerate(queries):
        results = _web_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
            url = r["url"]
            parsed = urlparse(url)
            if "instagram.com" not in parsed.netloc:
                continue
            path = parsed.path.strip("/")
            if not path or "/" in path:
                continue
            if _INSTAGRAM_REJECT.search(url):
                continue
            if _name_matches(url, biz_name):
                clean = f"https://instagram.com/{path}"
                confidence = _calc_confidence(qi, ri)
                logger.info("    Found Instagram (%s): %s", confidence, clean)
                return clean, confidence
            else:
                logger.debug("    Instagram rejected (name mismatch): %s", url)
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
        results = _web_search(query, max_results=5)
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
                logger.info("    Found Facebook (%s): %s", confidence, clean)
                return clean, confidence
            else:
                logger.debug("    Facebook rejected (name mismatch): %s", url)
    return "", ""


def _find_tiktok(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's TikTok profile. Returns (url, confidence)."""
    queries = [f"site:tiktok.com {biz_name} {city}"]
    if city:
        queries.append(f"site:tiktok.com {biz_name}")
    for qi, query in enumerate(queries):
        results = _web_search(query, max_results=5)
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
                continue
            if _name_matches(url, biz_name):
                clean = f"https://tiktok.com/{path}"
                confidence = _calc_confidence(qi, ri)
                logger.info("    Found TikTok (%s): %s", confidence, clean)
                return clean, confidence
    return "", ""


def _find_yelp(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's Yelp page. Returns (url, confidence)."""
    queries = [f"site:yelp.com {biz_name} {city}"]
    if city:
        queries.append(f"site:yelp.com {biz_name}")
    for qi, query in enumerate(queries):
        results = _web_search(query, max_results=5)
        _rate_limit()
        for ri, r in enumerate(results):
            url = r["url"]
            parsed = urlparse(url)
            if "yelp.com" not in parsed.netloc:
                continue
            if "/biz/" not in parsed.path:
                continue
            if _name_matches(url, biz_name):
                confidence = _calc_confidence(qi, ri)
                logger.info("    Found Yelp (%s): %s", confidence, url)
                return url, confidence
    return "", ""


def _find_email(biz_name: str, city: str) -> tuple[str, str]:
    """Search for the business's email address in search snippets."""
    queries = [
        f"{biz_name} {city} email",
        f"{biz_name} {city} contact email",
    ]
    found_emails: list[str] = []

    for query in queries:
        results = _web_search(query, max_results=5)
        _rate_limit()
        for r in results:
            text = f"{r['title']} {r['snippet']}"
            emails = _EMAIL_RE.findall(text)
            for email in emails:
                domain = email.split("@")[1].lower()
                if domain in _JUNK_EMAIL_DOMAINS:
                    continue
                local = email.split("@")[0].lower()
                if local in ("info", "noreply", "no-reply", "example", "test"):
                    continue
                found_emails.append(email.lower())

        if found_emails:
            break

    if found_emails:
        best = max(set(found_emails), key=found_emails.count)
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

    found = {}

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
            info.instagram_confidence = "high"
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
    """
    global _search_backend
    total = len(businesses)
    results: dict[int, ContactInfo] = {}

    # --- Connectivity test: try all backends to find one that works ---
    logger.info("Testing search backends ...")
    _search_backend = "unknown"

    test_query = "Knuckleheads Sports Bar instagram"  # realistic query

    # Test DDGS library
    if _HAS_DDGS:
        logger.info("  Testing duckduckgo-search library ...")
        test = _ddg_search_library(test_query, max_results=2)
        if test:
            _search_backend = "ddgs"
            logger.info("  ✓ duckduckgo-search library works (%d results)", len(test))
        else:
            logger.info("  ✗ duckduckgo-search library: 0 results")
    else:
        logger.info("  – duckduckgo-search library not installed")

    # Test Bing HTML scraping (most reliable)
    if _search_backend == "unknown" and _HAS_BS4:
        logger.info("  Testing Bing HTML scraping ...")
        test = _bing_search(test_query, max_results=2)
        if test:
            _search_backend = "bing"
            logger.info("  ✓ Bing HTML scraping works (%d results)", len(test))
        else:
            logger.info("  ✗ Bing HTML scraping: 0 results")

    # Test Google HTML
    if _search_backend == "unknown" and _HAS_BS4:
        logger.info("  Testing Google HTML scraping ...")
        test = _google_search(test_query, max_results=2)
        if test:
            _search_backend = "google"
            logger.info("  ✓ Google HTML scraping works (%d results)", len(test))
        else:
            logger.info("  ✗ Google HTML scraping: 0 results")

    # Test DDG HTML
    if _search_backend == "unknown" and _HAS_BS4:
        logger.info("  Testing DuckDuckGo HTML scraping ...")
        test = _ddg_search_html(test_query, max_results=2)
        if test:
            _search_backend = "ddg_html"
            logger.info("  ✓ DuckDuckGo HTML scraping works (%d results)", len(test))
        else:
            logger.info("  ✗ DuckDuckGo HTML scraping: 0 results")

    if not _HAS_BS4:
        logger.warning("beautifulsoup4 is not installed — HTML scraping backends unavailable")

    if _search_backend == "unknown":
        logger.warning("ALL search backends failed — contact discovery will return empty results")
        logger.warning("This usually means search engines are blocking requests.")
        logger.warning("Try again in a few minutes, or check your internet connection.")
        _search_backend = "none"
    else:
        logger.info("Using search backend: %s", _search_backend)

    for i, biz in enumerate(businesses):
        name = biz.get("business_name", "?")
        logger.info("Contact discovery [%d/%d]: %s", i + 1, total, name)

        try:
            info = discover_contacts(biz)
            results[i] = info
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
            logger.info("  → %s", summary)

            if progress_callback:
                progress_callback(i, total, name, info)

        except Exception as exc:
            logger.error("  → Error discovering contacts for %s: %s", name, exc)
            results[i] = ContactInfo()
            if progress_callback:
                progress_callback(i, total, name, results[i])

    found_any = sum(1 for c in results.values() if c.contact_methods_found > 0)
    logger.info("Contact discovery complete: %d/%d businesses have contacts (backend: %s)",
                found_any, total, _search_backend)

    return results
