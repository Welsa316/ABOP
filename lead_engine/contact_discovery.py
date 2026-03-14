"""
contact_discovery.py — Email discovery for business leads.

Finds primary contact emails using two free methods:
  1. Scraping the business's own website (contact/about pages)
  2. DuckDuckGo search as a fallback

No paid services or API keys required.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

from . import config

logger = logging.getLogger("lead_engine")

# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# Junk email prefixes and domains to skip
_JUNK_PREFIXES = {
    "noreply", "no-reply", "donotreply", "mailer-daemon", "postmaster",
    "webmaster", "admin", "root", "test", "example", "support",
    "abuse", "hostmaster",
}

_JUNK_DOMAINS = {
    "example.com", "example.org", "test.com",
    "wix.com", "wixpress.com", "squarespace.com", "wordpress.com",
    "wordpress.org", "godaddy.com", "sentry.io", "googleapis.com",
    "googleusercontent.com", "gstatic.com", "w3.org", "schema.org",
    "facebook.com", "twitter.com", "instagram.com", "linkedin.com",
    "youtube.com", "tiktok.com",
}


def _is_junk_email(email: str) -> bool:
    """Return True if the email is likely junk / not a real contact."""
    email = email.lower().strip()
    local, _, domain = email.partition("@")
    if not domain:
        return True
    if local in _JUNK_PREFIXES:
        return True
    if domain in _JUNK_DOMAINS:
        return True
    # Skip image filenames that look like emails
    if email.endswith((".png", ".jpg", ".gif", ".svg", ".webp")):
        return True
    return False


def _extract_emails(text: str) -> list[str]:
    """Extract valid, non-junk emails from text."""
    raw = EMAIL_RE.findall(text)
    seen = set()
    result = []
    for e in raw:
        e_lower = e.lower().strip()
        if e_lower not in seen and not _is_junk_email(e_lower):
            seen.add(e_lower)
            result.append(e_lower)
    return result


# ---------------------------------------------------------------------------
# Contact info dataclass
# ---------------------------------------------------------------------------
@dataclass
class ContactInfo:
    """Contact information discovered for a business."""
    email: str = ""
    email_confidence: str = ""  # "high" | "medium" | ""
    contact_methods_found: int = 0
    best_contact_channel: str = ""


# ---------------------------------------------------------------------------
# Method 1: Scrape the business website
# ---------------------------------------------------------------------------
# Subpages to check for contact info
_CONTACT_PATHS = [
    "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/staff", "/support", "/get-in-touch",
    "/info", "/connect", "/our-team", "/people",
]

# Keywords for discovering contact-related links on the homepage
_CONTACT_LINK_KEYWORDS = {
    "contact", "about", "team", "staff", "people", "support",
    "get-in-touch", "reach", "connect", "email", "inquiry",
    "enquiry", "info", "our-team",
}


def _find_contact_links(soup, base_url: str, existing_urls: set[str]) -> list[str]:
    """
    Scan a parsed page for links that look like contact/about pages.
    Returns up to 5 new URLs not already in existing_urls.
    """
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()
    discovered = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        # Same domain only
        if parsed.netloc.lower() != base_domain:
            continue

        # Check if href path or anchor text contains a contact keyword
        path_lower = parsed.path.lower()
        text_lower = (a_tag.get_text() or "").lower()
        combined = f"{path_lower} {text_lower}"

        if any(kw in combined for kw in _CONTACT_LINK_KEYWORDS):
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if clean not in existing_urls and clean not in discovered:
                discovered.append(clean)
                if len(discovered) >= 5:
                    break

    return discovered


async def _scrape_website_emails(
    website_url: str,
    timeout: int,
    semaphore: asyncio.Semaphore,
) -> list[str]:
    """
    Scrape a business website for email addresses.
    Checks the homepage, common contact/about pages, and dynamically
    discovered links that look like contact pages.
    """
    import httpx

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 not installed — skipping website scraping")
        return []

    all_emails: list[str] = []
    urls_to_check = [website_url]

    # Add common contact page URLs
    parsed = urlparse(website_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in _CONTACT_PATHS:
        urls_to_check.append(urljoin(base, path))

    async with semaphore:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            verify=False,
        ) as client:
            for idx, url in enumerate(urls_to_check):
                try:
                    resp = await client.get(url)
                    if resp.status_code >= 400:
                        continue
                    content_type = resp.headers.get("content-type", "")
                    if "text/html" not in content_type:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # On the homepage, discover additional contact links
                    if idx == 0:
                        existing = set(urls_to_check)
                        extra_links = _find_contact_links(soup, base, existing)
                        urls_to_check.extend(extra_links)

                    # Check mailto: links first (most reliable)
                    for a_tag in soup.find_all("a", href=True):
                        href = a_tag["href"]
                        if href.startswith("mailto:"):
                            email = href[7:].split("?")[0].strip()
                            if email and not _is_junk_email(email):
                                all_emails.append(email.lower())
                    # Also check page text
                    page_text = soup.get_text(separator=" ")
                    all_emails.extend(_extract_emails(page_text))
                except Exception:
                    continue

    # Deduplicate preserving order
    seen = set()
    deduped = []
    for e in all_emails:
        if e not in seen:
            seen.add(e)
            deduped.append(e)
    return deduped


# ---------------------------------------------------------------------------
# Method 2: DuckDuckGo search
# ---------------------------------------------------------------------------
async def _search_emails_ddg(
    business_name: str,
    city: str,
) -> tuple[list[str], str]:
    """
    Search DuckDuckGo for a business's contact email.
    Returns (emails, confidence) where confidence is "high" or "medium".
    """
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        logger.warning("duckduckgo-search not installed — skipping DDG search")
        return [], ""

    query = f'"{business_name}" "{city}" email contact'
    emails_count: dict[str, int] = {}

    try:
        loop = asyncio.get_event_loop()
        # Run synchronous DDGS in executor to not block
        def _do_search():
            results = []
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=8):
                    results.append(r)
            return results

        results = await loop.run_in_executor(None, _do_search)

        for r in results:
            text = f"{r.get('title', '')} {r.get('body', '')} {r.get('href', '')}"
            found = _extract_emails(text)
            for e in found:
                emails_count[e] = emails_count.get(e, 0) + 1

    except Exception as exc:
        logger.debug("DDG search failed for %s: %s", business_name, exc)
        return [], ""

    if not emails_count:
        return [], ""

    # Sort by frequency (most common first)
    sorted_emails = sorted(emails_count.keys(), key=lambda e: -emails_count[e])
    # Confidence: high if found in 2+ results
    top_email = sorted_emails[0]
    confidence = "high" if emails_count[top_email] >= 2 else "medium"

    return sorted_emails, confidence


# ---------------------------------------------------------------------------
# Method 3: Google search (supplements DDG)
# ---------------------------------------------------------------------------
async def _search_emails_google(
    business_name: str,
    city: str,
) -> tuple[list[str], str]:
    """
    Search Google for a business's contact email.
    Returns (emails, confidence). Requires googlesearch-python (optional).
    """
    try:
        from googlesearch import search as google_search
    except ImportError:
        # Optional dependency — skip silently
        return [], ""

    query = f'"{business_name}" "{city}" email contact'
    emails_count: dict[str, int] = {}

    try:
        loop = asyncio.get_event_loop()

        def _do_search():
            results = []
            for url in google_search(query, num_results=8):
                results.append(url)
            return results

        urls = await loop.run_in_executor(None, _do_search)

        # Extract emails from the URLs themselves (often contain email in URL params)
        for url in urls:
            found = _extract_emails(url)
            for e in found:
                emails_count[e] = emails_count.get(e, 0) + 1

    except Exception as exc:
        logger.debug("Google search failed for %s: %s", business_name, exc)
        return [], ""

    if not emails_count:
        return [], ""

    sorted_emails = sorted(emails_count.keys(), key=lambda e: -emails_count[e])
    top_email = sorted_emails[0]
    confidence = "high" if emails_count[top_email] >= 2 else "medium"

    return sorted_emails, confidence


# ---------------------------------------------------------------------------
# Single business discovery
# ---------------------------------------------------------------------------
async def _discover_contacts(
    biz: dict,
    timeout: int,
    semaphore: asyncio.Semaphore,
) -> ContactInfo:
    """Discover contact email for a single business."""
    name = biz.get("business_name", "")
    city = biz.get("city", "")
    website = biz.get("website", "")

    all_emails: list[str] = []
    confidence = ""

    # Method 1: Scrape website
    if website:
        try:
            scraped = await _scrape_website_emails(website, timeout, semaphore)
            all_emails.extend(scraped)
            if scraped:
                confidence = "high"  # Found on their own site
        except Exception as exc:
            logger.debug("Website scrape failed for %s: %s", name, exc)

    # Method 2: DuckDuckGo search (always try, to supplement/verify)
    try:
        ddg_emails, ddg_confidence = await _search_emails_ddg(name, city)
        for e in ddg_emails:
            if e not in all_emails:
                all_emails.append(e)
        # Upgrade confidence if DDG confirms
        if not confidence and ddg_confidence:
            confidence = ddg_confidence
        elif confidence == "medium" and ddg_confidence == "high":
            confidence = "high"
    except Exception as exc:
        logger.debug("DDG search failed for %s: %s", name, exc)

    # Method 3: Google search (supplements DDG)
    try:
        google_emails, google_confidence = await _search_emails_google(name, city)
        for e in google_emails:
            if e not in all_emails:
                all_emails.append(e)
        if not confidence and google_confidence:
            confidence = google_confidence
        elif confidence == "medium" and google_confidence == "high":
            confidence = "high"
    except Exception as exc:
        logger.debug("Google search failed for %s: %s", name, exc)

    if not all_emails:
        return ContactInfo()

    # Prefer emails whose domain matches the business website
    best_email = all_emails[0]
    if website:
        site_domain = urlparse(website).netloc.lower().removeprefix("www.")
        for e in all_emails:
            email_domain = e.split("@")[1] if "@" in e else ""
            if email_domain == site_domain or email_domain == f"www.{site_domain}":
                best_email = e
                break
    return ContactInfo(
        email=best_email,
        email_confidence=confidence or "medium",
        contact_methods_found=1,
        best_contact_channel="email",
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
async def _discover_all_async(
    businesses: list[dict],
) -> dict[int, ContactInfo]:
    """Async implementation of discover_all_contacts."""
    concurrency = config.EMAIL_MAX_CONCURRENT
    timeout = config.EMAIL_REQUEST_TIMEOUT
    semaphore = asyncio.Semaphore(concurrency)

    tasks = []
    for i, biz in enumerate(businesses):
        tasks.append(_discover_contacts(biz, timeout, semaphore))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    contacts: dict[int, ContactInfo] = {}
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.warning("Contact discovery failed for %s: %s",
                           businesses[i].get("business_name"), result)
            contacts[i] = ContactInfo()
        else:
            contacts[i] = result

    return contacts


def discover_all_contacts(businesses: list[dict]) -> dict[int, ContactInfo]:
    """
    Discover contact emails for all businesses.

    Uses website scraping and DuckDuckGo search (both free).
    Returns dict mapping business index -> ContactInfo.

    Called by run.py Stage 3.
    """
    return asyncio.run(_discover_all_async(businesses))
