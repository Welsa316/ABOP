"""
analyzer.py — Website discovery and reachability analysis.

For businesses without a listed website, attempts to discover one via
DNS resolution and HTTP checks on common domain patterns.

Categorises every business into one of three tiers:
  - "listed"     — website was provided in the CSV / GBP data
  - "discovered" — found a live website via automated search
  - "not_found"  — could not find any website
"""

import asyncio
import logging
import re
import socket
from dataclasses import dataclass, field
from urllib.parse import urlparse

import httpx

from .config import REQUEST_TIMEOUT, MAX_CONCURRENT_REQUESTS

logger = logging.getLogger("lead_engine")

# Business-name suffixes to strip when generating domain candidates
_STRIP_SUFFIXES = re.compile(
    r"\b(llc|inc|incorporated|corp|corporation|co|company|ltd|limited|"
    r"group|pllc|plc|pa|pc|lp|llp|dba|the)\b",
    re.IGNORECASE,
)

# Characters that aren't part of a domain slug
_NON_ALPHA = re.compile(r"[^a-z0-9]+")


@dataclass
class SiteAnalysis:
    """Result of analysing one business's web presence."""
    reachable: bool = False
    status_code: int = 0
    url: str = ""
    website_status: str = "not_found"  # "listed" | "discovered" | "not_found"
    error: str = ""


# ------------------------------------------------------------------
# Domain candidate generation
# ------------------------------------------------------------------

def _slug(name: str) -> str:
    """Turn a business name into a domain-safe slug."""
    name = _STRIP_SUFFIXES.sub("", name.lower())
    return _NON_ALPHA.sub("", name)


def _generate_candidates(biz: dict) -> list[str]:
    """
    Generate plausible domain names for a business.

    Examples for "Thompson Law Firm":
        thompsonlawfirm.com, thompsonlawfirm.net,
        thompsonlaw.com, etc.
    """
    name = biz.get("business_name", "")
    if not name:
        return []

    slug = _slug(name)
    if not slug:
        return []

    # Individual words (stripped of suffixes)
    words = _NON_ALPHA.sub(" ", _STRIP_SUFFIXES.sub("", name.lower())).split()
    words = [w for w in words if len(w) > 1]

    candidates = []

    # Full slug
    candidates.append(f"{slug}.com")
    candidates.append(f"{slug}.net")

    # First word + last word (if different from full slug)
    if len(words) >= 2:
        short = words[0] + words[-1]
        if short != slug:
            candidates.append(f"{short}.com")

    # First word only (common for single-word brands)
    if len(words) >= 1 and words[0] != slug:
        candidates.append(f"{words[0]}.com")

    # With city appended
    city = biz.get("city", "").strip().lower()
    city_slug = _NON_ALPHA.sub("", city)
    if city_slug and len(words) >= 1:
        candidates.append(f"{words[0]}{city_slug}.com")

    # De-duplicate while preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


# ------------------------------------------------------------------
# DNS + HTTP checks
# ------------------------------------------------------------------

async def _dns_resolve(domain: str) -> bool:
    """Check if a domain resolves via DNS. Fast and free."""
    loop = asyncio.get_event_loop()
    try:
        await loop.getaddrinfo(domain, 80, family=socket.AF_INET)
        return True
    except (socket.gaierror, OSError):
        return False


async def _http_check(client: httpx.AsyncClient, url: str) -> tuple[bool, int]:
    """
    Quick HTTP HEAD check.  Returns (reachable, status_code).
    Parked / dead domains typically return errors or non-200 codes.
    """
    try:
        resp = await client.head(url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
        # Accept any 2xx or 3xx as "live"
        return (resp.status_code < 400, resp.status_code)
    except (httpx.HTTPError, httpx.InvalidURL, Exception):
        # Try GET as fallback (some servers reject HEAD)
        try:
            resp = await client.get(url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
            return (resp.status_code < 400, resp.status_code)
        except Exception:
            return (False, 0)


async def _discover_website(
    client: httpx.AsyncClient,
    biz: dict,
    sem: asyncio.Semaphore,
) -> SiteAnalysis:
    """Try to discover a website for a business that has no listed URL."""
    candidates = _generate_candidates(biz)
    if not candidates:
        return SiteAnalysis(website_status="not_found")

    for domain in candidates:
        async with sem:
            if not await _dns_resolve(domain):
                continue

            url = f"http://{domain}"
            reachable, code = await _http_check(client, url)
            if reachable:
                # Use the final URL (may have redirected to https)
                logger.info("Discovered website for %s: %s",
                            biz.get("business_name", "?"), domain)
                return SiteAnalysis(
                    reachable=True,
                    status_code=code,
                    url=f"http://{domain}",
                    website_status="discovered",
                )

    return SiteAnalysis(website_status="not_found")


async def _check_listed_website(
    client: httpx.AsyncClient,
    biz: dict,
    sem: asyncio.Semaphore,
) -> SiteAnalysis:
    """Verify that a business's listed website is reachable."""
    url = biz.get("website", "")
    async with sem:
        reachable, code = await _http_check(client, url)
        return SiteAnalysis(
            reachable=reachable,
            status_code=code,
            url=url,
            website_status="listed",
        )


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

async def analyze_websites(
    businesses: list[dict],
    max_concurrent: int | None = None,
) -> dict[int, SiteAnalysis]:
    """
    Analyse every business's web presence.

    - Businesses WITH a listed website get a reachability check.
    - Businesses WITHOUT a listed website get a discovery attempt.

    Sets ``biz["website_status"]`` on each business dict and
    populates ``biz["website"]`` if a site is discovered.

    Returns a dict keyed by business index → SiteAnalysis.
    """
    concurrency = max_concurrent or MAX_CONCURRENT_REQUESTS
    sem = asyncio.Semaphore(concurrency)
    analyses: dict[int, SiteAnalysis] = {}

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": "LeadEngine/1.0 (website-check)"},
    ) as client:

        tasks = []
        for i, biz in enumerate(businesses):
            if biz.get("website"):
                tasks.append((i, _check_listed_website(client, biz, sem)))
            else:
                tasks.append((i, _discover_website(client, biz, sem)))

        # Run all tasks concurrently
        coros = [coro for _, coro in tasks]
        results = await asyncio.gather(*coros, return_exceptions=True)

        for (idx, _), result in zip(tasks, results):
            biz = businesses[idx]
            if isinstance(result, Exception):
                logger.warning("Analysis failed for %s: %s",
                               biz.get("business_name", "?"), result)
                analysis = SiteAnalysis(
                    website_status="listed" if biz.get("website") else "not_found",
                    error=str(result),
                )
            else:
                analysis = result

            analyses[idx] = analysis

            # Update business dict with results
            biz["website_status"] = analysis.website_status
            if analysis.website_status == "discovered" and analysis.url:
                biz["website"] = analysis.url

    listed = sum(1 for a in analyses.values() if a.website_status == "listed")
    discovered = sum(1 for a in analyses.values() if a.website_status == "discovered")
    not_found = sum(1 for a in analyses.values() if a.website_status == "not_found")
    logger.info("Analysis complete: %d listed, %d discovered, %d not found",
                listed, discovered, not_found)

    return analyses
