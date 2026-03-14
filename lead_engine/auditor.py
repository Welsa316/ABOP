"""
auditor.py — AI-powered website content audit.

Fetches HTML from reachable websites, extracts structural signals
locally with BeautifulSoup, then sends structured data to Claude
for a concise audit summary. The summary is stored on each business
dict as ``website_audit`` and fed into the message generation prompt
so outreach references real issues (not generic guesses).
"""

import asyncio
import logging
import re

import httpx

from . import config

logger = logging.getLogger("lead_engine")

# Common CTA phrases to detect
_CTA_PATTERNS = re.compile(
    r"\b(contact\s+us|book\s+(now|online|a|an)|schedule|get\s+started|"
    r"free\s+(quote|consultation|estimate)|request\s+a?\s*quote|call\s+us|"
    r"get\s+in\s+touch)\b",
    re.IGNORECASE,
)


# ------------------------------------------------------------------
# HTML extraction (no AI cost)
# ------------------------------------------------------------------

def _extract_signals(html: str, url: str) -> dict:
    """Extract structural signals from raw HTML using BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("bs4 not installed — skipping HTML extraction")
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # Page title
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Meta description
    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag:
        meta_desc = meta_tag.get("content", "")

    # H1 headings
    h1s = [h.get_text(strip=True) for h in soup.find_all("h1")]

    # Visible body text (first 500 chars)
    body = soup.find("body")
    if body:
        # Remove script/style tags
        for tag in body.find_all(["script", "style", "noscript"]):
            tag.decompose()
        text = body.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        text_preview = text[:500]
    else:
        text_preview = ""

    # Contact form detection
    has_form = bool(soup.find("form"))

    # CTA detection
    page_text = soup.get_text(separator=" ", strip=True)
    has_cta = bool(_CTA_PATTERNS.search(page_text))

    # Mobile viewport
    viewport = soup.find("meta", attrs={"name": "viewport"})
    has_viewport = bool(viewport)

    # HTTPS
    uses_https = url.startswith("https")

    return {
        "title": title,
        "meta_description": meta_desc,
        "h1_headings": h1s[:3],  # limit to first 3
        "text_preview": text_preview,
        "has_contact_form": has_form,
        "has_cta": has_cta,
        "has_mobile_viewport": has_viewport,
        "uses_https": uses_https,
    }


# ------------------------------------------------------------------
# AI audit via Claude
# ------------------------------------------------------------------

def _build_audit_prompt(signals: dict, biz: dict) -> str:
    """Build a concise prompt for Claude to audit the website."""
    name = biz.get("business_name", "Unknown")
    category = biz.get("primary_category", biz.get("category", ""))

    parts = [
        f"Business: {name}",
        f"Category: {category}" if category else "",
        f"Page title: {signals.get('title', 'None')}",
        f"Meta description: {signals.get('meta_description', 'None')}",
        f"H1 headings: {', '.join(signals.get('h1_headings', [])) or 'None'}",
        f"Has contact form: {'Yes' if signals.get('has_contact_form') else 'No'}",
        f"Has call-to-action: {'Yes' if signals.get('has_cta') else 'No'}",
        f"Mobile-responsive (viewport tag): {'Yes' if signals.get('has_mobile_viewport') else 'No'}",
        f"Uses HTTPS: {'Yes' if signals.get('uses_https') else 'No'}",
        "",
        f"Page text preview: {signals.get('text_preview', '')[:300]}",
    ]

    site_data = "\n".join(p for p in parts if p or p == "")

    return f"""Analyze this business website and provide a brief audit.

SITE DATA:
{site_data}

Respond with ONLY a 2-3 sentence summary covering:
1. What the site communicates about the business
2. One or two specific weaknesses or missing elements (e.g. no online booking, no contact form, outdated look, missing service descriptions, no reviews/testimonials, poor SEO metadata)

Be specific and factual based on the data above. Do not speculate beyond what the data shows. Keep it under 50 words total."""


async def _ai_audit(signals: dict, biz: dict, sem: asyncio.Semaphore) -> str:
    """Call Claude to generate a website audit summary."""
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed")
        return ""

    if not config.ANTHROPIC_API_KEY:
        return ""

    prompt = _build_audit_prompt(signals, biz)

    async with sem:
        try:
            client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
            response = client.messages.create(
                model=config.AUDIT_MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.warning("Audit API error for %s: %s",
                           biz.get("business_name", "?"), e)
            return ""


# ------------------------------------------------------------------
# Fetch + audit pipeline
# ------------------------------------------------------------------

async def _audit_one(
    client: httpx.AsyncClient,
    biz: dict,
    url: str,
    ai_sem: asyncio.Semaphore,
    http_sem: asyncio.Semaphore,
) -> str:
    """Fetch HTML and run AI audit for a single business."""
    # Fetch HTML
    async with http_sem:
        try:
            resp = await client.get(
                url, follow_redirects=True, timeout=config.REQUEST_TIMEOUT
            )
            if resp.status_code >= 400:
                logger.debug("HTTP %d for %s", resp.status_code, url)
                return ""
            html = resp.text
        except Exception as e:
            logger.debug("Failed to fetch %s: %s", url, e)
            return ""

    # Extract signals locally
    signals = _extract_signals(html, url)
    if not signals:
        return ""

    # Store extracted booleans on biz dict for scorer
    biz["has_contact_form"] = signals.get("has_contact_form", False)
    biz["has_mobile_viewport"] = signals.get("has_mobile_viewport", False)

    # AI audit
    return await _ai_audit(signals, biz, ai_sem)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

async def audit_websites(businesses: list[dict], analyses: dict) -> None:
    """
    Audit website content for all reachable businesses.

    Fetches HTML, extracts signals, and calls Claude for a summary.
    Sets ``biz["website_audit"]`` on each business dict.

    Args:
        businesses: List of business dicts from the pipeline.
        analyses: Dict of index → SiteAnalysis from analyzer stage.
    """
    ai_sem = asyncio.Semaphore(config.AUDIT_CONCURRENCY)
    http_sem = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

    # Identify reachable sites
    to_audit = []
    for i, biz in enumerate(businesses):
        analysis = analyses.get(i)
        if analysis and analysis.reachable and biz.get("website"):
            to_audit.append((i, biz))
        else:
            biz["website_audit"] = ""

    if not to_audit:
        logger.info("No reachable websites to audit")
        return

    logger.info("Auditing %d reachable websites ...", len(to_audit))

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=config.REQUEST_TIMEOUT,
        headers={"User-Agent": "LeadEngine/1.0 (website-audit)"},
    ) as client:
        tasks = []
        for i, biz in to_audit:
            url = biz["website"]
            if not url.startswith("http"):
                url = f"http://{url}"
            tasks.append((i, _audit_one(client, biz, url, ai_sem, http_sem)))

        coros = [coro for _, coro in tasks]
        results = await asyncio.gather(*coros, return_exceptions=True)

        success = 0
        for (idx, _), result in zip(tasks, results):
            biz = businesses[idx]
            if isinstance(result, Exception):
                logger.warning("Audit failed for %s: %s",
                               biz.get("business_name", "?"), result)
                biz["website_audit"] = ""
            else:
                biz["website_audit"] = result or ""
                if result:
                    success += 1

    logger.info("Audited %d/%d websites successfully", success, len(to_audit))
