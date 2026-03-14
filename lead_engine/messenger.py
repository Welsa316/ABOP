"""
messenger.py — AI outreach message generation using Claude.

Generates personalised outreach messages (email, contact-form, DM,
follow-up, and call script) for each qualifying business lead.

Called by run.py Stage 5.
"""

import json
import logging

from . import config

logger = logging.getLogger("lead_engine")

# The six message fields that run.py expects on every business dict.
_MESSAGE_FIELDS = (
    "email_subject",
    "email_message",
    "contact_form_message",
    "dm_message",
    "follow_up_message",
    "call_script",
)


def _build_prompt(biz: dict) -> str:
    """Build a Claude prompt using real business data for personalisation."""

    name = biz.get("business_name", "the business")
    website = biz.get("website", "")
    city = biz.get("city", "")
    category = biz.get("primary_category", biz.get("category", ""))
    rating = biz.get("rating", 0)
    reviews = biz.get("review_count", 0)
    website_status = biz.get("website_status", "")
    email = biz.get("email", "")

    # Build context about the lead
    ctx = []
    if website:
        ctx.append(f"Website: {website} (status: {website_status or 'listed'}).")
    else:
        ctx.append("No website found for this business.")
    if city:
        ctx.append(f"Located in {city}.")
    if category:
        ctx.append(f"Category: {category}.")
    if rating and reviews:
        ctx.append(f"Google rating: {rating} stars with {reviews} reviews.")
    if email:
        ctx.append(f"Contact email: {email}.")

    biz_context = " ".join(ctx)

    return f"""Generate outreach messages for {name}.

BUSINESS CONTEXT:
{biz_context}

Generate SIX messages in JSON format. Each must be professional, concise,
human-sounding, and tailored to this specific business. No buzzwords, no
emojis, no "I hope this finds you well", no generic filler.

Return ONLY valid JSON (no markdown, no code fences) with these keys:
{{
  "email_subject": "Short, specific subject line",
  "email_message": "Full email body (greeting through sign-off, use \\n for line breaks). Under 120 words.",
  "contact_form_message": "Shorter version suitable for a website contact form. Under 80 words.",
  "dm_message": "Very short social media DM. Under 40 words. Casual-professional.",
  "follow_up_message": "A polite follow-up email for 5-7 days later. Under 80 words. Reference the first email without being pushy.",
  "call_script": "Brief phone intro script. Under 60 words. Natural speaking tone."
}}

Rules:
- Reference something specific about their business (location, category, reviews, website status)
- Be direct about what you offer and why it matters to them
- Sound like a real person, not a template
- No "leverage", "synergy", "game-changer", "revolutionary", "cutting-edge"
- Each message type should feel distinct, not just the same text reformatted"""


def _generate_for_business(biz: dict) -> dict | None:
    """
    Call Claude to generate messages for a single business.

    Returns the parsed JSON dict on success, None on failure.
    """
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed (pip install anthropic)")
        return None

    prompt = _build_prompt(biz)

    try:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0]
            text = text.strip()

        return json.loads(text)

    except json.JSONDecodeError as e:
        logger.warning("JSON parse error for %s: %s",
                       biz.get("business_name", "?"), e)
        return None
    except Exception as e:
        logger.warning("Claude API error for %s: %s",
                       biz.get("business_name", "?"), e)
        return None


def generate_messages(
    businesses: list[dict],
    score_threshold: int = 0,
    max_messages: int = 0,
) -> list[dict]:
    """
    Generate AI outreach messages for qualifying businesses.

    Args:
        businesses: List of business dicts from the pipeline.
        score_threshold: Minimum lead_score to generate messages for.
        max_messages: Max number of businesses to generate for (0 = no limit).

    Returns:
        The same businesses list with message fields populated.
    """
    threshold = score_threshold or config.MESSAGE_SCORE_THRESHOLD

    # Identify which businesses qualify
    qualifying = []
    for i, biz in enumerate(businesses):
        score = biz.get("lead_score", 0)
        if score >= threshold:
            qualifying.append((i, biz))

    # Apply limit
    if max_messages and len(qualifying) > max_messages:
        qualifying = qualifying[:max_messages]

    qualified_indices = {idx for idx, _ in qualifying}
    logger.info(
        "%d of %d businesses qualify for messages (score >= %d, limit %s)",
        len(qualifying), len(businesses), threshold,
        max_messages or "none",
    )

    # Set defaults on ALL businesses first
    for biz in businesses:
        for field in _MESSAGE_FIELDS:
            biz.setdefault(field, "")
        if biz.get("lead_score", 0) < threshold:
            biz["message_error"] = "below_threshold"
        else:
            biz.setdefault("message_error", "")

    # Generate messages for qualifying businesses
    success = 0
    errors = 0
    for count, (idx, biz) in enumerate(qualifying, 1):
        name = biz.get("business_name", "?")
        logger.info("  [%d/%d] Generating messages for %s ...",
                    count, len(qualifying), name)

        result = _generate_for_business(biz)

        if result:
            for field in _MESSAGE_FIELDS:
                biz[field] = result.get(field, "")
            biz["message_error"] = ""
            success += 1
        else:
            for field in _MESSAGE_FIELDS:
                biz[field] = ""
            biz["message_error"] = "generation_failed"
            errors += 1

    logger.info("Message generation complete: %d success, %d errors", success, errors)
    return businesses
