"""
messenger.py — Generate tailored outreach messages using the Claude API.

Builds a compact context summary per business and asks Claude to write
multiple message variants: email, contact-form, Instagram DM, follow-up,
and call script.
"""

import logging
import time
import anthropic

from . import config

logger = logging.getLogger("lead_engine")

# ---------------------------------------------------------------------------
# Pitch-angle descriptions (used inside the prompt)
# ---------------------------------------------------------------------------
ANGLE_DESCRIPTIONS = {
    "needs_new_website": (
        "This business has NO website at all. Emphasise that a professional, "
        "modern, mobile-friendly website will help customers find them online, "
        "build trust, and grow their business."
    ),
    "site_broken": (
        "This business has a website URL but the site is unreachable or broken. "
        "Emphasise that their current web presence is not working and you can "
        "build them a reliable, professional site."
    ),
    "social_media_only": (
        "This business relies only on social media (Instagram/Facebook) with no "
        "central website. Emphasise that a website centralises their info, shows "
        "up in Google searches, and converts interest into customers."
    ),
    "redesign": (
        "This business has an outdated-looking website with old HTML patterns. "
        "Emphasise a modern redesign that looks fresh, loads fast, and converts "
        "visitors into customers."
    ),
    "mobile_improvement": (
        "This business's website is not mobile-friendly. Emphasise that most "
        "local customers search on their phones and a mobile-optimised site "
        "will capture more visitors."
    ),
    "speed_improvement": (
        "This business's website is slow to load. Emphasise that a fast, "
        "optimised website keeps visitors from leaving and improves their "
        "Google ranking."
    ),
    "cta_improvement": (
        "This business's website lacks clear calls-to-action. Emphasise that "
        "adding prominent buttons for ordering, booking, or contacting will "
        "turn more visitors into paying customers."
    ),
    "contact_improvement": (
        "This business's website makes it hard to find contact information. "
        "Emphasise making it easy for customers to call, email, or visit."
    ),
    "content_improvement": (
        "This business's website has very little content. Emphasise that "
        "a well-structured site with good content builds trust and ranks "
        "better in local search."
    ),
    "security_upgrade": (
        "This business's website does not use HTTPS. Emphasise that a secure "
        "site builds trust and is now expected by both customers and Google."
    ),
    "add_menu": (
        "This restaurant's website has no visible menu. Emphasise that "
        "customers want to see the menu online before they visit or order."
    ),
    "add_ordering": (
        "This restaurant's website has no online ordering option. Emphasise "
        "that online ordering drives more revenue and convenience."
    ),
    "add_booking": (
        "This service business's website has no online booking option. "
        "Emphasise that letting customers book online increases appointments."
    ),
    "general_improvement": (
        "This business has a website but there are opportunities to improve it. "
        "Focus on general professionalism, user experience, and helping convert "
        "more visitors into customers."
    ),
    "low_priority": (
        "This business has a strong website. Keep the message very light — "
        "just introduce yourself and mention you specialise in local business "
        "websites if they ever need help."
    ),
}


def _build_context_block(biz: dict) -> str:
    """Build a compact context summary for the prompt."""
    name = biz.get("business_name", "the business")
    city = biz.get("city", "")
    state = biz.get("state", "")
    location = f"{city}, {state}".strip(", ") if city else ""
    category = biz.get("primary_category", "local business")
    rating = biz.get("rating", 0)
    reviews = biz.get("review_count", 0)
    website = biz.get("website", "")
    website_status = biz.get("website_status", "none")
    issues = biz.get("detected_issues", [])
    angle = biz.get("pitch_angle", "general_improvement")
    score = biz.get("lead_score", 0)

    context_lines = [
        f"Business name: {name}",
        f"Category: {category}",
    ]
    if location:
        context_lines.append(f"Location: {location}")
    if rating:
        context_lines.append(f"Google rating: {rating} stars")
    if reviews:
        context_lines.append(f"Google reviews: {reviews}")
    if website:
        context_lines.append(f"Website: {website}")
    context_lines.append(f"Website status: {website_status}")
    if issues:
        context_lines.append(f"Detected issues: {', '.join(issues)}")

    # Discovered contact methods
    for field in ("instagram", "facebook", "tiktok", "email"):
        val = biz.get(field, "")
        if val:
            context_lines.append(f"{field.title()}: {val}")

    contact_count = biz.get("contact_methods_found", 0)
    if contact_count:
        context_lines.append(f"Contact methods found: {contact_count}")

    context_lines.append(f"Lead score: {score}")
    context_lines.append(f"Recommended angle: {angle}")

    # Score summary if available
    summary = biz.get("score_summary", "")
    if summary:
        context_lines.append(f"Score analysis: {summary}")

    return "\n".join(context_lines)


def _build_prompt(biz: dict) -> str:
    """Build the Claude prompt for generating all message types."""
    name = biz.get("business_name", "the business")
    angle = biz.get("pitch_angle", "general_improvement")
    angle_desc = ANGLE_DESCRIPTIONS.get(angle, ANGLE_DESCRIPTIONS["general_improvement"])
    context_block = _build_context_block(biz)

    # Determine which channels to generate
    channels = config.MESSAGE_CHANNELS

    format_sections = []
    if channels.get("email"):
        format_sections.append("""SUBJECT:
[Email subject line — short, specific, not spammy]

EMAIL:
[Your cold email message — 3-5 sentences. Professional, personal, direct.]""")

    if channels.get("contact_form"):
        format_sections.append("""CONTACT_FORM:
[Contact-form message — slightly shorter and more casual than email]""")

    if channels.get("instagram_dm"):
        format_sections.append("""DM:
[Instagram/social DM — 2-3 sentences max, very casual and direct, no subject line]""")

    if channels.get("follow_up"):
        format_sections.append("""FOLLOW_UP:
[Follow-up message if no reply after 5-7 days — friendly check-in, 2-3 sentences, reference the original message briefly, no pressure]""")

    if channels.get("call_script"):
        format_sections.append("""CALL_SCRIPT:
[Phone call opener — what to say in the first 15 seconds. Introduce yourself, mention why you're calling, ask if they have a moment. 3-4 sentences max.]""")

    format_block = "\n\n".join(format_sections)

    prompt = f"""You are helping a freelance web developer write outreach messages to a local business.

BUSINESS CONTEXT:
{context_block}

PITCH ANGLE:
{angle_desc}

YOUR GOAL:
Write personalized outreach messages for this business. Each must:
- Mention the business by name ("{name}")
- Sound like a real person wrote it — natural, conversational, human
- Be concise (respect the length limits for each format)
- Be professional, friendly, and direct
- Mention a specific benefit relevant to their business type
- Reference something specific you noticed about their business (reviews, category, social presence, etc.)
- NOT sound like spam or a mass template
- NOT promise fake audits, made-up statistics, or guaranteed results
- NOT use hype words like "skyrocket", "explosive growth", "dominate"
- NOT be pushy or overly salesy — just a helpful introduction
- NOT mention that you "ran an audit" or "analyzed their business" — just offer help naturally

Format your response EXACTLY like this (keep the labels):

{format_block}"""

    return prompt


def _parse_response(text: str) -> dict:
    """Parse Claude's response into message types."""
    messages = {
        "subject": "",
        "email": "",
        "contact_form": "",
        "dm": "",
        "follow_up": "",
        "call_script": "",
    }

    sections = {
        "SUBJECT:": "subject",
        "EMAIL:": "email",
        "CONTACT_FORM:": "contact_form",
        "DM:": "dm",
        "FOLLOW_UP:": "follow_up",
        "CALL_SCRIPT:": "call_script",
    }

    lines = text.strip().split("\n")
    current_key = None
    current_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        matched = False
        for label, key in sections.items():
            if stripped.upper().startswith(label.upper().rstrip(":")):
                if current_key:
                    messages[current_key] = "\n".join(current_lines).strip()
                current_key = key
                remainder = stripped[len(label):].strip()
                current_lines = [remainder] if remainder else []
                matched = True
                break
        if not matched and current_key is not None:
            current_lines.append(line)

    if current_key:
        messages[current_key] = "\n".join(current_lines).strip()

    return messages


def _call_claude(client, prompt: str) -> str:
    """Make a single Claude API call with retry on rate limit."""
    try:
        response = client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except anthropic.RateLimitError:
        logger.warning("Rate limited — pausing 30s before retry")
        time.sleep(30)
        response = client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text


def generate_messages(businesses: list[dict],
                      score_threshold: int | None = None,
                      max_messages: int = 0,
                      progress_callback=None) -> list[dict]:
    """
    Generate outreach messages for qualifying businesses using Claude API.

    Modifies each business dict in-place, adding:
      email_subject, email_message, contact_form_message, dm_message,
      follow_up_message, call_script, message_error

    Returns the same list.
    """
    # Initialize all message fields
    for biz in businesses:
        for field in ("email_subject", "email_message", "contact_form_message",
                      "dm_message", "follow_up_message", "call_script",
                      "message_error"):
            if field not in biz:
                biz[field] = ""

    if not config.ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set — skipping message generation")
        for biz in businesses:
            biz["message_error"] = "api_key_missing"
        return businesses

    threshold = score_threshold if score_threshold is not None else config.MESSAGE_SCORE_THRESHOLD
    limit = max_messages if max_messages else config.MAX_MESSAGES_PER_RUN

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    generated = 0
    total = len(businesses)
    for i, biz in enumerate(businesses):
        name = biz.get("business_name", f"#{i}")

        # Skip low-score leads
        if biz.get("lead_score", 0) < threshold:
            biz["message_error"] = "below_threshold"
            if progress_callback:
                progress_callback(i, total, name, "skipped")
            continue

        # Skip chains
        if biz.get("pitch_angle") == "skip_chain":
            biz["message_error"] = "chain_skipped"
            if progress_callback:
                progress_callback(i, total, name, "skipped")
            continue

        # Respect limit
        if limit and generated >= limit:
            biz["message_error"] = "limit_reached"
            if progress_callback:
                progress_callback(i, total, name, "skipped")
            continue

        prompt = _build_prompt(biz)

        try:
            logger.info("Generating messages for: %s (score=%d)",
                        name, biz.get("lead_score", 0))
            text = _call_claude(client, prompt)
            parsed = _parse_response(text)

            biz["email_subject"] = parsed["subject"]
            biz["email_message"] = parsed["email"]
            biz["contact_form_message"] = parsed["contact_form"]
            biz["dm_message"] = parsed["dm"]
            biz["follow_up_message"] = parsed["follow_up"]
            biz["call_script"] = parsed["call_script"]
            generated += 1

            if progress_callback:
                progress_callback(i, total, name, "generated")

        except Exception as exc:
            logger.error("Claude API error for %s: %s", name, exc)
            biz["message_error"] = f"api_error: {exc}"
            if progress_callback:
                progress_callback(i, total, name, "error")

    logger.info("Generated messages for %d / %d businesses", generated, len(businesses))
    return businesses
