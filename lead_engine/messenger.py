"""
messenger.py — Generate tailored outreach messages using the Claude API.

Builds a compact context summary per business and asks Claude to write
three message variants: email, contact-form, and DM.
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


def _build_prompt(biz: dict) -> str:
    """
    Build the Claude prompt for one business.

    Summarises findings into a compact payload — no raw HTML.
    """
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

    angle_desc = ANGLE_DESCRIPTIONS.get(angle, ANGLE_DESCRIPTIONS["general_improvement"])

    # Build context block
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
    context_lines.append(f"Lead score: {score}")
    context_lines.append(f"Recommended angle: {angle}")

    context_block = "\n".join(context_lines)

    prompt = f"""You are helping a freelance web developer write outreach messages to local businesses.

BUSINESS CONTEXT:
{context_block}

PITCH ANGLE:
{angle_desc}

Write exactly three outreach messages for this business. Each must:
- Mention the business by name ("{name}")
- Sound like a real person wrote it (not a template)
- Be concise (3-5 sentences max)
- Be professional, friendly, and direct
- NOT sound like spam
- NOT promise fake audits or made-up statistics
- NOT mention things you could not realistically know
- NOT use hype words like "skyrocket" or "explosive growth"
- Reference a specific, relevant issue ONLY if one was actually detected

Format your response EXACTLY like this (keep the labels):

EMAIL:
[Your cold email message here]

CONTACT_FORM:
[Your contact-form message here — slightly shorter and more casual]

DM:
[Your DM message — 2-3 sentences max, very casual and direct]"""

    return prompt


def _parse_response(text: str) -> dict:
    """Parse Claude's response into the three message types."""
    messages = {"email": "", "contact_form": "", "dm": ""}

    sections = {
        "EMAIL:": "email",
        "CONTACT_FORM:": "contact_form",
        "DM:": "dm",
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
                # Grab anything after the label on the same line
                remainder = stripped[len(label):].strip()
                current_lines = [remainder] if remainder else []
                matched = True
                break
        if not matched and current_key is not None:
            current_lines.append(line)

    if current_key:
        messages[current_key] = "\n".join(current_lines).strip()

    return messages


def generate_messages(businesses: list[dict],
                      score_threshold: int | None = None,
                      max_messages: int = 0) -> list[dict]:
    """
    Generate outreach messages for qualifying businesses using Claude API.

    Modifies each business dict in-place, adding:
      email_message, contact_form_message, dm_message, message_error

    Returns the same list.
    """
    if not config.ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set — skipping message generation")
        for biz in businesses:
            biz["email_message"] = ""
            biz["contact_form_message"] = ""
            biz["dm_message"] = ""
            biz["message_error"] = "api_key_missing"
        return businesses

    threshold = score_threshold if score_threshold is not None else config.MESSAGE_SCORE_THRESHOLD
    limit = max_messages if max_messages else config.MAX_MESSAGES_PER_RUN

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    generated = 0
    for i, biz in enumerate(businesses):
        # Default empty
        biz["email_message"] = ""
        biz["contact_form_message"] = ""
        biz["dm_message"] = ""
        biz["message_error"] = ""

        # Skip low-score leads
        if biz.get("lead_score", 0) < threshold:
            biz["message_error"] = "below_threshold"
            continue

        # Skip chains
        if biz.get("pitch_angle") == "skip_chain":
            biz["message_error"] = "chain_skipped"
            continue

        # Respect limit
        if limit and generated >= limit:
            biz["message_error"] = "limit_reached"
            continue

        prompt = _build_prompt(biz)
        name = biz.get("business_name", f"#{i}")

        try:
            logger.info("Generating messages for: %s (score=%d)",
                        name, biz.get("lead_score", 0))
            response = client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            parsed = _parse_response(text)
            biz["email_message"] = parsed["email"]
            biz["contact_form_message"] = parsed["contact_form"]
            biz["dm_message"] = parsed["dm"]
            generated += 1

        except anthropic.RateLimitError:
            logger.warning("Rate limited — pausing 30s before retrying %s", name)
            time.sleep(30)
            try:
                response = client.messages.create(
                    model=config.CLAUDE_MODEL,
                    max_tokens=800,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                parsed = _parse_response(text)
                biz["email_message"] = parsed["email"]
                biz["contact_form_message"] = parsed["contact_form"]
                biz["dm_message"] = parsed["dm"]
                generated += 1
            except Exception as exc:
                logger.error("Retry also failed for %s: %s", name, exc)
                biz["message_error"] = f"api_error: {exc}"

        except Exception as exc:
            logger.error("Claude API error for %s: %s", name, exc)
            biz["message_error"] = f"api_error: {exc}"

    logger.info("Generated messages for %d / %d businesses", generated, len(businesses))
    return businesses
