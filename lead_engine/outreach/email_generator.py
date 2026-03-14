"""
email_generator.py — AI-powered email draft generation using Claude.

Generates personalized, human-sounding outreach emails for each lead.
The tone is professional, concise, and respectful — written as if a real
person looked at the business's website and is reaching out with genuine value.

No buzzwords. No cheesy sales language. No "I hope this finds you well."
"""

import logging
import json

from . import outreach_config as cfg

logger = logging.getLogger("outreach")


def _build_prompt(lead: dict) -> str:
    """
    Build the Claude prompt for generating an outreach email.

    Uses real business data so the email feels personal and relevant.
    """
    # Gather context about the lead
    biz_name = lead.get("business_name", "the business")
    website = lead.get("website", "")
    city = lead.get("city", "")
    category = lead.get("category", "")
    rating = lead.get("rating", 0)
    reviews = lead.get("review_count", 0)
    score = lead.get("lead_score", 0)

    # Context about what we know
    context_parts = []
    if website:
        context_parts.append(f"Their website is {website}.")
    else:
        context_parts.append("They do not appear to have a website, or it was not found online.")
    if city:
        context_parts.append(f"Located in {city}.")
    if category:
        context_parts.append(f"Business category: {category}.")
    if rating and reviews:
        context_parts.append(f"They have a {rating}-star rating with {reviews} reviews on Google.")
    elif reviews:
        context_parts.append(f"They have {reviews} reviews on Google.")

    biz_context = " ".join(context_parts)

    # Info about the sender
    sender_parts = []
    if cfg.YOUR_NAME:
        sender_parts.append(f"Your name is {cfg.YOUR_NAME}.")
    if cfg.YOUR_BUSINESS:
        sender_parts.append(f"Your business is {cfg.YOUR_BUSINESS}.")
    if cfg.YOUR_SERVICE:
        sender_parts.append(f"You offer: {cfg.YOUR_SERVICE}.")
    if cfg.YOUR_WEBSITE:
        sender_parts.append(f"Your website is {cfg.YOUR_WEBSITE}.")

    sender_context = " ".join(sender_parts) if sender_parts else (
        "You are a professional reaching out to offer your services."
    )

    prompt = f"""Write a cold outreach email to {biz_name}.

ABOUT THE BUSINESS:
{biz_context}

ABOUT YOU (THE SENDER):
{sender_context}

RULES — follow these exactly:
1. Subject line: Short, specific to their business. No clickbait. No ALL CAPS. No "Quick question" cliches.
2. Opening: Reference something specific about their business (location, reviews, what they do). Do NOT say "I hope this finds you well" or "I came across your business."
3. Body: Explain what you do and why it's relevant to them specifically. Be direct. One short paragraph max.
4. Tone: Professional but warm. Like a real person writing a real email. Not corporate, not salesy.
5. Length: Under 120 words total for the body. Busy professionals won't read more.
6. Closing: Simple call to action. "Would you be open to a quick chat?" or similar. No pressure.
7. Sign-off: Just your name. No "Best regards" or "Warmly" — keep it casual-professional.
8. Do NOT use any of these words/phrases: "leverage", "synergy", "game-changer", "revolutionary", "cutting-edge", "reach out", "touch base", "circle back", "low-hanging fruit", "value proposition", "solutions"
9. Do NOT use emojis.
10. Do NOT include any unsubscribe text — that will be added automatically.

Respond in this exact JSON format (no markdown, no code fences):
{{"subject": "Your subject line here", "body": "Your email body here"}}

The body should include a greeting, the message, and sign-off — all as a single string with \\n for line breaks."""

    return prompt


def generate_draft(lead: dict) -> tuple[str, str, str]:
    """
    Generate a personalized email draft for a single lead using Claude.

    Returns (subject, body, error).
    - On success: (subject, body, "")
    - On failure: ("", "", error_message)
    """
    if not cfg.ANTHROPIC_API_KEY:
        return "", "", "ANTHROPIC_API_KEY not configured"

    try:
        import anthropic
    except ImportError:
        return "", "", "anthropic package not installed (pip install anthropic)"

    prompt = _build_prompt(lead)

    try:
        client = anthropic.Anthropic(api_key=cfg.ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        # Extract the text response
        text = response.content[0].text.strip()

        # Parse JSON response
        # Handle potential markdown code fences
        if text.startswith("```"):
            text = text.split("\n", 1)[1]  # Remove first line
            text = text.rsplit("```", 1)[0]  # Remove last fence
            text = text.strip()

        data = json.loads(text)
        subject = data.get("subject", "").strip()
        body = data.get("body", "").strip()

        if not subject or not body:
            return "", "", "Claude returned empty subject or body"

        return subject, body, ""

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Claude response as JSON: %s", e)
        return "", "", f"JSON parse error: {e}"
    except Exception as e:
        logger.error("Claude API error for %s: %s",
                     lead.get("business_name", "?"), e)
        return "", "", str(e)


def generate_drafts_batch(leads: list[dict], db) -> tuple[int, int]:
    """
    Generate email drafts for a list of leads and save to the database.

    Args:
        leads: List of lead dicts (from db.get_new_leads()).
        db: OutreachDB instance.

    Returns:
        (success_count, error_count)
    """
    success = 0
    errors = 0

    for i, lead in enumerate(leads):
        biz_name = lead.get("business_name", "?")
        email = lead.get("email", "")
        logger.info("[%d/%d] Generating draft for %s (%s)",
                    i + 1, len(leads), biz_name, email)

        subject, body, error = generate_draft(lead)

        if error:
            logger.warning("  Draft failed: %s", error)
            db.update_status(email, "Failed", last_error=f"draft_error: {error}")
            errors += 1
        else:
            logger.info("  Draft OK — subject: %s", subject[:60])
            db.update_status(
                email, "Reviewed",
                subject_line=subject,
                email_body=body,
            )
            success += 1

    return success, errors
