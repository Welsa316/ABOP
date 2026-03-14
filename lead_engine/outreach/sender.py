"""
sender.py — Email sending via Resend API.

Sends emails one at a time with safety delays between each send.
Every send is logged and tracked in the database.

Resend docs: https://resend.com/docs/api-reference/emails/send-email
"""

import time
import random
import logging

from . import outreach_config as cfg
from .safety import check_lead_safety, check_from_address

logger = logging.getLogger("outreach")


def _add_unsubscribe_footer(body: str, email: str) -> str:
    """
    Append a simple unsubscribe line to the email body.

    For a solo developer system, this is a plain-text opt-out notice.
    In a production system, you'd use a link to an unsubscribe endpoint.
    """
    footer = (
        f"\n\n---\n"
        f"If you'd prefer not to hear from us, simply reply with "
        f"\"unsubscribe\" and we'll remove you from our list immediately."
    )
    return body + footer


def send_single(lead: dict, db, dry_run: bool = False) -> tuple[bool, str]:
    """
    Send a single email to a lead.

    Args:
        lead: Lead dict from the database.
        db: OutreachDB instance.
        dry_run: If True, simulate the send without actually sending.

    Returns:
        (success, message) where message is the provider ID or error.
    """
    email = lead.get("email", "").strip().lower()
    biz_name = lead.get("business_name", "?")
    subject = lead.get("subject_line", "")
    body = lead.get("email_body", "")

    # Run all safety checks
    safety = check_lead_safety(lead, db)
    if not safety.passed:
        logger.warning("Safety blocked %s (%s): %s", biz_name, email, safety)
        return False, str(safety)

    # Check sender configuration
    from_ok, from_reason = check_from_address()
    if not from_ok and not dry_run:
        logger.error("Sender config error: %s", from_reason)
        return False, from_reason

    # Add unsubscribe footer
    body_with_footer = _add_unsubscribe_footer(body, email)

    # Build the "from" field
    from_field = cfg.FROM_EMAIL
    if cfg.FROM_NAME:
        from_field = f"{cfg.FROM_NAME} <{cfg.FROM_EMAIL}>"

    # --- Dry run mode ---
    if dry_run or cfg.DRY_RUN:
        logger.info("[DRY RUN] Would send to %s (%s)", biz_name, email)
        logger.info("  From: %s", from_field)
        logger.info("  Subject: %s", subject)
        logger.info("  Body preview: %s...", body[:100])
        return True, "dry_run"

    # --- Actually send via Resend ---
    try:
        import resend
    except ImportError:
        error = "resend package not installed (pip install resend)"
        logger.error(error)
        db.mark_failed(email, error)
        return False, error

    resend.api_key = cfg.RESEND_API_KEY

    try:
        params = {
            "from": from_field,
            "to": [email],
            "subject": subject,
            "text": body_with_footer,
        }

        result = resend.Emails.send(params)
        message_id = result.get("id", "") if isinstance(result, dict) else str(result)

        logger.info("Sent to %s (%s) — ID: %s", biz_name, email, message_id)
        db.mark_sent(email, message_id)
        return True, message_id

    except Exception as e:
        error_msg = str(e)
        logger.error("Send failed for %s (%s): %s", biz_name, email, error_msg)
        db.mark_failed(email, error_msg)
        return False, error_msg


def send_batch(leads: list[dict], db, dry_run: bool = False) -> tuple[int, int, int]:
    """
    Send emails to a batch of leads with safety delays between sends.

    Respects the daily send cap and pauses between sends.

    Args:
        leads: List of lead dicts ready to send.
        db: OutreachDB instance.
        dry_run: If True, simulate all sends.

    Returns:
        (sent_count, failed_count, skipped_count)
    """
    sent = 0
    failed = 0
    skipped = 0

    for i, lead in enumerate(leads):
        email = lead.get("email", "")
        biz_name = lead.get("business_name", "?")

        # Check daily cap before each send
        if not dry_run and db.count_sent_today() >= cfg.DAILY_SEND_CAP:
            remaining = len(leads) - i
            logger.warning(
                "Daily send cap reached (%d). Stopping. %d leads remaining.",
                cfg.DAILY_SEND_CAP, remaining,
            )
            skipped += remaining
            break

        # Check if campaign was paused mid-batch
        if cfg.CAMPAIGN_PAUSED:
            remaining = len(leads) - i
            logger.warning("Campaign paused. Stopping. %d leads remaining.", remaining)
            skipped += remaining
            break

        logger.info("[%d/%d] Processing %s (%s)", i + 1, len(leads), biz_name, email)

        success, message = send_single(lead, db, dry_run=dry_run)

        if success:
            sent += 1
        elif "BLOCKED" in message:
            skipped += 1
        else:
            failed += 1

        # Delay between sends (skip delay on last item or dry run)
        if i < len(leads) - 1 and not dry_run:
            delay = random.uniform(cfg.MIN_DELAY_SECONDS, cfg.MAX_DELAY_SECONDS)
            logger.info("  Waiting %.0f seconds before next send...", delay)
            time.sleep(delay)

    return sent, failed, skipped
