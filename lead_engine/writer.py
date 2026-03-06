"""
writer.py — Output generation: enriched CSV, ranked CSV, JSON, text report,
             and optional high-priority CSV.
"""

import csv
import logging
from pathlib import Path
from collections import Counter

from .utils import save_json, save_text

logger = logging.getLogger("lead_engine")

# Columns for the output CSVs
OUTPUT_COLUMNS = [
    "business_name",
    "primary_category",
    "city",
    "state",
    "phone",
    "website",
    "final_url",
    "rating",
    "review_count",
    "lead_score",
    "score_breakdown_text",
    "website_status",
    "detected_issues_text",
    "pitch_angle",
    "email_message",
    "contact_form_message",
    "dm_message",
    "message_error",
    "google_url",
]


def _breakdown_text(breakdown: list[dict]) -> str:
    """Flatten score breakdown into a readable string."""
    if not breakdown:
        return ""
    parts = [f"{item['reason']} (+{item['points']})" if item['points'] > 0
             else f"{item['reason']} ({item['points']})"
             for item in breakdown]
    return "; ".join(parts)


def _issues_text(issues: list[str]) -> str:
    return ", ".join(issues) if issues else ""


def _biz_to_row(biz: dict) -> dict:
    """Convert a business dict into a flat row for CSV output."""
    return {
        "business_name":        biz.get("business_name", ""),
        "primary_category":     biz.get("primary_category", ""),
        "city":                 biz.get("city", ""),
        "state":                biz.get("state", ""),
        "phone":                biz.get("phone", ""),
        "website":              biz.get("website", ""),
        "final_url":            biz.get("final_url", ""),
        "rating":               biz.get("rating", ""),
        "review_count":         biz.get("review_count", ""),
        "lead_score":           biz.get("lead_score", 0),
        "score_breakdown_text": _breakdown_text(biz.get("score_breakdown", [])),
        "website_status":       biz.get("website_status", ""),
        "detected_issues_text": _issues_text(biz.get("detected_issues", [])),
        "pitch_angle":          biz.get("pitch_angle", ""),
        "email_message":        biz.get("email_message", ""),
        "contact_form_message": biz.get("contact_form_message", ""),
        "dm_message":           biz.get("dm_message", ""),
        "message_error":        biz.get("message_error", ""),
        "google_url":           biz.get("google_url", ""),
    }


def _write_csv(rows: list[dict], path: Path) -> None:
    """Write a list of row dicts to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Saved CSV → %s (%d rows)", path, len(rows))


def _build_report(businesses: list[dict]) -> str:
    """Build a plain-text summary report."""
    total = len(businesses)
    no_website = sum(1 for b in businesses if not b.get("website"))
    unreachable = sum(1 for b in businesses
                      if b.get("website_status") == "unreachable")
    social_only = sum(1 for b in businesses
                      if b.get("website_status") == "social_only")

    # Count issue frequencies
    issue_counter: Counter = Counter()
    for b in businesses:
        for issue in b.get("detected_issues", []):
            issue_counter[issue] += 1

    # Top 20 leads
    top20 = businesses[:20]

    lines = [
        "=" * 60,
        "  LEAD SCORING SUMMARY REPORT",
        "=" * 60,
        "",
        f"Total businesses processed:   {total}",
        f"Businesses with NO website:   {no_website}",
        f"Websites unreachable/broken:  {unreachable}",
        f"Social-media-only profiles:   {social_only}",
        "",
        "-" * 60,
        "  TOP REASONS FOR HIGH SCORES",
        "-" * 60,
    ]
    for issue, count in issue_counter.most_common(15):
        lines.append(f"  {issue:30s}  {count:>4d} businesses")

    lines.extend([
        "",
        "-" * 60,
        "  TOP 20 LEADS",
        "-" * 60,
    ])
    for i, b in enumerate(top20, 1):
        name = b.get("business_name", "?")
        score = b.get("lead_score", 0)
        angle = b.get("pitch_angle", "")
        city = b.get("city", "")
        lines.append(f"  {i:>2}. [{score:>3} pts]  {name}  ({city})  → {angle}")

    lines.extend(["", "=" * 60, "  END OF REPORT", "=" * 60, ""])
    return "\n".join(lines)


def write_outputs(businesses: list[dict], output_dir: str | Path) -> dict:
    """
    Write all output files.

    Returns a dict of {file_type: Path} for the files created.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    rows = [_biz_to_row(b) for b in businesses]

    files: dict[str, Path] = {}

    # 1. Enriched CSV (same order as input, but sorted by score now)
    enriched_path = out / "leads_enriched.csv"
    _write_csv(rows, enriched_path)
    files["enriched_csv"] = enriched_path

    # 2. Ranked CSV (same data, explicitly sorted — already sorted)
    ranked_path = out / "leads_ranked.csv"
    _write_csv(rows, ranked_path)
    files["ranked_csv"] = ranked_path

    # 3. High-priority CSV (score >= 30)
    high_priority = [r for r in rows if int(r.get("lead_score", 0)) >= 30]
    if high_priority:
        hp_path = out / "leads_high_priority.csv"
        _write_csv(high_priority, hp_path)
        files["high_priority_csv"] = hp_path

    # 4. JSON with full structured data
    json_data = []
    for b in businesses:
        entry = {k: v for k, v in b.items() if k != "_raw"}
        json_data.append(entry)
    json_path = out / "leads_full.json"
    save_json(json_data, json_path)
    files["json"] = json_path

    # 5. Text summary report
    report = _build_report(businesses)
    report_path = out / "summary_report.txt"
    save_text(report, report_path)
    files["report"] = report_path

    # Print report to console too
    print("\n" + report)

    return files
