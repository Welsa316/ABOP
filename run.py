"""
run.py — Main entry point for the Lead Scoring & Outreach Tool.

Usage:
    python run.py                              (interactive prompts)
    python run.py --csv data.csv               (minimal CLI)
    python run.py --csv data.csv --no-ai       (skip Claude messages)
    python run.py --csv data.csv --limit 50    (first 50 rows only)
    python run.py --help                       (show all options)
"""

import argparse
import asyncio
import sys
import time
import json
from pathlib import Path

# Load .env file if present (must happen before config import reads env vars)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional but recommended

from lead_engine import config
# Re-read API key after dotenv has loaded
config.ANTHROPIC_API_KEY = __import__("os").getenv("ANTHROPIC_API_KEY", "")
from lead_engine.utils import setup_logging, save_json
from lead_engine.loader import load_csv
from lead_engine.analyzer import analyze_websites
from lead_engine.scorer import score_all
from lead_engine.messenger import generate_messages
from lead_engine.writer import write_outputs

import logging
logger = logging.getLogger("lead_engine")

PROGRESS_FILE = "output/.progress.json"


def _save_progress(businesses: list[dict], stage: str) -> None:
    """Save intermediate progress so work is not lost on crash."""
    path = Path(PROGRESS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Strip non-serialisable items
    safe = []
    for b in businesses:
        entry = {k: v for k, v in b.items() if k != "_raw"}
        safe.append(entry)
    save_json({"stage": stage, "count": len(safe), "data": safe}, path)
    logger.debug("Progress saved at stage=%s (%d businesses)", stage, len(safe))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Lead Scoring & Outreach Generator for Local Businesses",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py --csv businesses.csv
  python run.py --csv businesses.csv --output results --limit 100
  python run.py --csv businesses.csv --no-ai --no-analyze
  python run.py --csv businesses.csv --ai-limit 25 --score-threshold 30
        """,
    )
    p.add_argument("--csv", type=str, default="",
                   help="Path to input CSV file")
    p.add_argument("--output", type=str, default="output",
                   help="Output directory (default: output)")
    p.add_argument("--limit", type=int, default=0,
                   help="Only process first N rows (0 = all)")
    p.add_argument("--no-analyze", action="store_true",
                   help="Skip website analysis (score based on metadata only)")
    p.add_argument("--no-ai", action="store_true",
                   help="Skip Claude message generation entirely")
    p.add_argument("--ai-limit", type=int, default=0,
                   help="Max businesses to generate messages for (0 = unlimited)")
    p.add_argument("--score-threshold", type=int, default=None,
                   help=f"Min score for message generation (default: {config.MESSAGE_SCORE_THRESHOLD})")
    p.add_argument("--timeout", type=int, default=None,
                   help=f"HTTP timeout in seconds (default: {config.REQUEST_TIMEOUT})")
    p.add_argument("--concurrency", type=int, default=None,
                   help=f"Max concurrent website checks (default: {config.MAX_CONCURRENT_REQUESTS})")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable debug logging")
    return p.parse_args()


def interactive_csv_prompt() -> str:
    """If no --csv flag, prompt the user for a file path."""
    print("\n=== Lead Scoring & Outreach Generator ===\n")

    # Auto-detect CSV files in current directory
    csvs = sorted(Path(".").glob("*.csv"))
    if csvs:
        print("CSV files found in current directory:")
        for i, f in enumerate(csvs, 1):
            print(f"  {i}. {f.name}")
        print()
        choice = input(f"Enter number (1-{len(csvs)}) or full path: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(csvs):
            return str(csvs[int(choice) - 1])
        return choice
    else:
        return input("Enter path to CSV file: ").strip()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    # Apply config overrides
    if args.timeout:
        config.REQUEST_TIMEOUT = args.timeout
    if args.concurrency:
        config.MAX_CONCURRENT_REQUESTS = args.concurrency

    # Get CSV path
    csv_path = args.csv
    if not csv_path:
        csv_path = interactive_csv_prompt()
    if not csv_path:
        print("No CSV file specified. Exiting.")
        sys.exit(1)
    if not Path(csv_path).exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)

    t_start = time.time()

    # ------------------------------------------------------------------
    # Stage 1: Load & normalise
    # ------------------------------------------------------------------
    print("\n[1/4] Loading and normalising CSV ...")
    businesses = load_csv(csv_path)
    if args.limit:
        businesses = businesses[:args.limit]
        logger.info("Limited to first %d rows", args.limit)
    print(f"      Loaded {len(businesses)} businesses.")
    _save_progress(businesses, "loaded")

    # ------------------------------------------------------------------
    # Stage 2: Website analysis
    # ------------------------------------------------------------------
    if args.no_analyze:
        print("\n[2/4] Skipping website analysis (--no-analyze)")
        analyses = {}
    else:
        print(f"\n[2/4] Analysing websites (timeout={config.REQUEST_TIMEOUT}s, "
              f"concurrency={config.MAX_CONCURRENT_REQUESTS}) ...")
        analyses = asyncio.run(
            analyze_websites(businesses, max_concurrent=args.concurrency)
        )
        sites_ok = sum(1 for a in analyses.values() if a.reachable)
        print(f"      {sites_ok} reachable / {len(analyses)} checked.")
    _save_progress(businesses, "analyzed")

    # ------------------------------------------------------------------
    # Stage 3: Scoring
    # ------------------------------------------------------------------
    print("\n[3/4] Scoring leads ...")
    businesses = score_all(businesses, analyses)
    top = businesses[0] if businesses else {}
    print(f"      Top lead: {top.get('business_name', '?')} "
          f"(score={top.get('lead_score', 0)})")
    _save_progress(businesses, "scored")

    # ------------------------------------------------------------------
    # Stage 4: Message generation
    # ------------------------------------------------------------------
    if args.no_ai:
        print("\n[4/4] Skipping AI message generation (--no-ai)")
        for biz in businesses:
            biz["email_message"] = ""
            biz["contact_form_message"] = ""
            biz["dm_message"] = ""
            biz["message_error"] = "skipped"
    else:
        if not config.ANTHROPIC_API_KEY:
            print("\n[4/4] WARNING: ANTHROPIC_API_KEY not set. Skipping messages.")
            print("      Set it with:  set ANTHROPIC_API_KEY=sk-ant-...")
            for biz in businesses:
                biz["email_message"] = ""
                biz["contact_form_message"] = ""
                biz["dm_message"] = ""
                biz["message_error"] = "api_key_missing"
        else:
            print("\n[4/4] Generating outreach messages with Claude ...")
            businesses = generate_messages(
                businesses,
                score_threshold=args.score_threshold,
                max_messages=args.ai_limit,
            )
    _save_progress(businesses, "messaged")

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    print("\nWriting output files ...")
    files = write_outputs(businesses, args.output)
    elapsed = time.time() - t_start

    print(f"\nDone in {elapsed:.1f}s. Output files:")
    for label, path in files.items():
        print(f"  {label:20s} → {path}")
    print()


if __name__ == "__main__":
    main()
