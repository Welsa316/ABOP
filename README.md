# Lead Scoring & Outreach Generator

A local Python tool that takes a CSV export of businesses (from Google Maps / Apify), scores them as leads for a freelance web developer, analyses their websites, and auto-generates tailored outreach messages using Claude AI.

## What It Does

1. **Loads** a CSV of local businesses (Google Places / Apify export)
2. **Normalises** column names automatically — works with varying CSV formats
3. **Analyses** each business website (HTTPS, mobile viewport, speed, CTAs, contact info, menu/booking/ordering links, outdated design, placeholder pages)
4. **Scores** every business with a weighted lead-scoring system — businesses with no website score highest
5. **Generates** three tailored outreach messages per lead (email, contact form, DM) using the Claude API
6. **Outputs** enriched CSVs, a ranked CSV, high-priority CSV, full JSON, and a text summary report

## Option A: Run as a .exe (easiest)

No Python knowledge needed after the initial build.

### One-time build

1. Install Python 3.11+ from [python.org](https://www.python.org/downloads/) (check **"Add Python to PATH"**)
2. Open a terminal in this project folder and run:
   ```
   pip install -r requirements.txt
   python build_exe.py
   ```
3. This creates a `dist/LeadEngine/` folder with `LeadEngine.exe` inside

### Using the .exe

1. Copy the entire `dist/LeadEngine/` folder wherever you want (Desktop, etc.)
2. Drop your CSV file into that folder
3. Double-click **LeadEngine.exe**
4. On first run, it will ask for your Anthropic API key — paste it and press Enter
5. The key is saved automatically in a `.env` file next to the .exe, so you only do this once
6. Pick your CSV file from the menu, and it runs

Output files appear in an `output/` folder next to the .exe.

---

## Option B: Run as a Python script

### 1. Install Python

Download Python 3.11+ from [python.org](https://www.python.org/downloads/). During install, check **"Add Python to PATH"**.

### 2. Clone / download this project

```
git clone https://github.com/Welsa316/Automatic-Business-Outreach-Program.git
cd Automatic-Business-Outreach-Program
```

### 3. Install dependencies

```
pip install -r requirements.txt
```

### 4. Run the tool

```
python run.py
```

On first run, it will ask for your Anthropic API key and save it automatically. Or you can set it manually:

```
copy .env.example .env
```
Then edit `.env` and paste your key.

You can also pass a CSV file directly:
```
python run.py --csv your-businesses.csv
```

## CLI Options

| Flag | Description | Default |
|---|---|---|
| `--csv PATH` | Path to input CSV file | (interactive prompt) |
| `--output DIR` | Output directory | `output/` |
| `--limit N` | Process only first N rows | all |
| `--no-analyze` | Skip website analysis | off |
| `--no-ai` | Skip Claude message generation | off |
| `--ai-limit N` | Max businesses to generate messages for | unlimited |
| `--score-threshold N` | Min lead score to generate messages | 20 |
| `--timeout N` | HTTP timeout per request (seconds) | 12 |
| `--concurrency N` | Max simultaneous website checks | 10 |
| `-v / --verbose` | Debug logging | off |

### Example commands

```bash
# Full run on all businesses
python run.py --csv businesses.csv

# Quick test on first 20 rows, no AI
python run.py --csv businesses.csv --limit 20 --no-ai

# Generate messages only for top leads (score >= 35)
python run.py --csv businesses.csv --score-threshold 35

# Cap AI costs: only generate messages for top 25 leads
python run.py --csv businesses.csv --ai-limit 25

# Skip website fetching entirely (score from metadata only)
python run.py --csv businesses.csv --no-analyze
```

## Output Files

All outputs go to the `output/` directory:

| File | Description |
|---|---|
| `leads_enriched.csv` | All businesses with scores, issues, and messages |
| `leads_ranked.csv` | Same data, sorted by lead score (highest first) |
| `leads_high_priority.csv` | Only leads with score >= 30 |
| `leads_full.json` | Full structured JSON with all data |
| `summary_report.txt` | Text summary: stats, top issues, top 20 leads |

## Scoring System

Businesses are scored on a points system. Higher score = better lead.

| Factor | Points |
|---|---|
| No website at all | +40 |
| Website unreachable | +35 |
| Social media only (no real site) | +30 |
| Placeholder / parked page | +15 |
| No mobile viewport | +12 |
| No call-to-action | +10 |
| Thin content | +10 |
| Outdated HTML design | +10 |
| No HTTPS | +10 |
| No contact info visible | +8 |
| Slow response (>5s) | +8 |
| No online ordering (restaurants) | +6 |
| No booking option (services) | +6 |
| No menu link (restaurants) | +5 |
| 500+ reviews | +12 |
| 100+ reviews | +8 |
| 4.8+ star rating | +8 |
| 4.5+ star rating | +5 |
| Suspected chain/franchise | -20 |
| Strong modern website | -15 |

All weights are configurable in `lead_engine/config.py`.

## Project Structure

```
Automatic-Business-Outreach-Program/
├── run.py                  # Main entry point (CLI)
├── build_exe.py            # One-click .exe builder
├── requirements.txt        # Python dependencies
├── .env.example            # API key template
├── .gitignore
├── README.md
├── lead_engine/
│   ├── __init__.py
│   ├── config.py           # Weights, thresholds, constants
│   ├── utils.py            # Helper functions
│   ├── loader.py           # CSV loading & column normalisation
│   ├── analyzer.py         # Website analysis (requests + BeautifulSoup)
│   ├── scorer.py           # Lead scoring engine
│   ├── messenger.py        # Claude API message generation
│   └── writer.py           # Output file generation
└── output/                 # Generated output files
```

## Customisation

### Adjust scoring weights
Edit `lead_engine/config.py` → `SCORE_WEIGHTS` dictionary.

### Add chain names to filter
Edit `lead_engine/config.py` → `CHAIN_KEYWORDS` list.

### Change the Claude model
Edit `lead_engine/config.py` → `CLAUDE_MODEL` (default: `claude-sonnet-4-20250514`).

### Change message tone
Edit the prompt template in `lead_engine/messenger.py` → `_build_prompt()`.

## Limitations

- Website analysis is heuristic-based (requests + BeautifulSoup), not a full browser render. JavaScript-heavy sites may not be fully analysed.
- Claude API costs apply per message generated. Use `--ai-limit` or `--score-threshold` to control costs.
- The tool does not send any messages — it only generates them for your review.
- Rate limiting is handled with a single retry + 30s pause. Very large batches may need manual re-runs.

## Cost Estimate

With `claude-sonnet-4-20250514` at ~$3/1M input tokens and ~$15/1M output tokens:
- Each business uses roughly 500 input + 300 output tokens
- 100 businesses ≈ $0.60
- 300 businesses ≈ $1.80

Use `--ai-limit` to cap costs during testing.
