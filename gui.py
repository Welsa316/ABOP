"""
gui.py — Desktop GUI for the Lead Scoring & Outreach Tool.

Double-click this file (or run `python gui.py`) to launch the app
in a proper window instead of the terminal.

Uses tkinter (built into Python — no extra install needed).
"""

import asyncio
import os
import sys
import threading
import time
import webbrowser
from pathlib import Path
from tkinter import (
    Tk, ttk, StringVar, IntVar, BooleanVar,
    filedialog, messagebox, scrolledtext,
    N, S, E, W, EW, END, DISABLED, NORMAL, LEFT, BOTH, X, Y, TOP, BOTTOM, RIGHT,
)

# ---------------------------------------------------------------------------
# Resolve base directory — works both as .py script and as frozen .exe
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

# Load .env
_env_path = BASE_DIR / ".env"
try:
    from dotenv import load_dotenv
    load_dotenv(_env_path)
except ImportError:
    pass

from lead_engine import config
config.ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
from lead_engine.loader import load_csv
from lead_engine.analyzer import analyze_websites
from lead_engine.contact_discovery import discover_all_contacts
from lead_engine.scorer import score_all
from lead_engine.messenger import generate_messages
from lead_engine.writer import write_outputs

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
BG          = "#1e1e2e"     # dark background
BG_CARD     = "#2a2a3d"     # card / input background
FG          = "#e0e0e0"     # main text
FG_DIM      = "#8888aa"     # secondary text
ACCENT      = "#7c6ff7"     # purple accent (buttons)
ACCENT_HOV  = "#9580ff"     # button hover
SUCCESS     = "#50fa7b"     # green
WARNING     = "#ffb86c"     # orange
ERROR       = "#ff5555"     # red
BORDER      = "#3a3a5c"     # subtle borders


def _save_api_key(key: str) -> None:
    """Save API key to .env file."""
    config.ANTHROPIC_API_KEY = key
    os.environ["ANTHROPIC_API_KEY"] = key
    env_lines = []
    if _env_path.exists():
        env_lines = _env_path.read_text(encoding="utf-8").splitlines()
    found = False
    for i, line in enumerate(env_lines):
        if line.strip().startswith("ANTHROPIC_API_KEY"):
            env_lines[i] = f"ANTHROPIC_API_KEY={key}"
            found = True
            break
    if not found:
        env_lines.append(f"ANTHROPIC_API_KEY={key}")
    _env_path.write_text("\n".join(env_lines) + "\n", encoding="utf-8")


class LeadEngineApp:
    """Main application window."""

    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("Lead Engine — Lead Intelligence CRM")
        self.root.geometry("920x720")
        self.root.minsize(750, 550)
        self.root.configure(bg=BG)

        # Try to set icon (won't crash if missing)
        try:
            self.root.iconbitmap(default="")
        except Exception:
            pass

        # --- Variables ---
        self.csv_path = StringVar()
        self.output_dir = StringVar(value=str(BASE_DIR / "output"))
        self.row_limit = IntVar(value=0)
        self.ai_limit = IntVar(value=0)
        self.score_threshold = IntVar(value=config.MESSAGE_SCORE_THRESHOLD)
        self.skip_analysis = BooleanVar(value=False)
        self.skip_contacts = BooleanVar(value=False)
        self.skip_ai = BooleanVar(value=False)
        self.api_key_var = StringVar(value=config.ANTHROPIC_API_KEY)
        self.running = False

        self._build_ui()
        self._style_widgets()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        root = self.root

        # ---- Header ----
        header = ttk.Frame(root, style="Header.TFrame")
        header.pack(fill=X, padx=20, pady=(18, 8))
        ttk.Label(header, text="Lead Engine", style="Title.TLabel").pack(side=LEFT)
        ttk.Label(header, text="Score leads & generate outreach",
                  style="Subtitle.TLabel").pack(side=LEFT, padx=(14, 0))

        # ---- Main content in a scrollable-ish frame ----
        body = ttk.Frame(root, style="Body.TFrame")
        body.pack(fill=BOTH, expand=True, padx=20, pady=4)

        # -- Row 1: CSV file --
        row1 = ttk.LabelFrame(body, text="  Input  ", style="Card.TLabelframe")
        row1.pack(fill=X, pady=(0, 10))

        f1 = ttk.Frame(row1, style="Card.TFrame")
        f1.pack(fill=X, padx=12, pady=10)
        ttk.Label(f1, text="CSV File:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8))
        csv_entry = ttk.Entry(f1, textvariable=self.csv_path, width=58, style="Input.TEntry")
        csv_entry.grid(row=0, column=1, sticky=EW, padx=(0, 8))
        ttk.Button(f1, text="Browse", command=self._browse_csv,
                   style="Accent.TButton").grid(row=0, column=2)
        f1.columnconfigure(1, weight=1)

        f1b = ttk.Frame(row1, style="Card.TFrame")
        f1b.pack(fill=X, padx=12, pady=(0, 10))
        ttk.Label(f1b, text="Output Folder:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8))
        ttk.Entry(f1b, textvariable=self.output_dir, width=58, style="Input.TEntry").grid(
            row=0, column=1, sticky=EW, padx=(0, 8))
        ttk.Button(f1b, text="Browse", command=self._browse_output,
                   style="Accent.TButton").grid(row=0, column=2)
        f1b.columnconfigure(1, weight=1)

        # -- Row 2: Settings --
        row2 = ttk.LabelFrame(body, text="  Settings  ", style="Card.TLabelframe")
        row2.pack(fill=X, pady=(0, 10))

        grid = ttk.Frame(row2, style="Card.TFrame")
        grid.pack(fill=X, padx=12, pady=10)

        # API Key
        ttk.Label(grid, text="API Key:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8), pady=3)
        self.api_entry = ttk.Entry(grid, textvariable=self.api_key_var, width=44,
                                   show="*", style="Input.TEntry")
        self.api_entry.grid(row=0, column=1, sticky=EW, padx=(0, 8), pady=3, columnspan=2)
        ttk.Button(grid, text="Show", command=self._toggle_key_visibility,
                   style="Small.TButton", width=5).grid(row=0, column=3, pady=3)
        self._key_visible = False

        # Row limit
        ttk.Label(grid, text="Row Limit:", style="Label.TLabel").grid(
            row=1, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=99999, textvariable=self.row_limit,
                     width=10, style="Input.TSpinbox").grid(
            row=1, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(0 = all rows)", style="Dim.TLabel").grid(
            row=1, column=2, sticky=W, padx=(6, 0), pady=3)

        # AI limit
        ttk.Label(grid, text="AI Msg Limit:", style="Label.TLabel").grid(
            row=2, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=9999, textvariable=self.ai_limit,
                     width=10, style="Input.TSpinbox").grid(
            row=2, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(0 = unlimited)", style="Dim.TLabel").grid(
            row=2, column=2, sticky=W, padx=(6, 0), pady=3)

        # Score threshold
        ttk.Label(grid, text="Score Threshold:", style="Label.TLabel").grid(
            row=3, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=200, textvariable=self.score_threshold,
                     width=10, style="Input.TSpinbox").grid(
            row=3, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(min score for AI messages)", style="Dim.TLabel").grid(
            row=3, column=2, sticky=W, padx=(6, 0), pady=3)

        grid.columnconfigure(1, weight=1)

        # Checkboxes
        chk_frame = ttk.Frame(row2, style="Card.TFrame")
        chk_frame.pack(fill=X, padx=12, pady=(0, 10))
        ttk.Checkbutton(chk_frame, text="Skip website analysis",
                         variable=self.skip_analysis,
                         style="Toggle.TCheckbutton").pack(side=LEFT, padx=(0, 20))
        ttk.Checkbutton(chk_frame, text="Skip contact discovery",
                         variable=self.skip_contacts,
                         style="Toggle.TCheckbutton").pack(side=LEFT, padx=(0, 20))
        ttk.Checkbutton(chk_frame, text="Skip AI message generation",
                         variable=self.skip_ai,
                         style="Toggle.TCheckbutton").pack(side=LEFT)

        # -- Run button --
        btn_frame = ttk.Frame(body, style="Body.TFrame")
        btn_frame.pack(fill=X, pady=(0, 10))
        self.run_btn = ttk.Button(btn_frame, text="  Run Lead Engine  ",
                                   command=self._on_run, style="Run.TButton")
        self.run_btn.pack(side=LEFT)
        self.open_btn = ttk.Button(btn_frame, text="Open Output Folder",
                                    command=self._open_output, style="Accent.TButton")
        self.open_btn.pack(side=LEFT, padx=(12, 0))

        # -- Progress --
        self.progress_var = StringVar(value="Ready")
        self.progress_bar = ttk.Progressbar(body, mode="determinate", length=400,
                                             style="Custom.Horizontal.TProgressbar")
        self.progress_bar.pack(fill=X, pady=(0, 4))
        ttk.Label(body, textvariable=self.progress_var,
                  style="Status.TLabel").pack(fill=X, pady=(0, 6))

        # -- Log area --
        log_frame = ttk.LabelFrame(body, text="  Log  ", style="Card.TLabelframe")
        log_frame.pack(fill=BOTH, expand=True, pady=(0, 8))
        self.log = scrolledtext.ScrolledText(
            log_frame, height=10, wrap="word",
            bg=BG_CARD, fg=FG, insertbackground=FG,
            font=("Consolas", 9), relief="flat", borderwidth=0,
            selectbackground=ACCENT, selectforeground="#ffffff",
        )
        self.log.pack(fill=BOTH, expand=True, padx=6, pady=6)
        self.log.configure(state=DISABLED)

    # ------------------------------------------------------------------
    # Styling
    # ------------------------------------------------------------------
    def _style_widgets(self) -> None:
        s = ttk.Style()
        s.theme_use("clam")

        # Frame backgrounds
        s.configure("Header.TFrame", background=BG)
        s.configure("Body.TFrame", background=BG)
        s.configure("Card.TFrame", background=BG_CARD)
        s.configure("Card.TLabelframe", background=BG_CARD, foreground=FG_DIM,
                     bordercolor=BORDER, relief="groove")
        s.configure("Card.TLabelframe.Label", background=BG_CARD, foreground=FG_DIM,
                     font=("Segoe UI", 9, "bold"))

        # Labels
        s.configure("Title.TLabel", background=BG, foreground=FG,
                     font=("Segoe UI", 18, "bold"))
        s.configure("Subtitle.TLabel", background=BG, foreground=FG_DIM,
                     font=("Segoe UI", 11))
        s.configure("Label.TLabel", background=BG_CARD, foreground=FG,
                     font=("Segoe UI", 10))
        s.configure("Dim.TLabel", background=BG_CARD, foreground=FG_DIM,
                     font=("Segoe UI", 9))
        s.configure("Status.TLabel", background=BG, foreground=FG_DIM,
                     font=("Segoe UI", 9))

        # Entries
        s.configure("Input.TEntry", fieldbackground=BG, foreground=FG,
                     insertcolor=FG, bordercolor=BORDER, lightcolor=BORDER,
                     darkcolor=BORDER)
        s.configure("Input.TSpinbox", fieldbackground=BG, foreground=FG,
                     insertcolor=FG, bordercolor=BORDER, arrowcolor=FG_DIM)

        # Buttons
        s.configure("Accent.TButton", background=ACCENT, foreground="#ffffff",
                     font=("Segoe UI", 9, "bold"), borderwidth=0, padding=(10, 4))
        s.map("Accent.TButton",
              background=[("active", ACCENT_HOV), ("disabled", BORDER)])

        s.configure("Small.TButton", background=BG_CARD, foreground=FG_DIM,
                     font=("Segoe UI", 8), borderwidth=1, padding=(4, 2))
        s.map("Small.TButton", background=[("active", BORDER)])

        s.configure("Run.TButton", background=SUCCESS, foreground=BG,
                     font=("Segoe UI", 12, "bold"), borderwidth=0, padding=(18, 8))
        s.map("Run.TButton",
              background=[("active", "#6dff96"), ("disabled", BORDER)],
              foreground=[("disabled", FG_DIM)])

        # Checkbuttons
        s.configure("Toggle.TCheckbutton", background=BG_CARD, foreground=FG,
                     font=("Segoe UI", 10), indicatorcolor=BG,
                     indicatorrelief="flat")
        s.map("Toggle.TCheckbutton",
              indicatorcolor=[("selected", ACCENT), ("!selected", BG)],
              background=[("active", BG_CARD)])

        # Progress bar
        s.configure("Custom.Horizontal.TProgressbar",
                     troughcolor=BG_CARD, background=ACCENT, bordercolor=BG_CARD,
                     lightcolor=ACCENT, darkcolor=ACCENT)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def _browse_csv(self) -> None:
        path = filedialog.askopenfilename(
            title="Select CSV file",
            initialdir=str(BASE_DIR),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.csv_path.set(path)

    def _browse_output(self) -> None:
        path = filedialog.askdirectory(
            title="Select output folder",
            initialdir=self.output_dir.get() or str(BASE_DIR),
        )
        if path:
            self.output_dir.set(path)

    def _toggle_key_visibility(self) -> None:
        self._key_visible = not self._key_visible
        self.api_entry.configure(show="" if self._key_visible else "*")

    def _open_output(self) -> None:
        out = Path(self.output_dir.get())
        if out.exists():
            webbrowser.open(str(out))
        else:
            messagebox.showinfo("Output", "Output folder doesn't exist yet.\nRun the engine first.")

    def _log(self, msg: str, tag: str = "") -> None:
        """Append a message to the log widget (thread-safe via after())."""
        def _write():
            self.log.configure(state=NORMAL)
            self.log.insert(END, msg + "\n", tag)
            self.log.see(END)
            self.log.configure(state=DISABLED)
        self.root.after(0, _write)

    def _set_progress(self, value: int, text: str = "") -> None:
        def _update():
            self.progress_bar["value"] = value
            if text:
                self.progress_var.set(text)
        self.root.after(0, _update)

    def _set_running(self, running: bool) -> None:
        def _update():
            self.running = running
            state = DISABLED if running else NORMAL
            self.run_btn.configure(state=state)
        self.root.after(0, _update)

    # ------------------------------------------------------------------
    # Main pipeline (runs in a background thread)
    # ------------------------------------------------------------------
    def _on_run(self) -> None:
        # Validate inputs
        csv = self.csv_path.get().strip()
        if not csv:
            messagebox.showwarning("Missing CSV", "Please select a CSV file first.")
            return
        if not Path(csv).exists():
            messagebox.showerror("File Not Found", f"Cannot find:\n{csv}")
            return

        # Save API key if user typed one
        key = self.api_key_var.get().strip()
        if key and key != config.ANTHROPIC_API_KEY:
            _save_api_key(key)
            self._log("API key saved.")

        # Clear log
        self.log.configure(state=NORMAL)
        self.log.delete("1.0", END)
        self.log.configure(state=DISABLED)

        # Run in background thread so GUI stays responsive
        self._set_running(True)
        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self) -> None:
        try:
            csv_path = self.csv_path.get().strip()
            output_dir = self.output_dir.get().strip() or str(BASE_DIR / "output")
            limit = self.row_limit.get()
            ai_limit = self.ai_limit.get()
            threshold = self.score_threshold.get()
            skip_analyze = self.skip_analysis.get()
            skip_contacts = self.skip_contacts.get()
            skip_ai = self.skip_ai.get()

            t_start = time.time()

            # ---- Stage 1: Load CSV ----
            self._set_progress(5, "Loading CSV ...")
            self._log("[1/5] Loading and normalising CSV ...")
            businesses = load_csv(csv_path)
            if limit:
                businesses = businesses[:limit]
            self._log(f"      Loaded {len(businesses)} businesses.")
            self._set_progress(10)

            # ---- Stage 2: Website analysis ----
            do_skip = skip_analyze or config.NO_WEBSITE_ONLY
            if do_skip:
                reason = "no-website-only mode" if config.NO_WEBSITE_ONLY else "skipped"
                self._set_progress(15, f"Skipping website analysis ({reason})")
                self._log(f"[2/5] Skipping website analysis ({reason}).")
                analyses = {}
            else:
                self._set_progress(15, "Analysing websites ...")
                self._log(f"[2/5] Analysing websites (this may take a moment) ...")
                analyses = asyncio.run(analyze_websites(businesses))
                ok = sum(1 for a in analyses.values() if a.reachable)
                self._log(f"      {ok} reachable / {len(analyses)} checked.")
            self._set_progress(20)

            # ---- Stage 3: Contact discovery ----
            if skip_contacts:
                self._set_progress(45, "Skipping contact discovery")
                self._log("[3/5] Skipping contact discovery.")
            else:
                self._set_progress(25, "Discovering contacts ...")
                self._log("[3/5] Discovering contacts (social media, emails) ...")
                self._log("      This searches for each business — may take a few minutes.")

                def _contact_progress(idx, total, name, info):
                    pct = 25 + int((idx + 1) / total * 25)  # 25% → 50%
                    parts = []
                    if info.instagram:
                        parts.append("IG")
                    if info.facebook:
                        parts.append("FB")
                    if info.tiktok:
                        parts.append("TT")
                    if info.yelp:
                        parts.append("Yelp")
                    if info.email:
                        parts.append("Email")
                    found = ", ".join(parts) if parts else "none"
                    self._set_progress(pct, f"Contacts: {idx+1}/{total}")
                    self._log(f"      [{idx+1}/{total}] {name} → {found}")

                contacts = discover_all_contacts(businesses, progress_callback=_contact_progress)
                for i, biz in enumerate(businesses):
                    info = contacts.get(i)
                    if info:
                        biz["instagram"] = info.instagram
                        biz["facebook"] = info.facebook
                        biz["tiktok"] = info.tiktok
                        biz["email"] = info.email
                        biz["yelp"] = info.yelp
                        biz["contact_methods_found"] = info.contact_methods_found
                        biz["best_contact_channel"] = info.best_contact_channel
                        biz["instagram_confidence"] = info.instagram_confidence
                        biz["facebook_confidence"] = info.facebook_confidence
                        biz["tiktok_confidence"] = info.tiktok_confidence
                        biz["email_confidence"] = info.email_confidence
                found_any = sum(1 for c in contacts.values() if c.contact_methods_found > 0)
                self._log(f"      Found contacts for {found_any}/{len(businesses)} businesses.")
            self._set_progress(50)

            # ---- Stage 4: Scoring ----
            self._set_progress(55, "Scoring leads ...")
            self._log("[4/5] Scoring leads ...")
            businesses = score_all(businesses, analyses)
            top = businesses[0] if businesses else {}
            self._log(f"      Top lead: {top.get('business_name', '?')} "
                      f"(score={top.get('lead_score', 0)})")
            if top.get("recommended_pitch_label"):
                self._log(f"      Angle: {top.get('recommended_pitch_label')}")
            self._set_progress(65)

            # ---- Stage 5: AI messages ----
            if skip_ai or not config.ANTHROPIC_API_KEY:
                reason = "disabled" if skip_ai else "no API key"
                self._set_progress(85, f"Skipping AI messages ({reason})")
                self._log(f"[5/5] Skipping AI messages ({reason}).")
                for biz in businesses:
                    for field in ("email_subject", "email_message",
                                  "contact_form_message", "dm_message",
                                  "follow_up_message", "call_script"):
                        biz[field] = ""
                    biz["message_error"] = "skipped" if skip_ai else "api_key_missing"
            else:
                self._set_progress(70, "Generating outreach messages ...")
                self._log("[5/5] Generating outreach messages with Claude ...")
                businesses = generate_messages(
                    businesses,
                    score_threshold=threshold,
                    max_messages=ai_limit,
                )
                gen_count = sum(1 for b in businesses if b.get("email_message"))
                self._log(f"      Generated messages for {gen_count} businesses.")
            self._set_progress(90)

            # ---- Write outputs ----
            self._set_progress(92, "Writing output files ...")
            self._log("Writing output files ...")
            files = write_outputs(businesses, output_dir)
            for label, path in files.items():
                self._log(f"  {label:20s} -> {path}")

            elapsed = time.time() - t_start
            self._set_progress(100, f"Done in {elapsed:.1f}s")
            self._log(f"\nDone in {elapsed:.1f}s!")

            # Summary stats in the log
            self._log(f"\nTotal: {len(businesses)} leads (no website)")
            self._log(f"Top 5 leads:")
            for b in businesses[:5]:
                self._log(f"  [{b.get('lead_score', 0):>3} pts]  {b.get('business_name', '?')}")

        except Exception as exc:
            self._log(f"\nERROR: {exc}")
            self._set_progress(0, f"Error: {exc}")
        finally:
            self._set_running(False)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    app = LeadEngineApp()
    app.run()


if __name__ == "__main__":
    main()
