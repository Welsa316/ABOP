"""
gui.py — Desktop GUI for the Lead Engine Tool.

Double-click this file (or run `python gui.py`) to launch the app.
Uses tkinter (built into Python — no extra install needed).
"""

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path
from tkinter import (
    Tk, ttk, StringVar, IntVar, BooleanVar,
    filedialog, messagebox, scrolledtext, simpledialog,
    W, EW, END, DISABLED, NORMAL, LEFT, BOTH, X, TOP,
)

# ---------------------------------------------------------------------------
# Resolve base directory — works both as .py script and as frozen .exe
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

# Load .env file
_env_path = BASE_DIR / ".env"
try:
    from dotenv import load_dotenv
    load_dotenv(_env_path)
except ImportError:
    pass

import asyncio

from lead_engine import config
config.ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

from lead_engine.loader import load_csv
from lead_engine.analyzer import analyze_websites
from lead_engine.auditor import audit_websites
from lead_engine.scorer import score_all
from lead_engine.messenger import generate_messages
from lead_engine.writer import write_outputs, load_contacted
from lead_engine.contact_discovery import discover_all_contacts

# Outreach system
from lead_engine.outreach import outreach_config as outreach_cfg
from lead_engine.outreach.campaign import (
    run_ingest_pipeline,
    approve_all_reviewed,
    send_approved,
    get_campaign_stats,
)

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
BG          = "#1e1e2e"
BG_CARD     = "#2a2a3d"
FG          = "#e0e0e0"
FG_DIM      = "#8888aa"
ACCENT      = "#7c6ff7"
ACCENT_HOV  = "#9580ff"
SUCCESS     = "#50fa7b"
BORDER      = "#3a3a5c"


class LeadEngineApp:
    """Main application window."""

    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("Lead Engine — Business Lead Scorer & Outreach")
        self.root.geometry("920x820")
        self.root.minsize(750, 650)
        self.root.configure(bg=BG)

        try:
            self.root.iconbitmap(default="")
        except Exception:
            pass

        # --- Variables ---
        self.csv_path = StringVar()
        self.output_dir = StringVar(value=str(BASE_DIR / "output"))
        self.row_limit = IntVar(value=0)
        self.msg_limit = IntVar(value=0)
        self.score_threshold = IntVar(value=config.MESSAGE_SCORE_THRESHOLD)
        self.skip_audit = BooleanVar(value=False)
        self.skip_contacts = BooleanVar(value=False)
        self.skip_ai = BooleanVar(value=False)
        self.auto_send = BooleanVar(value=False)
        self.running = False

        # Outreach config — load from .env
        self.email_provider = StringVar(value=os.getenv("EMAIL_PROVIDER", "gmail"))
        self.gmail_app_password = StringVar(value=os.getenv("GMAIL_APP_PASSWORD", ""))
        self.resend_key = StringVar(value=os.getenv("RESEND_API_KEY", ""))
        self.from_email = StringVar(value=os.getenv("OUTREACH_FROM_EMAIL", ""))
        self.from_name = StringVar(value=os.getenv("OUTREACH_FROM_NAME", ""))
        self.your_name = StringVar(value=os.getenv("OUTREACH_YOUR_NAME", ""))
        self.your_business = StringVar(value=os.getenv("OUTREACH_YOUR_BUSINESS", ""))
        self.your_service = StringVar(value=os.getenv("OUTREACH_YOUR_SERVICE", ""))
        self.your_website = StringVar(value=os.getenv("OUTREACH_YOUR_WEBSITE", ""))

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
        ttk.Label(header, text="Score, audit & generate outreach",
                  style="Subtitle.TLabel").pack(side=LEFT, padx=(14, 0))

        # ---- Main content ----
        body = ttk.Frame(root, style="Body.TFrame")
        body.pack(fill=BOTH, expand=True, padx=20, pady=4)

        # -- Input section --
        row1 = ttk.LabelFrame(body, text="  Input  ", style="Card.TLabelframe")
        row1.pack(fill=X, pady=(0, 10))

        f1 = ttk.Frame(row1, style="Card.TFrame")
        f1.pack(fill=X, padx=12, pady=10)
        ttk.Label(f1, text="CSV File:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8))
        ttk.Entry(f1, textvariable=self.csv_path, width=58, style="Input.TEntry").grid(
            row=0, column=1, sticky=EW, padx=(0, 8))
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

        # -- Settings section --
        row2 = ttk.LabelFrame(body, text="  Settings  ", style="Card.TLabelframe")
        row2.pack(fill=X, pady=(0, 10))

        grid = ttk.Frame(row2, style="Card.TFrame")
        grid.pack(fill=X, padx=12, pady=10)

        # Row 0: Row Limit
        ttk.Label(grid, text="Row Limit:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=99999, textvariable=self.row_limit,
                     width=10, style="Input.TSpinbox").grid(
            row=0, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(0 = all rows)", style="Dim.TLabel").grid(
            row=0, column=2, sticky=W, padx=(6, 0), pady=3)

        # Row 1: Message Limit
        ttk.Label(grid, text="Message Limit:", style="Label.TLabel").grid(
            row=1, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=9999, textvariable=self.msg_limit,
                     width=10, style="Input.TSpinbox").grid(
            row=1, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(0 = unlimited)", style="Dim.TLabel").grid(
            row=1, column=2, sticky=W, padx=(6, 0), pady=3)

        # Row 2: Score Threshold
        ttk.Label(grid, text="Score Threshold:", style="Label.TLabel").grid(
            row=2, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=100, textvariable=self.score_threshold,
                     width=10, style="Input.TSpinbox").grid(
            row=2, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(min score for messages)", style="Dim.TLabel").grid(
            row=2, column=2, sticky=W, padx=(6, 0), pady=3)

        # Row 3: Checkboxes
        cb_frame = ttk.Frame(grid, style="Card.TFrame")
        cb_frame.grid(row=3, column=0, columnspan=4, sticky=W, pady=(6, 0))

        ttk.Checkbutton(cb_frame, text="Skip Website Audit",
                        variable=self.skip_audit, style="Card.TCheckbutton").pack(
            side=LEFT, padx=(0, 16))
        ttk.Checkbutton(cb_frame, text="Skip Contact Discovery",
                        variable=self.skip_contacts, style="Card.TCheckbutton").pack(
            side=LEFT, padx=(0, 16))
        ttk.Checkbutton(cb_frame, text="Skip AI Messages",
                        variable=self.skip_ai, style="Card.TCheckbutton").pack(
            side=LEFT, padx=(0, 16))
        ttk.Checkbutton(cb_frame, text="Send Emails After Run",
                        variable=self.auto_send, style="Card.TCheckbutton").pack(
            side=LEFT, padx=(0, 16))

        grid.columnconfigure(1, weight=1)

        # -- Outreach settings (collapsible) --
        row3 = ttk.LabelFrame(body, text="  Email Outreach Settings  ",
                               style="Card.TLabelframe")
        row3.pack(fill=X, pady=(0, 10))

        og = ttk.Frame(row3, style="Card.TFrame")
        og.pack(fill=X, padx=12, pady=10)

        # Provider selector
        r = 0
        ttk.Label(og, text="Email Provider:", style="Label.TLabel").grid(
            row=r, column=0, sticky=W, padx=(0, 8), pady=2)
        provider_frame = ttk.Frame(og, style="Card.TFrame")
        provider_frame.grid(row=r, column=1, sticky=W, pady=2)
        ttk.Radiobutton(provider_frame, text="Gmail (free)",
                        variable=self.email_provider, value="gmail",
                        style="Card.TRadiobutton").pack(side=LEFT, padx=(0, 16))
        ttk.Radiobutton(provider_frame, text="Resend (custom domain)",
                        variable=self.email_provider, value="resend",
                        style="Card.TRadiobutton").pack(side=LEFT)

        outreach_fields = [
            (1, "Gmail App Password:", self.gmail_app_password, "(myaccount.google.com > App Passwords)"),
            (2, "Resend API Key:", self.resend_key, "(resend.com — free 100/day)"),
            (3, "From Email:", self.from_email, "(your Gmail or verified domain)"),
            (4, "From Name:", self.from_name, "(your display name)"),
            (5, "Your Name:", self.your_name, ""),
            (6, "Your Business:", self.your_business, ""),
            (7, "Your Service:", self.your_service, "(what you offer)"),
            (8, "Your Website:", self.your_website, ""),
        ]
        for r, label, var, hint in outreach_fields:
            ttk.Label(og, text=label, style="Label.TLabel").grid(
                row=r, column=0, sticky=W, padx=(0, 8), pady=2)
            show = "*" if ("Key" in label or "Password" in label) else None
            e = ttk.Entry(og, textvariable=var, width=45, style="Input.TEntry",
                          show=show or "")
            e.grid(row=r, column=1, sticky=EW, pady=2)
            if hint:
                ttk.Label(og, text=hint, style="Dim.TLabel").grid(
                    row=r, column=2, sticky=W, padx=(6, 0), pady=2)
        og.columnconfigure(1, weight=1)

        # -- Run / Stop buttons --
        btn_frame = ttk.Frame(body, style="Body.TFrame")
        btn_frame.pack(fill=X, pady=(0, 10))
        self.run_btn = ttk.Button(btn_frame, text="  Run Lead Engine  ",
                                   command=self._on_run, style="Run.TButton")
        self.run_btn.pack(side=LEFT)
        self.stop_btn = ttk.Button(btn_frame, text="  Stop  ",
                                    command=self._on_stop, style="Stop.TButton",
                                    state=DISABLED)
        self.stop_btn.pack(side=LEFT, padx=(12, 0))
        self.send_btn = ttk.Button(btn_frame, text="  Send Emails  ",
                                    command=self._on_send, style="Accent.TButton")
        self.send_btn.pack(side=LEFT, padx=(12, 0))
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

        s.configure("Header.TFrame", background=BG)
        s.configure("Body.TFrame", background=BG)
        s.configure("Card.TFrame", background=BG_CARD)
        s.configure("Card.TLabelframe", background=BG_CARD, foreground=FG_DIM,
                     bordercolor=BORDER, relief="groove")
        s.configure("Card.TLabelframe.Label", background=BG_CARD, foreground=FG_DIM,
                     font=("Segoe UI", 9, "bold"))

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

        s.configure("Input.TEntry", fieldbackground=BG, foreground=FG,
                     insertcolor=FG, bordercolor=BORDER, lightcolor=BORDER,
                     darkcolor=BORDER)
        s.configure("Input.TSpinbox", fieldbackground=BG, foreground=FG,
                     insertcolor=FG, bordercolor=BORDER, arrowcolor=FG_DIM)

        s.configure("Card.TCheckbutton", background=BG_CARD, foreground=FG,
                     font=("Segoe UI", 9))
        s.map("Card.TCheckbutton", background=[("active", BG_CARD)])

        s.configure("Card.TRadiobutton", background=BG_CARD, foreground=FG,
                     font=("Segoe UI", 10))
        s.map("Card.TRadiobutton", background=[("active", BG_CARD)])

        s.configure("Accent.TButton", background=ACCENT, foreground="#ffffff",
                     font=("Segoe UI", 9, "bold"), borderwidth=0, padding=(10, 4))
        s.map("Accent.TButton",
              background=[("active", ACCENT_HOV), ("disabled", BORDER)])

        s.configure("Run.TButton", background=SUCCESS, foreground=BG,
                     font=("Segoe UI", 12, "bold"), borderwidth=0, padding=(18, 8))
        s.map("Run.TButton",
              background=[("active", "#6dff96"), ("disabled", BORDER)],
              foreground=[("disabled", FG_DIM)])

        s.configure("Stop.TButton", background="#ff5555", foreground="#ffffff",
                     font=("Segoe UI", 12, "bold"), borderwidth=0, padding=(18, 8))
        s.map("Stop.TButton",
              background=[("active", "#ff6e6e"), ("disabled", BORDER)],
              foreground=[("disabled", FG_DIM)])

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

    def _open_output(self) -> None:
        out = Path(self.output_dir.get())
        if out.exists():
            webbrowser.open(str(out))
        else:
            messagebox.showinfo("Output", "Output folder doesn't exist yet.\nRun the engine first.")

    def _log(self, msg: str) -> None:
        def _write():
            self.log.configure(state=NORMAL)
            self.log.insert(END, msg + "\n")
            self.log.see(END)
            self.log.configure(state=DISABLED)
        self.root.after(0, _write)

    def _set_progress(self, value: int, text: str = "") -> None:
        def _update():
            self.progress_bar["value"] = value
            if text:
                self.progress_var.set(text)
        self.root.after(0, _update)

    def _on_stop(self) -> None:
        """Handle Stop button click — request graceful shutdown."""
        config.request_shutdown()
        self._log("\n*** Stop requested — finishing current task... ***")
        self._set_progress(self.progress_bar["value"], "Stopping...")
        self.stop_btn.configure(state=DISABLED)

    def _set_running(self, running: bool) -> None:
        def _update():
            self.running = running
            self.run_btn.configure(state=DISABLED if running else NORMAL)
            self.stop_btn.configure(state=NORMAL if running else DISABLED)
        self.root.after(0, _update)

    # ------------------------------------------------------------------
    # API key handling
    # ------------------------------------------------------------------
    def _ensure_api_key(self) -> bool:
        """Check for API key; prompt via dialog if missing. Returns True if key is available."""
        if config.ANTHROPIC_API_KEY:
            return True

        # Ask user for key via dialog (must run on main thread)
        result = [None]
        event = threading.Event()

        def _ask():
            key = simpledialog.askstring(
                "API Key Required",
                "Enter your Anthropic API key:\n"
                "(get one at console.anthropic.com/settings/keys)\n\n"
                "Leave blank to skip AI features this run.",
                parent=self.root,
            )
            result[0] = key
            event.set()

        self.root.after(0, _ask)
        event.wait()

        key = (result[0] or "").strip()
        if not key:
            return False

        # Save to config and .env
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
        self._log(f"      API key saved to {_env_path}")
        return True

    # ------------------------------------------------------------------
    # Outreach config helpers
    # ------------------------------------------------------------------
    def _save_outreach_config(self) -> None:
        """Persist outreach settings to .env and update runtime config."""
        env_vars = {
            "EMAIL_PROVIDER": self.email_provider.get().strip(),
            "GMAIL_APP_PASSWORD": self.gmail_app_password.get().strip(),
            "RESEND_API_KEY": self.resend_key.get().strip(),
            "OUTREACH_FROM_EMAIL": self.from_email.get().strip(),
            "OUTREACH_FROM_NAME": self.from_name.get().strip(),
            "OUTREACH_YOUR_NAME": self.your_name.get().strip(),
            "OUTREACH_YOUR_BUSINESS": self.your_business.get().strip(),
            "OUTREACH_YOUR_SERVICE": self.your_service.get().strip(),
            "OUTREACH_YOUR_WEBSITE": self.your_website.get().strip(),
        }

        # Update runtime config
        outreach_cfg.EMAIL_PROVIDER = env_vars["EMAIL_PROVIDER"]
        outreach_cfg.GMAIL_APP_PASSWORD = env_vars["GMAIL_APP_PASSWORD"]
        outreach_cfg.RESEND_API_KEY = env_vars["RESEND_API_KEY"]
        outreach_cfg.FROM_EMAIL = env_vars["OUTREACH_FROM_EMAIL"]
        outreach_cfg.FROM_NAME = env_vars["OUTREACH_FROM_NAME"]
        outreach_cfg.ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY
        outreach_cfg.YOUR_NAME = env_vars["OUTREACH_YOUR_NAME"]
        outreach_cfg.YOUR_BUSINESS = env_vars["OUTREACH_YOUR_BUSINESS"]
        outreach_cfg.YOUR_SERVICE = env_vars["OUTREACH_YOUR_SERVICE"]
        outreach_cfg.YOUR_WEBSITE = env_vars["OUTREACH_YOUR_WEBSITE"]

        for k, v in env_vars.items():
            os.environ[k] = v

        # Save to .env file
        env_lines = []
        if _env_path.exists():
            env_lines = _env_path.read_text(encoding="utf-8").splitlines()

        for key, value in env_vars.items():
            if not value:
                continue
            found = False
            for i, line in enumerate(env_lines):
                if line.strip().startswith(key + "=") or line.strip().startswith(key + " ="):
                    env_lines[i] = f"{key}={value}"
                    found = True
                    break
            if not found:
                env_lines.append(f"{key}={value}")

        _env_path.write_text("\n".join(env_lines) + "\n", encoding="utf-8")

    def _validate_outreach_config(self) -> str | None:
        """Check outreach config is ready. Returns error message or None."""
        if not self.from_email.get().strip():
            return "From Email is required."

        provider = self.email_provider.get().strip()
        if provider == "gmail":
            if not self.gmail_app_password.get().strip():
                return (
                    "Gmail App Password is required.\n"
                    "Generate one at:\n"
                    "myaccount.google.com > Security > App Passwords"
                )
        else:
            if not self.resend_key.get().strip():
                return "Resend API Key is required.\nGet one free at resend.com"
        return None

    # ------------------------------------------------------------------
    # Send Emails action
    # ------------------------------------------------------------------
    def _on_send(self) -> None:
        """Handle Send Emails button click."""
        error = self._validate_outreach_config()
        if error:
            messagebox.showwarning("Outreach Config Missing", error)
            return

        output_dir = self.output_dir.get().strip() or str(BASE_DIR / "output")
        excel_path = Path(output_dir) / "lead_tracker.xlsx"
        if not excel_path.exists():
            messagebox.showwarning(
                "No Leads",
                "No lead_tracker.xlsx found.\nRun the lead engine first to generate leads.",
            )
            return

        self._set_running(True)
        thread = threading.Thread(target=self._run_send_pipeline, daemon=True)
        thread.start()

    def _run_send_pipeline(self) -> None:
        """Run the outreach send pipeline in a background thread."""
        try:
            config.reset_shutdown()
            self._save_outreach_config()
            output_dir = self.output_dir.get().strip() or str(BASE_DIR / "output")
            excel_path = str(Path(output_dir) / "lead_tracker.xlsx")

            # Point outreach config at correct paths
            outreach_cfg.LEAD_EXCEL_PATH = excel_path
            outreach_cfg.DB_PATH = str(Path(output_dir) / "outreach.db")

            # Step 1: Ingest leads from Excel into outreach DB
            self._set_progress(10, "Ingesting leads ...")
            self._log("\n=== Email Outreach ===")
            self._log("[1/4] Ingesting leads from Excel ...")
            summary = run_ingest_pipeline(excel_path)
            self._log(f"      New leads imported: {summary['ingested']}")
            self._log(f"      Duplicates skipped: {summary['skipped_duplicates']}")
            self._log(f"      Drafts generated:   {summary['drafts_generated']}")
            if summary.get("draft_errors"):
                self._log(f"      Draft errors:       {summary['draft_errors']}")
            self._set_progress(40)

            # Step 2: Auto-approve all reviewed leads
            self._set_progress(50, "Approving leads ...")
            self._log("[2/4] Auto-approving leads ...")
            approved_count = approve_all_reviewed()
            self._log(f"      Approved {approved_count} leads.")
            self._set_progress(60)

            # Step 3: Show stats before sending
            stats = get_campaign_stats()
            sendable = stats.get("approved", 0)
            self._log(f"[3/4] Ready to send {sendable} emails")
            self._log(f"      From: {outreach_cfg.FROM_NAME} <{outreach_cfg.FROM_EMAIL}>")
            self._log(f"      Daily cap: {outreach_cfg.DAILY_SEND_CAP}")

            if sendable == 0:
                self._log("      No approved leads to send.")
                self._set_progress(100, "No emails to send")
                return

            # Step 4: Send
            self._set_progress(70, f"Sending {sendable} emails ...")
            self._log(f"[4/4] Sending emails ...")
            sent, failed, skipped = send_approved(dry_run=False)

            self._set_progress(100, "Outreach complete!")
            self._log(f"\n=== Send Results ===")
            self._log(f"      Sent:    {sent}")
            self._log(f"      Failed:  {failed}")
            self._log(f"      Skipped: {skipped}")

            if sent > 0:
                self._log(f"\nEmails sent successfully!")

        except Exception as exc:
            self._log(f"\nOUTREACH ERROR: {exc}")
            self._set_progress(0, f"Send error: {exc}")
        finally:
            self._set_running(False)

    # ------------------------------------------------------------------
    # Pipeline (runs in background thread)
    # ------------------------------------------------------------------
    def _on_run(self) -> None:
        csv = self.csv_path.get().strip()
        if not csv:
            messagebox.showwarning("Missing CSV", "Please select a CSV file first.")
            return
        if not Path(csv).exists():
            messagebox.showerror("File Not Found", f"Cannot find:\n{csv}")
            return

        # Clear log
        self.log.configure(state=NORMAL)
        self.log.delete("1.0", END)
        self.log.configure(state=DISABLED)

        self._set_running(True)
        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self) -> None:
        try:
            config.reset_shutdown()
            csv_path = self.csv_path.get().strip()
            output_dir = self.output_dir.get().strip() or str(BASE_DIR / "output")
            limit = self.row_limit.get()
            msg_limit = self.msg_limit.get()
            score_thresh = self.score_threshold.get()
            do_audit = not self.skip_audit.get()
            do_contacts = not self.skip_contacts.get()
            do_ai = not self.skip_ai.get()

            t_start = time.time()

            # ---- Stage 1: Load CSV ----
            self._set_progress(5, "Loading CSV ...")
            self._log("[1/6] Loading CSV ...")
            businesses = load_csv(csv_path)
            if limit:
                businesses = businesses[:limit]
            self._log(f"      Loaded {len(businesses)} businesses.")

            no_listed = sum(1 for b in businesses if not b.get("website"))
            self._log(f"      {no_listed} without listed website.")
            self._set_progress(10)

            if config.is_shutting_down():
                self._log("\n*** Stopped. Partial results saved. ***")
                write_outputs(businesses, output_dir)
                self._set_progress(100, "Stopped (partial results saved)")
                return

            # ---- Stage 2: Website discovery & analysis ----
            self._set_progress(12, "Analysing websites ...")
            self._log("[2/6] Analysing & discovering websites ...")
            analyses = asyncio.run(analyze_websites(businesses))
            listed = sum(1 for a in analyses.values() if a.website_status == "listed")
            discovered = sum(1 for a in analyses.values() if a.website_status == "discovered")
            not_found = sum(1 for a in analyses.values() if a.website_status == "not_found")
            self._log(f"      {listed} listed, {discovered} discovered, {not_found} not found.")
            self._set_progress(25)

            if config.is_shutting_down():
                self._log("\n*** Stopped after website analysis. Saving results... ***")
                write_outputs(businesses, output_dir)
                self._set_progress(100, "Stopped (partial results saved)")
                return

            # ---- Stage 2b: Website content audit ----
            if do_audit:
                has_key = config.ANTHROPIC_API_KEY or self._ensure_api_key()
                if has_key:
                    self._set_progress(28, "Auditing website content ...")
                    self._log("[2b] Auditing website content with AI ...")
                    asyncio.run(audit_websites(businesses, analyses))
                    audited = sum(1 for b in businesses if b.get("website_audit"))
                    self._log(f"      Audited {audited} websites.")
                else:
                    self._log("[2b] Skipping website audit (no API key).")
            else:
                self._log("[2b] Skipping website audit.")
            self._set_progress(35)

            if config.is_shutting_down():
                self._log("\n*** Stopped after audit. Saving results... ***")
                write_outputs(businesses, output_dir)
                self._set_progress(100, "Stopped (partial results saved)")
                return

            # ---- Stage 3: Discover contacts ----
            if do_contacts:
                self._set_progress(38, "Discovering contacts ...")
                self._log("[3/6] Discovering contact emails ...")
                self._log("      Searching websites & DuckDuckGo (may take a minute) ...")
                contacts = discover_all_contacts(businesses)
                for i, biz in enumerate(businesses):
                    info = contacts.get(i)
                    if info:
                        biz["email"] = info.email
                        biz["email_confidence"] = info.email_confidence
                        biz["contact_methods_found"] = info.contact_methods_found
                        biz["best_contact_channel"] = info.best_contact_channel
                emails_found = sum(1 for c in contacts.values() if c.email)
                self._log(f"      Found emails for {emails_found}/{len(businesses)} businesses.")
            else:
                self._log("[3/6] Skipping contact discovery.")
            self._set_progress(55)

            if config.is_shutting_down():
                self._log("\n*** Stopped after contact discovery. Saving results... ***")
                write_outputs(businesses, output_dir)
                self._set_progress(100, "Stopped (partial results saved)")
                return

            # ---- Stage 4: Score ----
            self._set_progress(58, "Scoring leads ...")
            self._log("[4/6] Scoring leads ...")
            businesses = score_all(businesses, analyses)

            if businesses:
                top = businesses[0]
                self._log(f"      Top lead: {top.get('business_name', '?')} "
                          f"(score={top.get('lead_score', 0)})")
            self._set_progress(65)

            if config.is_shutting_down():
                self._log("\n*** Stopped after scoring. Saving results... ***")
                write_outputs(businesses, output_dir)
                self._set_progress(100, "Stopped (partial results saved)")
                return

            # ---- Stage 5: AI message generation ----
            if do_ai:
                has_key = config.ANTHROPIC_API_KEY or self._ensure_api_key()
                if has_key:
                    self._set_progress(68, "Generating outreach messages ...")
                    self._log("[5/6] Generating outreach messages with Claude ...")

                    contacted = load_contacted(output_dir)
                    if contacted:
                        self._log(f"      Skipping {len(contacted)} previously contacted businesses.")

                    limit_text = str(msg_limit) if msg_limit else "unlimited"
                    self._log(f"      Message limit: {limit_text}, score threshold: {score_thresh}")

                    businesses = generate_messages(
                        businesses,
                        score_threshold=score_thresh,
                        max_messages=msg_limit,
                        contacted_keys=contacted,
                    )

                    messaged = sum(1 for b in businesses
                                   if b.get("email_subject") and not b.get("message_error"))
                    self._log(f"      Generated messages for {messaged} businesses.")
                else:
                    self._log("[5/6] Skipping AI messages (no API key).")
                    for biz in businesses:
                        for field in ("email_subject", "email_message", "contact_form_message",
                                      "dm_message", "follow_up_message", "call_script"):
                            biz[field] = ""
                        biz["message_error"] = "api_key_missing"
            else:
                self._log("[5/6] Skipping AI messages.")
                for biz in businesses:
                    for field in ("email_subject", "email_message", "contact_form_message",
                                  "dm_message", "follow_up_message", "call_script"):
                        biz[field] = ""
                    biz["message_error"] = "skipped"
            self._set_progress(85)

            # ---- Stage 6: Write outputs ----
            self._set_progress(88, "Writing output files ...")
            self._log("[6/6] Writing output files ...")
            files = write_outputs(businesses, output_dir)
            for label, path in files.items():
                self._log(f"      {label} -> {path}")

            elapsed = time.time() - t_start
            self._set_progress(100, f"Done in {elapsed:.1f}s")
            self._log(f"\nDone in {elapsed:.1f}s!")

            self._log(f"\nTotal: {len(businesses)} leads")
            self._log(f"  {listed} with listed website")
            self._log(f"  {discovered} with discovered website (not on Google)")
            self._log(f"  {not_found} no website found (highest priority)")

            if do_ai and config.ANTHROPIC_API_KEY:
                messaged = sum(1 for b in businesses
                               if b.get("email_subject") and not b.get("message_error"))
                self._log(f"  {messaged} with outreach messages generated")

            self._log(f"\nTop 5:")
            for b in businesses[:5]:
                status = b.get("website_status", "not_found")
                tag = {"discovered": " [UNLISTED SITE]",
                       "not_found": " [NO SITE FOUND]"}.get(status, "")
                email_tag = f" [{b.get('email', '')}]" if b.get("email") else ""
                msg_tag = " [MSG]" if b.get("email_subject") else ""
                self._log(f"  [{b.get('lead_score', 0):>3} pts]  "
                          f"{b.get('business_name', '?')}{tag}{email_tag}{msg_tag}")

            # ---- Auto-send if enabled ----
            if self.auto_send.get():
                error = self._validate_outreach_config()
                if error:
                    self._log(f"\nSkipping auto-send: {error}")
                else:
                    self._run_send_pipeline()

        except Exception as exc:
            self._log(f"\nERROR: {exc}")
            self._set_progress(0, f"Error: {exc}")
        finally:
            self._set_running(False)

    # ------------------------------------------------------------------
    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    app = LeadEngineApp()
    app.run()


if __name__ == "__main__":
    main()
