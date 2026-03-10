"""
gui.py — Desktop GUI for the Lead Scoring Tool.

Double-click this file (or run `python gui.py`) to launch the app.
Uses tkinter (built into Python — no extra install needed).
"""

import sys
import threading
import time
import webbrowser
from pathlib import Path
from tkinter import (
    Tk, ttk, StringVar, IntVar,
    filedialog, messagebox, scrolledtext,
    W, EW, END, DISABLED, NORMAL, LEFT, BOTH, X, TOP,
)

# ---------------------------------------------------------------------------
# Resolve base directory — works both as .py script and as frozen .exe
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

from lead_engine.loader import load_csv
from lead_engine.scorer import score_all
from lead_engine.writer import write_outputs

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
        self.root.title("Lead Engine — Business Lead Scorer")
        self.root.geometry("920x620")
        self.root.minsize(750, 450)
        self.root.configure(bg=BG)

        try:
            self.root.iconbitmap(default="")
        except Exception:
            pass

        # --- Variables ---
        self.csv_path = StringVar()
        self.output_dir = StringVar(value=str(BASE_DIR / "output"))
        self.row_limit = IntVar(value=0)
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
        ttk.Label(header, text="Score & categorize business leads",
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

        ttk.Label(grid, text="Row Limit:", style="Label.TLabel").grid(
            row=0, column=0, sticky=W, padx=(0, 8), pady=3)
        ttk.Spinbox(grid, from_=0, to=99999, textvariable=self.row_limit,
                     width=10, style="Input.TSpinbox").grid(
            row=0, column=1, sticky=W, pady=3)
        ttk.Label(grid, text="(0 = all rows)", style="Dim.TLabel").grid(
            row=0, column=2, sticky=W, padx=(6, 0), pady=3)
        grid.columnconfigure(1, weight=1)

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

        s.configure("Accent.TButton", background=ACCENT, foreground="#ffffff",
                     font=("Segoe UI", 9, "bold"), borderwidth=0, padding=(10, 4))
        s.map("Accent.TButton",
              background=[("active", ACCENT_HOV), ("disabled", BORDER)])

        s.configure("Run.TButton", background=SUCCESS, foreground=BG,
                     font=("Segoe UI", 12, "bold"), borderwidth=0, padding=(18, 8))
        s.map("Run.TButton",
              background=[("active", "#6dff96"), ("disabled", BORDER)],
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

    def _set_running(self, running: bool) -> None:
        def _update():
            self.running = running
            state = DISABLED if running else NORMAL
            self.run_btn.configure(state=state)
        self.root.after(0, _update)

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
            csv_path = self.csv_path.get().strip()
            output_dir = self.output_dir.get().strip() or str(BASE_DIR / "output")
            limit = self.row_limit.get()

            t_start = time.time()

            # ---- Stage 1: Load CSV ----
            self._set_progress(10, "Loading CSV ...")
            self._log("[1/3] Loading CSV ...")
            businesses = load_csv(csv_path)
            if limit:
                businesses = businesses[:limit]
            self._log(f"      Loaded {len(businesses)} businesses.")

            no_website = sum(1 for b in businesses if not b.get("website"))
            has_website = len(businesses) - no_website
            self._log(f"      {no_website} without website, {has_website} with website.")
            self._set_progress(30)

            # ---- Stage 2: Score ----
            self._set_progress(40, "Scoring leads ...")
            self._log("[2/3] Scoring leads ...")
            businesses = score_all(businesses)

            if businesses:
                top = businesses[0]
                self._log(f"      Top lead: {top.get('business_name', '?')} "
                          f"(score={top.get('lead_score', 0)})")
            self._set_progress(60)

            # ---- Stage 3: Write Excel ----
            self._set_progress(70, "Writing Excel ...")
            self._log("[3/3] Writing Excel tracker ...")
            files = write_outputs(businesses, output_dir)
            for label, path in files.items():
                self._log(f"      {label} -> {path}")

            elapsed = time.time() - t_start
            self._set_progress(100, f"Done in {elapsed:.1f}s")
            self._log(f"\nDone in {elapsed:.1f}s!")

            self._log(f"\nTotal: {len(businesses)} leads")
            self._log(f"  {no_website} without website (highest priority)")
            self._log(f"  {has_website} with website")
            self._log(f"\nTop 5:")
            for b in businesses[:5]:
                website_tag = "" if b.get("has_website") else " [NO WEBSITE]"
                self._log(f"  [{b.get('lead_score', 0):>3} pts]  "
                          f"{b.get('business_name', '?')}{website_tag}")

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
