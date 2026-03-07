"""
build_exe.py — One-click script to package the lead tool into a standalone .exe

Run this once:
    python build_exe.py

It will create:
    dist/LeadEngine/LeadEngine.exe   (the GUI app)

To use:
    1. Copy the entire dist/LeadEngine/ folder wherever you want
    2. Put your CSV file in that same folder (or browse to it from the app)
    3. Double-click LeadEngine.exe
"""

import subprocess
import sys
import shutil
from pathlib import Path

def main():
    # Ensure PyInstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("Installing PyInstaller ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    print("Building .exe ...")
    print("This may take a minute on first run.\n")

    # Locate Tcl/Tk data directories so PyInstaller bundles them correctly.
    # Without these the .exe crashes with "Tcl data directory ... not found".
    import tkinter
    import os

    tcl_tk_data = []
    # Python embeds tcl/tk under its install prefix (e.g. Python312/tcl/tcl8.6)
    python_dir = Path(sys.executable).parent
    for candidate_parent in [python_dir / "tcl", python_dir / "lib", Path(sys.prefix) / "tcl"]:
        if not candidate_parent.is_dir():
            continue
        for child in candidate_parent.iterdir():
            if child.is_dir() and child.name.startswith(("tcl", "tk")):
                tcl_tk_data += ["--add-data", f"{child};{child.name}"]

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LeadEngine",
        "--windowed",                  # no terminal window — proper GUI app
        "--noconfirm",                 # overwrite previous build without asking
        "--add-data", "lead_engine;lead_engine",  # bundle the package
        *tcl_tk_data,
        "--collect-all", "tkinter",
        "--hidden-import", "anthropic",
        "--hidden-import", "httpx",
        "--hidden-import", "bs4",
        "--hidden-import", "dotenv",
        "--hidden-import", "certifi",
        "--hidden-import", "httpcore",
        "--hidden-import", "h11",
        "--hidden-import", "sniffio",
        "--hidden-import", "anyio",
        "--hidden-import", "socksio",
        "--hidden-import", "idna",
        "--hidden-import", "charset_normalizer",
        "--hidden-import", "duckduckgo_search",
        "--hidden-import", "primp",
        "--hidden-import", "lxml",
        "--hidden-import", "click",
        "--hidden-import", "openpyxl",
        "gui.py",                      # build the GUI version
    ]

    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    if result.returncode != 0:
        print("\nBuild failed. Check the errors above.")
        sys.exit(1)

    dist_dir = Path(__file__).parent / "dist" / "LeadEngine"

    # Copy .env.example into the dist folder so the user has a template
    env_example = Path(__file__).parent / ".env.example"
    if env_example.exists():
        shutil.copy(env_example, dist_dir / ".env.example")

    print("\n" + "=" * 55)
    print("  BUILD SUCCESSFUL")
    print("=" * 55)
    print(f"\n  Your app is at:\n  {dist_dir / 'LeadEngine.exe'}\n")
    print("  To use it:")
    print("  1. Copy the entire dist/LeadEngine/ folder to where you want")
    print("  2. Double-click LeadEngine.exe")
    print("  3. Browse to your CSV, paste your API key, and hit Run")
    print()


if __name__ == "__main__":
    main()
