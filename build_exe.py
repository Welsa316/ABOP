"""
build_exe.py — One-click script to package the lead tool into a standalone .exe

Run this once:
    python build_exe.py

It will create:
    dist/LeadEngine.exe   (single self-contained file)

To use:
    1. Copy LeadEngine.exe wherever you want
    2. Double-click it
    3. Browse to your CSV and hit Run
"""

import subprocess
import sys
from pathlib import Path


def main():
    # Ensure PyInstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("Installing PyInstaller ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    print("Building .exe (onefile mode) ...")
    print("This may take a minute on first run.\n")

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LeadEngine",
        "--onefile",
        "--windowed",
        "--noconfirm",
        "--add-data", "lead_engine;lead_engine",
        "--collect-all", "tkinter",
        "--hidden-import", "httpx",
        "--hidden-import", "dotenv",
        "--hidden-import", "certifi",
        "--hidden-import", "httpcore",
        "--hidden-import", "h11",
        "--hidden-import", "sniffio",
        "--hidden-import", "anyio",
        "--hidden-import", "idna",
        "--hidden-import", "charset_normalizer",
        "--hidden-import", "openpyxl",
        "--hidden-import", "anthropic",
        "--hidden-import", "bs4",
        "--hidden-import", "resend",
        "--hidden-import", "googlesearch",
        "--hidden-import", "sqlite3",
        "gui.py",
    ]

    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    if result.returncode != 0:
        print("\nBuild failed. Check the errors above.")
        sys.exit(1)

    exe_path = Path(__file__).parent / "dist" / "LeadEngine.exe"

    print("\n" + "=" * 55)
    print("  BUILD SUCCESSFUL")
    print("=" * 55)
    print(f"\n  Your app is at:\n  {exe_path}\n")
    print("  To use it:")
    print("  1. Copy LeadEngine.exe wherever you want")
    print("  2. Double-click it")
    print("  3. Browse to your CSV and hit Run")
    print()


if __name__ == "__main__":
    main()
