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
    import tkinter
    import os

    tcl_tk_data = []
    python_dir = Path(sys.executable).parent
    for candidate_parent in [python_dir / "tcl", python_dir / "lib", Path(sys.prefix) / "tcl"]:
        if not candidate_parent.is_dir():
            continue
        for child in candidate_parent.iterdir():
            if child.is_dir() and child.name.startswith(("tcl", "tk")):
                tcl_tk_data += ["--add-data", f"{child};{child.name}"]

    # Find and bundle the Python DLL explicitly
    # (fixes "Failed to load Python DLL" on some Windows machines)
    add_python_dll = []
    for search_dir in [python_dir, Path(sys.prefix), Path(sys.base_prefix)]:
        matches = list(search_dir.glob("python3*.dll"))
        if matches:
            add_python_dll = ["--add-binary", f"{matches[0]};."]
            print(f"  Found Python DLL: {matches[0]}")
            break

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LeadEngine",
        "--windowed",
        "--noconfirm",
        "--add-data", "lead_engine;lead_engine",
        *add_python_dll,
        *tcl_tk_data,
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
        "gui.py",
    ]

    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    if result.returncode != 0:
        print("\nBuild failed. Check the errors above.")
        sys.exit(1)

    dist_dir = Path(__file__).parent / "dist" / "LeadEngine"

    print("\n" + "=" * 55)
    print("  BUILD SUCCESSFUL")
    print("=" * 55)
    print(f"\n  Your app is at:\n  {dist_dir / 'LeadEngine.exe'}\n")
    print("  To use it:")
    print("  1. Copy the entire dist/LeadEngine/ folder to where you want")
    print("  2. Double-click LeadEngine.exe")
    print("  3. Browse to your CSV and hit Run")
    print()


if __name__ == "__main__":
    main()
