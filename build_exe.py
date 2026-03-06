"""
build_exe.py — One-click script to package the lead tool into a standalone .exe

Run this once:
    python build_exe.py

It will create:
    dist/LeadEngine/LeadEngine.exe   (plus supporting files)

To use:
    1. Copy the entire dist/LeadEngine/ folder wherever you want
    2. Put your CSV file in that same folder
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

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LeadEngine",
        "--console",                   # keep the terminal window (we need it for interactive prompts)
        "--noconfirm",                 # overwrite previous build without asking
        "--add-data", "lead_engine;lead_engine",  # bundle the package
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
        "run.py",
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
    print(f"\n  Your .exe is at:\n  {dist_dir / 'LeadEngine.exe'}\n")
    print("  To use it:")
    print("  1. Copy the entire dist/LeadEngine/ folder to where you want")
    print("  2. Drop your CSV file into that folder")
    print("  3. Double-click LeadEngine.exe")
    print("  4. On first run it will ask for your Anthropic API key")
    print("     (saved automatically so you only do it once)")
    print()


if __name__ == "__main__":
    main()
