"""Render every Graphviz file in docs/diagrams/ to docs/<name>.png.

Diagrams are kept as text (*.dot) so they are versioned and reviewable like
code. Rendering needs the Graphviz ``dot`` program:
    Windows: winget install graphviz   macOS: brew install graphviz
    Ubuntu/WSL: sudo apt install graphviz

    python scripts/render_diagrams.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "docs" / "diagrams"
OUTPUT_DIR = ROOT / "docs"


def main() -> int:
    dot = shutil.which("dot")
    if dot is None:
        print("Graphviz 'dot' is not installed; the committed PNG files are still valid.")
        return 1
    for source in sorted(SOURCE_DIR.glob("*.dot")):
        target = OUTPUT_DIR / f"{source.stem}.png"
        subprocess.run([dot, "-Tpng", "-Gdpi=130", str(source), "-o", str(target)], check=True)
        print(f"rendered {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
