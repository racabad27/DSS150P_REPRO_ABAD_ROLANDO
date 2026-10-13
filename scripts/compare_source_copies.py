"""Compare two copies of the source CSV byte-wise and record-wise.

The old project zip contained a copy of the dataset that had been opened and
re-saved in a spreadsheet program. This script shows that the two copies hold
the same 100,000 records even though their bytes (and checksums) differ, and it
writes the result as Markdown evidence:

    python scripts/compare_source_copies.py --other path/to/zip_copy.csv \
        --out docs/evidence/source_copies_comparison.md
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FORMATS = ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S")


def describe_bytes(path: Path) -> dict:
    data = path.read_bytes()
    lines = data.split(b"\n")
    return {
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
        "line_endings": "CRLF" if b"\r\n" in data else "LF",
        "first_data_line": lines[1].decode("utf-8").rstrip("\r"),
        "last_data_line": next(l for l in reversed(lines) if l.strip()).decode("utf-8").rstrip("\r"),
    }


def parse_timestamps(df: pd.DataFrame) -> tuple[pd.Series, str]:
    for fmt in FORMATS:
        parsed = pd.to_datetime(df["Date"] + " " + df["Time"], format=fmt, errors="coerce")
        if parsed.notna().all():
            return parsed, fmt
    raise ValueError("timestamps match none of the known formats")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--authoritative", default=str(ROOT / "data/source/industrial_fire_risk_data.csv"))
    parser.add_argument("--other", required=True, help="the copy to compare (e.g. from the old zip)")
    parser.add_argument("--out", help="write the Markdown report here (default: print)")
    args = parser.parse_args()

    a_path, b_path = Path(args.authoritative), Path(args.other)
    a_bytes, b_bytes = describe_bytes(a_path), describe_bytes(b_path)
    a, b = pd.read_csv(a_path), pd.read_csv(b_path)
    a_ts, a_fmt = parse_timestamps(a)
    b_ts, b_fmt = parse_timestamps(b)

    rows = [("Same column names and order", list(a.columns) == list(b.columns)),
            ("Same row count", len(a) == len(b)),
            (f"Same timestamps (A parsed as `{a_fmt}`, B as `{b_fmt}`)", bool((a_ts == b_ts).all()))]
    mismatched = []
    for column in a.columns:
        if column in ("Date", "Time"):
            continue
        if pd.api.types.is_numeric_dtype(a[column]):
            same = np.array_equal(a[column].to_numpy(float), b[column].to_numpy(float), equal_nan=True)
        else:
            same = bool((a[column].astype(str) == b[column].astype(str)).all())
        if not same:
            mismatched.append(column)
    rows.append(("All other 18 columns equal value by value (NaN = NaN)", not mismatched))

    report = [
        "# Evidence: the two copies of the source CSV",
        "",
        "| | A: authoritative (Kaggle) | B: copy inside the old project zip |",
        "|---|---|---|",
        f"| File | `{a_path.name}` | `{b_path.name}` |",
        f"| SHA-256 | `{a_bytes['sha256']}` | `{b_bytes['sha256']}` |",
        f"| Size (bytes) | {a_bytes['size_bytes']:,} | {b_bytes['size_bytes']:,} |",
        f"| Line endings | {a_bytes['line_endings']} | {b_bytes['line_endings']} |",
        f"| First data line | `{a_bytes['first_data_line']}` | `{b_bytes['first_data_line']}` |",
        f"| Last data line | `{a_bytes['last_data_line']}` | `{b_bytes['last_data_line']}` |",
        "",
        "| Record-level check | Result |",
        "|---|---|",
        *[f"| {label} | {'yes' if ok else 'NO'} |" for label, ok in rows],
        "",
        ("**Conclusion:** the files are different bytes but the same logical records. "
         "Copy B was re-saved by a spreadsheet program (dates became DD/MM/YYYY, hours lost the "
         "leading zero, whole numbers lost the trailing `.0`). The pipeline uses copy A and its "
         "checksum is the one recorded in `metadata/source_manifest.yml`.")
        if all(ok for _, ok in rows) else
        f"**Conclusion:** the copies differ in content (columns: {', '.join(mismatched) or 'see table'}).",
        "",
    ]
    text = "\n".join(report)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
