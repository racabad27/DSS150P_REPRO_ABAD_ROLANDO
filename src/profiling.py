"""Profile the source CSV (Phase I: understand and control the source).

``python -m src.cli profile`` reads the immutable source file and writes

* ``metadata/source_profile.json`` - every number, machine-readable;
* the generated part of ``metadata/source_profile.md`` - the same numbers as
  tables. Text outside the GENERATED markers is my own interpretation and is
  kept when the profile is regenerated.

The profile is deterministic (no timestamps, rounded floats), so re-running it
on the same file produces byte-identical output.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import Settings

BEGIN_MARK = "<!-- BEGIN GENERATED PROFILE (python -m src.cli profile) -->"
END_MARK = "<!-- END GENERATED PROFILE -->"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    """SHA-256 of a file, read in chunks so large files do not fill memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _round(value: Any, digits: int = 6) -> Any:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return round(float(value), digits)
    return value


def file_facts(path: Path, root: Path) -> dict[str, Any]:
    """Format-level facts: size, checksum, encoding, line endings, delimiter."""
    data = path.read_bytes()
    try:
        data.decode("utf-8")
        utf8 = True
    except UnicodeDecodeError:
        utf8 = False
    crlf = data.count(b"\r\n")
    lf_only = data.count(b"\n") - crlf
    header = data.split(b"\n", 1)[0].decode("utf-8", errors="replace").rstrip("\r")
    try:
        location = str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        location = path.name
    return {
        "file_name": path.name,
        "location": location,
        "format": "CSV",
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "encoding": "UTF-8" if utf8 else "not UTF-8",
        "ascii_only": data.isascii(),
        "byte_order_mark": data.startswith(b"\xef\xbb\xbf"),
        "line_endings": "CRLF" if crlf and not lf_only else ("LF" if lf_only and not crlf else "mixed"),
        "delimiter": ",",
        "quote_characters_found": data.count(b'"'),
        "header": header.split(","),
    }


def profile_columns(text: pd.DataFrame, typed: pd.DataFrame) -> list[dict[str, Any]]:
    """Per-column profile: inferred type, nulls, distinct values, ranges."""
    rows = []
    n = len(text)
    for column in text.columns:
        raw = text[column]
        empty = int((raw.str.strip() == "").sum())
        series = typed[column]
        entry: dict[str, Any] = {
            "column": column,
            "inferred_type": str(series.dtype),
            "null_count": empty,
            "null_rate": _round(empty / n if n else 0.0),
            "distinct_count": int(raw[raw.str.strip() != ""].nunique()),
            "leading_or_trailing_spaces": int((raw != raw.str.strip()).sum()),
        }
        if pd.api.types.is_numeric_dtype(series):
            values = series.dropna()
            integral = bool((values == values.round()).all()) if len(values) else False
            entry.update({
                "logical_type": "integer" if integral else "float",
                "min": _round(values.min()),
                "max": _round(values.max()),
                "mean": _round(values.mean()),
                "std": _round(values.std()),
                "p25": _round(values.quantile(0.25)),
                "median": _round(values.median()),
                "p75": _round(values.quantile(0.75)),
            })
            if entry["distinct_count"] > 2:  # outlier tests make no sense for 0/1 flags
                q1, q3 = values.quantile(0.25), values.quantile(0.75)
                iqr = q3 - q1
                entry["iqr_outliers"] = int(((values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)).sum())
                z = (values - values.mean()) / values.std()
                entry["zscore_outliers"] = int((z.abs() > 3).sum())
            else:
                entry["iqr_outliers"] = entry["zscore_outliers"] = None
        else:
            counts = raw.value_counts().sort_index()
            entry["logical_type"] = "category" if entry["distinct_count"] <= 20 else "string"
            if entry["distinct_count"] <= 20:
                entry["values"] = {str(k): int(v) for k, v in counts.items()}
            else:
                entry["min"] = str(raw.min())
                entry["max"] = str(raw.max())
        rows.append(entry)
    return rows


def profile_source(settings: Settings) -> dict[str, Any]:
    """Build the full profile dictionary for the configured source file."""
    path = settings.source_file
    facts = file_facts(path, settings.root)
    text = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    typed = pd.read_csv(path)
    n_rows = len(text)

    reading_ts = pd.to_datetime(text["Date"] + " " + text["Time"], format=TIMESTAMP_FORMAT, errors="coerce")
    diffs = reading_ts.diff().dropna()
    per_day = reading_ts.dt.date.value_counts()
    workers_missing = typed["Workers"].isna()
    risk = typed["Risk"]
    accident = typed["Accident"]

    bins = list(range(0, 101, 10))
    labels = [f"{lo}-{lo + 10}" for lo in bins[:-1]]
    risk_hist = pd.cut(risk, bins=bins, labels=labels, include_lowest=True).value_counts().sort_index()
    above, below, at = risk > 75, risk < 75, risk == 75

    profile = {
        "file": facts,
        "shape": {"rows": n_rows, "columns": text.shape[1]},
        "duplicates": {
            "full_row_duplicates": int(text.duplicated().sum()),
            "business_key": ["Date", "Time"],
            "business_key_duplicates": int(text.duplicated(subset=["Date", "Time"]).sum()),
        },
        "candidate_keys": {
            "Date+Time": bool(not text.duplicated(subset=["Date", "Time"]).any()),
            "any_single_column": [c for c in text.columns if text[c].is_unique],
        },
        "timestamps": {
            "format": TIMESTAMP_FORMAT,
            "unparseable": int(reading_ts.isna().sum()),
            "first": str(reading_ts.min()),
            "last": str(reading_ts.max()),
            "strictly_increasing": bool(reading_ts.is_monotonic_increasing and reading_ts.is_unique),
            "cadence_minutes": _round(diffs.mode().iloc[0].total_seconds() / 60 if len(diffs) else None),
            "gaps_not_15_min": int((diffs != pd.Timedelta(minutes=15)).sum()),
            "days_covered": int(per_day.size),
            "readings_per_full_day": int(per_day.mode().iloc[0]) if len(per_day) else None,
            "days_with_fewer_readings": {str(k): int(v) for k, v in per_day[per_day < 96].sort_index().items()},
            "rows_per_year": {str(k): int(v) for k, v in reading_ts.dt.year.value_counts().sort_index().items()},
        },
        "columns": profile_columns(text, typed),
        "target": {
            "column": "Risk",
            "range_observed": [_round(risk.min()), _round(risk.max())],
            "mean": _round(risk.mean()),
            "std": _round(risk.std()),
            "histogram_0_100_step_10": {str(k): int(v) for k, v in risk_hist.items()},
            "values_at_bounds": {"0": int((risk == 0).sum()), "100": int((risk == 100).sum())},
        },
        "accident": {
            "positive_rate": _round(accident.mean()),
            "positives": int(accident.sum()),
            "vs_risk_threshold_75": {
                "risk_above_75": {"accident_0": int((above & (accident == 0)).sum()), "accident_1": int((above & (accident == 1)).sum())},
                "risk_equal_75": {"accident_0": int((at & (accident == 0)).sum()), "accident_1": int((at & (accident == 1)).sum())},
                "risk_below_75": {"accident_0": int((below & (accident == 0)).sum()), "accident_1": int((below & (accident == 1)).sum())},
            },
        },
        "workers_missing": {
            "rows": int(workers_missing.sum()),
            "rate": _round(workers_missing.mean()),
            "by_year": {str(k): int(v) for k, v in workers_missing.groupby(reading_ts.dt.year).sum().items()},
            "accidents_among_missing": int(accident[workers_missing].sum()),
        },
    }
    return profile


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------
def _table(headers: list[str], rows: list[list[Any]]) -> str:
    def cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, float):
            return f"{value:,.4f}".rstrip("0").rstrip(".") if abs(value) < 1e6 else f"{value:,.0f}"
        if isinstance(value, int):
            return f"{value:,}"
        return str(value).replace("|", "\\|")

    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def render_markdown(profile: dict[str, Any]) -> str:
    """Generated section of source_profile.md (tables only, no interpretation)."""
    f, ts = profile["file"], profile["timestamps"]
    numeric = [c for c in profile["columns"] if "mean" in c]
    categorical = [c for c in profile["columns"] if "values" in c]
    other = [c for c in profile["columns"] if "mean" not in c and "values" not in c]
    acc = profile["accident"]["vs_risk_threshold_75"]
    parts = [
        "### File facts",
        _table(["Property", "Value"], [
            ["File", f["file_name"]], ["Location", f["location"]], ["Format", f["format"]],
            ["Size (bytes)", f["size_bytes"]], ["SHA-256", f"`{f['sha256']}`"],
            ["Encoding", f"{f['encoding']} (ASCII only: {'yes' if f['ascii_only'] else 'no'}, byte-order mark: {'yes' if f['byte_order_mark'] else 'no'})"],
            ["Line endings", f["line_endings"]], ["Delimiter", f"`{f['delimiter']}`"],
            ["Quote characters", f["quote_characters_found"]],
            ["Rows x columns", f"{profile['shape']['rows']:,} x {profile['shape']['columns']}"],
            ["Full-row duplicates", profile["duplicates"]["full_row_duplicates"]],
            ["Duplicate business keys (Date+Time)", profile["duplicates"]["business_key_duplicates"]],
            ["Candidate key", "Date + Time" if profile["candidate_keys"]["Date+Time"] else "none found"],
        ]),
        "",
        "### Timestamps",
        _table(["Property", "Value"], [
            ["Format", f"`{ts['format']}`"], ["Unparseable", ts["unparseable"]],
            ["First reading", ts["first"]], ["Last reading", ts["last"]],
            ["Strictly increasing and unique", ts["strictly_increasing"]],
            ["Cadence (minutes)", ts["cadence_minutes"]], ["Gaps other than 15 minutes", ts["gaps_not_15_min"]],
            ["Days covered", ts["days_covered"]], ["Readings per full day", ts["readings_per_full_day"]],
            ["Days with fewer readings", ", ".join(f"{d} ({n})" for d, n in ts["days_with_fewer_readings"].items()) or "none"],
            ["Rows per year", ", ".join(f"{y}: {n:,}" for y, n in ts["rows_per_year"].items())],
        ]),
        "",
        "### Numeric columns",
        _table(["Column", "Logical type", "Nulls", "Null rate", "Distinct", "Min", "Max", "Mean", "Std", "IQR outliers", "z>3 outliers"],
               [[c["column"], c["logical_type"], c["null_count"], c["null_rate"], c["distinct_count"], c["min"], c["max"],
                 c["mean"], c["std"], c["iqr_outliers"], c["zscore_outliers"]] for c in numeric]),
        "",
        "### Categorical and text columns",
        _table(["Column", "Logical type", "Nulls", "Distinct", "Values (count)"],
               [[c["column"], c["logical_type"], c["null_count"], c["distinct_count"],
                 ", ".join(f"{k} ({v:,})" for k, v in c.get("values", {}).items()) or f"{c.get('min')} … {c.get('max')}"]
                for c in categorical + other]),
        "",
        "### Target: Risk",
        _table(["Range observed", "Mean", "Std", "Rows at 0", "Rows at 100"],
               [[f"{profile['target']['range_observed'][0]} – {profile['target']['range_observed'][1]}",
                 profile["target"]["mean"], profile["target"]["std"],
                 profile["target"]["values_at_bounds"]["0"], profile["target"]["values_at_bounds"]["100"]]]),
        "",
        _table(["Risk bin", "Rows"], [[k, v] for k, v in profile["target"]["histogram_0_100_step_10"].items()]),
        "",
        "### Accident vs Risk (leakage check)",
        _table(["Risk", "Accident = 0", "Accident = 1"], [
            ["< 75", acc["risk_below_75"]["accident_0"], acc["risk_below_75"]["accident_1"]],
            ["= 75.00", acc["risk_equal_75"]["accident_0"], acc["risk_equal_75"]["accident_1"]],
            ["> 75", acc["risk_above_75"]["accident_0"], acc["risk_above_75"]["accident_1"]],
        ]),
        "",
        f"Accident positive rate: {profile['accident']['positive_rate']:.4%} "
        f"({profile['accident']['positives']:,} rows).",
        "",
        "### Missing Workers",
        _table(["Rows", "Rate", "By year", "Accidents among them"], [[
            profile["workers_missing"]["rows"], profile["workers_missing"]["rate"],
            ", ".join(f"{y}: {n}" for y, n in profile["workers_missing"]["by_year"].items()),
            profile["workers_missing"]["accidents_among_missing"]]]),
    ]
    return "\n".join(parts)


def write_profile(settings: Settings, profile: dict[str, Any]) -> tuple[Path, Path]:
    """Write the JSON profile and refresh the generated block of the Markdown file."""
    json_path = settings.file_setting("source.profile_json")
    md_path = settings.file_setting("source.profile_markdown")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8", newline="\n")

    generated = f"{BEGIN_MARK}\n{render_markdown(profile)}\n{END_MARK}"
    if md_path.is_file() and BEGIN_MARK in md_path.read_text(encoding="utf-8"):
        current = md_path.read_text(encoding="utf-8")
        before = current.split(BEGIN_MARK, 1)[0]
        after = current.split(END_MARK, 1)[1] if END_MARK in current else "\n"
        md_path.write_text(before + generated + after, encoding="utf-8", newline="\n")
    else:
        md_path.write_text(
            "# Source profile — industrial_fire_risk_data.csv\n\n"
            "## Generated profile\n\n" + generated + "\n\n## Interpretation\n\n(write notes here)\n",
            encoding="utf-8",
            newline="\n",
        )
    return json_path, md_path
