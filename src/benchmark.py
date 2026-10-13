"""Storage benchmark: the SAME curated records as CSV, JSONL, Parquet and PostgreSQL.

``python -m src.cli benchmark`` writes the curated business view (98,000 rows,
business + feature columns; pipeline audit columns left out) in every format
and measures:

* serialized size;
* write time (median of ``write_repeats``);
* read time: one first read reported separately, then ``read_repeats`` timed
  reads whose MEDIAN is reported. Each reader returns a typed DataFrame
  (timestamps parsed) and is checked to contain the same logical records;
* the same selective filter (Factory_B since 2023-01-01) in each format and,
  for PostgreSQL, a full-table query, the selective query (index) and an
  aggregate query.

``python -m src.cli partition-demo`` reads ONE month from data/partitioned/
and checks its row count against the full dataset and PostgreSQL.
Results go to outputs/benchmark/*.json and the generated part of
docs/benchmark_report.md.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pyarrow
import pyarrow.dataset as pads
from psycopg import sql

from src.config import Settings
from src.errors import DataQualityError
from src.observability import StageLogger, utc_now_iso
from src.storage import CURATED_TABLE, connect, frame_to_csv
from src.transform import curated_path, partitioned_path
from src.validate import DERIVED_FEATURES, STAGING_BUSINESS_COLUMNS

BENCH_COLUMNS = ["source_row_key", "reading_year", "reading_month"] + STAGING_BUSINESS_COLUMNS + DERIVED_FEATURES
BEGIN_MARK = "<!-- BEGIN GENERATED BENCHMARK (python -m src.cli benchmark) -->"
END_MARK = "<!-- END GENERATED BENCHMARK -->"


def _timed(function: Callable[[], Any], repeats: int) -> tuple[list[float], Any]:
    times, result = [], None
    for _ in range(repeats):
        started = time.perf_counter()
        result = function()
        times.append(time.perf_counter() - started)
    return times, result


def _logical_signature(frame: pd.DataFrame) -> dict[str, Any]:
    """What 'same logical records' means here: same keys, same row count, same target sum."""
    return {
        "rows": int(len(frame)),
        "columns": int(frame.shape[1]),
        "source_row_key_sum": int(frame["source_row_key"].sum()),
        "risk_sum": round(float(frame["risk"].sum()), 6),
        "first_reading_ts": str(pd.to_datetime(frame["reading_ts"]).min()),
    }


def _size(path: Path) -> int:
    return path.stat().st_size


def _bench_file(name: str, path: Path, write: Callable[[], None], read: Callable[[], pd.DataFrame],
                selective: Callable[[], pd.DataFrame], expected: dict, write_repeats: int,
                read_repeats: int, warmup: int) -> dict[str, Any]:
    write_times, _ = _timed(write, write_repeats)
    first_times, first = _timed(read, warmup)
    read_times, frame = _timed(read, read_repeats)
    selective_times, subset = _timed(selective, read_repeats)
    signature = _logical_signature(frame)
    return {
        "format": name,
        "path": path.name,
        "size_bytes": _size(path),
        "write_s_median": round(statistics.median(write_times), 4),
        "first_read_s": round(first_times[0], 4) if first_times else None,
        "read_s_median": round(statistics.median(read_times), 4),
        "read_s_min": round(min(read_times), 4),
        "read_s_max": round(max(read_times), 4),
        "selective_read_s_median": round(statistics.median(selective_times), 4),
        "selective_rows": int(len(subset)),
        "same_logical_records": signature == expected,
        "signature": signature,
    }


def run_benchmark(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    if not curated_path(settings).is_file():
        raise DataQualityError("No curated data yet. Run the pipeline before the benchmark.")
    curated = pd.read_parquet(curated_path(settings))[BENCH_COLUMNS]
    expected = _logical_signature(curated)
    write_repeats = int(settings.get("benchmark.write_repeats", 3))
    read_repeats = int(settings.get("benchmark.read_repeats", 5))
    warmup = int(settings.get("benchmark.warmup_runs", 1))
    flt = settings.get("benchmark.selective_filter")
    factory, start_ts = flt["factory"], pd.Timestamp(flt["start_ts"])
    files = settings.benchmark_dir / "files"
    files.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    # --- CSV ---------------------------------------------------------------
    csv_path = files / "fire_risk_curated.csv"
    def read_csv() -> pd.DataFrame:
        return pd.read_csv(csv_path, parse_dates=["reading_ts"])
    def select_csv() -> pd.DataFrame:
        frame = read_csv()          # CSV has no index or pushdown: read all, then filter
        return frame[(frame["factory"] == factory) & (frame["reading_ts"] >= start_ts)]
    results.append(_bench_file("CSV", csv_path, lambda: curated.to_csv(csv_path, index=False),
                               read_csv, select_csv, expected, write_repeats, read_repeats, warmup))

    # --- JSON Lines ----------------------------------------------------------
    jsonl_path = files / "fire_risk_curated.jsonl"
    def read_jsonl() -> pd.DataFrame:
        return pd.read_json(jsonl_path, lines=True, convert_dates=["reading_ts"])
    def select_jsonl() -> pd.DataFrame:
        frame = read_jsonl()
        return frame[(frame["factory"] == factory) & (frame["reading_ts"] >= start_ts)]
    results.append(_bench_file(
        "JSONL", jsonl_path,
        lambda: curated.to_json(jsonl_path, orient="records", lines=True, date_format="iso"),
        read_jsonl, select_jsonl, expected, write_repeats, read_repeats, warmup))

    # --- Parquet ---------------------------------------------------------------
    parquet_path = files / "fire_risk_curated.parquet"
    def read_parquet() -> pd.DataFrame:
        return pd.read_parquet(parquet_path)
    def select_parquet() -> pd.DataFrame:  # predicate pushdown skips row groups/pages
        return pd.read_parquet(parquet_path, filters=[("factory", "==", factory), ("reading_ts", ">=", start_ts)])
    results.append(_bench_file("Parquet (snappy)", parquet_path,
                               lambda: curated.to_parquet(parquet_path, index=False),
                               read_parquet, select_parquet, expected, write_repeats, read_repeats, warmup))

    # --- PostgreSQL --------------------------------------------------------------
    results.append(_bench_postgres(settings, curated, expected, factory, start_ts,
                                   write_repeats, read_repeats, warmup, log))

    for res in results:
        log.event("benchmark_format", f"{res['format']}: {res['size_bytes'] / 1e6:.2f} MB, "
                  f"write {res['write_s_median']}s, read median {res['read_s_median']}s",
                  same_logical_records=res["same_logical_records"])
    if not all(r["same_logical_records"] for r in results):
        raise DataQualityError("A storage format did not return the same logical records")

    report = {
        "pipeline_run_id": run_id,
        "generated_at_utc": utc_now_iso(),
        "dataset": {"name": "curated business view", "rows": expected["rows"], "columns": len(BENCH_COLUMNS),
                    "note": "pipeline audit columns (hashes, run ids) are not part of the benchmark copy"},
        "method": {"write_repeats": write_repeats, "read_repeats": read_repeats,
                   "first_read_excluded_from_median": True,
                   "first_read_note": "the first read follows the write, so the OS page cache is already warm; "
                                      "it is reported separately, not as a true cold-cache read",
                   "selective_filter": f"factory = '{factory}' AND reading_ts >= '{start_ts}'"},
        "machine": machine_info(settings),
        "results": results,
    }
    settings.benchmark_dir.mkdir(parents=True, exist_ok=True)
    (settings.benchmark_dir / "benchmark_results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_report_markdown(settings)
    return report


def _bench_postgres(settings: Settings, curated: pd.DataFrame, expected: dict, factory: str,
                    start_ts: pd.Timestamp, write_repeats: int, read_repeats: int, warmup: int,
                    log: StageLogger) -> dict[str, Any]:
    schema_name, table_name = settings.get("benchmark.postgres_table").split(".")
    table = sql.Identifier(schema_name, table_name)
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in BENCH_COLUMNS)
    csv_text = frame_to_csv(curated)
    with connect(settings, log) as conn:
        conn.autocommit = True
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))

        def write() -> None:
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
            # same column types as the curated table, no constraints
            conn.execute(sql.SQL("CREATE TABLE {} AS SELECT {} FROM {} WITH NO DATA").format(
                table, columns, sql.Identifier(*CURATED_TABLE.split("."))))
            with conn.cursor() as cur:
                with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(table, columns)) as copy:
                    copy.write(csv_text)
            conn.execute(sql.SQL("CREATE INDEX ON {} (factory, reading_ts)").format(table))
            conn.execute(sql.SQL("ANALYZE {}").format(table))

        def fetch(query: sql.Composable, params: tuple = ()) -> pd.DataFrame:
            with conn.cursor() as cur:
                cur.execute(query, params)
                names = [d.name for d in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=names)

        full_query = sql.SQL("SELECT {} FROM {} ORDER BY source_row_key").format(columns, table)
        selective_query = sql.SQL("SELECT {} FROM {} WHERE factory = %s AND reading_ts >= %s").format(columns, table)
        aggregate_query = sql.SQL("SELECT factory, reading_year, reading_month, count(*) AS readings, "
                                  "avg(risk) AS avg_risk FROM {} GROUP BY 1, 2, 3").format(table)

        write_times, _ = _timed(write, write_repeats)
        first_times, _ = _timed(lambda: fetch(full_query), warmup)
        read_times, frame = _timed(lambda: fetch(full_query), read_repeats)
        sel_times, subset = _timed(lambda: fetch(selective_query, (factory, start_ts.to_pydatetime())), read_repeats)
        agg_times, aggregate = _timed(lambda: fetch(aggregate_query), read_repeats)
        size = conn.execute(sql.SQL("SELECT pg_total_relation_size(%s)"), (f"{schema_name}.{table_name}",)).fetchone()[0]
        heap = conn.execute(sql.SQL("SELECT pg_relation_size(%s)"), (f"{schema_name}.{table_name}",)).fetchone()[0]
        plan = "\n".join(r[0] for r in conn.execute(
            sql.SQL("EXPLAIN SELECT * FROM {} WHERE factory = %s AND reading_ts >= %s").format(table),
            (factory, start_ts.to_pydatetime())).fetchall())
        version = conn.execute("SHOW server_version").fetchone()[0]

    return {
        "format": "PostgreSQL 16 table",
        "path": f"{schema_name}.{table_name}",
        "size_bytes": int(size),
        "table_bytes_without_indexes": int(heap),
        "write_s_median": round(statistics.median(write_times), 4),
        "write_note": "CREATE TABLE + COPY + CREATE INDEX + ANALYZE",
        "first_read_s": round(first_times[0], 4) if first_times else None,
        "read_s_median": round(statistics.median(read_times), 4),
        "read_s_min": round(min(read_times), 4),
        "read_s_max": round(max(read_times), 4),
        "selective_read_s_median": round(statistics.median(sel_times), 4),
        "selective_rows": int(len(subset)),
        "aggregate_query_s_median": round(statistics.median(agg_times), 4),
        "aggregate_groups": int(len(aggregate)),
        "selective_query_plan": plan,
        "server_version": version,
        "same_logical_records": _logical_signature(frame) == expected,
        "signature": _logical_signature(frame),
    }


def machine_info(settings: Settings) -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "pandas": pd.__version__,
        "pyarrow": pyarrow.__version__,
        "inside_docker": Path("/.dockerenv").exists(),
    }


# ---------------------------------------------------------------------------
# partition demo
# ---------------------------------------------------------------------------
def partition_demo(settings: Settings, run_id: str, log: StageLogger, year: int | None = None,
                   month: int | None = None, check_database: bool = True) -> dict[str, Any]:
    """Load one partition only and verify its row count three ways."""
    demo = settings.get("benchmark.partition_demo")
    year, month = int(year or demo["reading_year"]), int(month or demo["reading_month"])
    base = partitioned_path(settings)
    if not base.is_dir():
        raise DataQualityError("No partitioned data. Run `python -m src.cli build-curated` first.")
    dataset = pads.dataset(base, format="parquet", partitioning="hive")
    flt = (pads.field("reading_year") == year) & (pads.field("reading_month") == month)

    repeats = int(settings.get("benchmark.read_repeats", 5))
    fragments = list(dataset.get_fragments(filter=flt))
    partition_times, table = _timed(lambda: dataset.to_table(filter=flt), repeats)

    def full_scan() -> pd.DataFrame:  # without partitions: read every row, then filter
        frame = pd.read_parquet(curated_path(settings))
        return frame[(frame["reading_year"] == year) & (frame["reading_month"] == month)]

    full_times, subset = _timed(full_scan, repeats)
    expected = int(len(subset))

    result = {
        "partition": f"reading_year={year}/reading_month={month}",
        "files_read": [Path(f.path).relative_to(settings.data_dir).as_posix() for f in fragments],
        "files_in_dataset": len(list(dataset.get_fragments())),
        "rows_from_partition": int(table.num_rows),
        "rows_expected_from_full_curated": expected,
        "partition_read_s_median": round(statistics.median(partition_times), 4),
        "full_file_scan_s_median": round(statistics.median(full_times), 4),
        "timing_repeats": repeats,
    }
    if check_database:
        with connect(settings, log) as conn:
            result["rows_in_postgres"] = int(conn.execute(
                "SELECT count(*) FROM curated.fire_risk_features WHERE reading_year = %s AND reading_month = %s",
                (year, month)).fetchone()[0])
    counts = {result["rows_from_partition"], expected} | ({result["rows_in_postgres"]} if check_database else set())
    result["row_counts_agree"] = len(counts) == 1
    log.event("partition_demo", f"Partition {result['partition']}: {result['rows_from_partition']:,} rows "
              f"from {len(fragments)} file(s); counts agree: {result['row_counts_agree']}")
    settings.benchmark_dir.mkdir(parents=True, exist_ok=True)
    (settings.benchmark_dir / "partition_demo.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    if not result["row_counts_agree"]:
        raise DataQualityError(f"Partition row counts disagree: {result}")
    write_report_markdown(settings)
    return result


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
def _mb(n: int) -> str:
    return f"{n / 1_000_000:.2f}"


def render_generated(settings: Settings) -> str:
    bench_file = settings.benchmark_dir / "benchmark_results.json"
    part_file = settings.benchmark_dir / "partition_demo.json"
    lines: list[str] = []
    if bench_file.is_file():
        b = json.loads(bench_file.read_text(encoding="utf-8"))
        m = b["machine"]
        lines += [
            f"Run `{b['pipeline_run_id']}` at {b['generated_at_utc']} on {m['platform']} "
            f"(Python {m['python']}, {m['cpu_count']} CPU, pandas {m['pandas']}, pyarrow {m['pyarrow']}, "
            f"inside Docker: {'yes' if m['inside_docker'] else 'no'}).",
            "",
            f"Dataset: {b['dataset']['rows']:,} rows x {b['dataset']['columns']} columns. "
            f"Writes: median of {b['method']['write_repeats']}. Reads: median of "
            f"{b['method']['read_repeats']} (first read excluded and shown separately). "
            f"Selective filter: `{b['method']['selective_filter']}`.",
            "",
            "| Format | Size (MB) | Write (s) | First read (s) | Read median (s) | Read min–max (s) | Selective read median (s) | Selective rows | Same records |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for r in b["results"]:
            lines.append(f"| {r['format']} | {_mb(r['size_bytes'])} | {r['write_s_median']} | {r['first_read_s']} | "
                         f"{r['read_s_median']} | {r['read_s_min']}–{r['read_s_max']} | "
                         f"{r['selective_read_s_median']} | {r['selective_rows']:,} | "
                         f"{'yes' if r['same_logical_records'] else 'NO'} |")
        pg = next(r for r in b["results"] if r["format"].startswith("PostgreSQL"))
        lines += [
            "",
            f"PostgreSQL {pg['server_version']}: table without indexes {_mb(pg['table_bytes_without_indexes'])} MB "
            f"(total with index {_mb(pg['size_bytes'])} MB). Full-table query median {pg['read_s_median']} s, "
            f"selective query (index on factory, reading_ts) median {pg['selective_read_s_median']} s, "
            f"aggregate query ({pg['aggregate_groups']} groups) median {pg['aggregate_query_s_median']} s.",
            "",
            "Plan of the selective query:",
            "",
            "```text",
            pg["selective_query_plan"],
            "```",
        ]
    if part_file.is_file():
        p = json.loads(part_file.read_text(encoding="utf-8"))
        lines += [
            "",
            f"**Partition demo** — `{p['partition']}`: read {len(p['files_read'])} of {p['files_in_dataset']} files "
            f"({', '.join(p['files_read'])}), **{p['rows_from_partition']:,} rows**; expected from the full curated "
            f"file: {p['rows_expected_from_full_curated']:,}"
            + (f"; in PostgreSQL: {p['rows_in_postgres']:,}" if "rows_in_postgres" in p else "")
            + f". Counts agree: {'yes' if p['row_counts_agree'] else 'NO'}. Median of {p['timing_repeats']} reads: "
              f"partition only {p['partition_read_s_median']} s vs reading the whole curated file and filtering "
              f"{p['full_file_scan_s_median']} s.",
        ]
    return "\n".join(lines) if lines else "(run `python -m src.cli benchmark` to fill this section)"


def write_report_markdown(settings: Settings) -> Path:
    path = settings.docs_dir / "benchmark_report.md"
    generated = f"{BEGIN_MARK}\n{render_generated(settings)}\n{END_MARK}"
    if path.is_file() and BEGIN_MARK in path.read_text(encoding="utf-8"):
        text = path.read_text(encoding="utf-8")
        before, rest = text.split(BEGIN_MARK, 1)
        after = rest.split(END_MARK, 1)[1] if END_MARK in rest else "\n"
        path.write_text(before + generated + after, encoding="utf-8")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Storage benchmark report\n\n## Measurements\n\n" + generated +
                        "\n\n## Interpretation\n\n(write notes here)\n", encoding="utf-8")
    return path
