"""Stage registry shared by the CLI and the Airflow DAG.

Each stage is a small function that calls the real logic in its module and
reports row counts. ``run_stage`` wraps it with logging, timing and the run
record, so a stage behaves the same whether it is started from the command
line or by Airflow. ``run_all`` runs the stages in order for one run id.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

from src.benchmark import partition_demo, run_benchmark
from src.config import Settings
from src.contract import load_contract
from src.errors import ConfigError, DataQualityError
from src.ingest import Checkpoint, ingest, read_raw
from src.observability import RunRecord, StageLogger, StageResult, setup_logging, track_stage
from src.storage import (
    CURATED_TABLE, STAGING_TABLE, connect, frame_fingerprint, load_postgres, scalar, sync_audit,
    table_fingerprint,
)
from src.transform import (
    EXCLUSIONS_FILE, build_curated, build_staging, curated_path, staging_path,
)
from src.validate import validate_curated_frame, validate_raw_frame

StageFunction = Callable[..., dict]
STAGES: dict[str, StageFunction] = {}

# Order used by `run-all` (and mirrored by the Airflow DAG).
PIPELINE_ORDER = [
    "ingest_source",
    "validate_raw",
    "transform_staging",
    "build_curated",
    "load_postgres",
    "validate_curated",
]
DATABASE_STAGES = {"load_postgres"}


def stage(name: str) -> Callable[[StageFunction], StageFunction]:
    """Decorator that registers a stage function under its task name."""

    def register(function: StageFunction) -> StageFunction:
        STAGES[name] = function
        return function

    return register


@stage("ingest_source")
def ingest_source(settings: Settings, run_id: str, log: StageLogger, result: StageResult,
                  mode: str = "incremental", max_batches: int | None = None,
                  fail_after_batches: int | None = None, **_: Any) -> dict:
    summary = ingest(settings, run_id, log, mode=mode, max_batches=max_batches,
                     fail_after_batches=fail_after_batches)
    result.counts.update({
        "source_rows": summary["source_rows"],
        "raw_rows": summary["raw_rows_total"],
        "raw_batches_committed": summary["batches_committed_total"],
    })
    result.details.update({k: v for k, v in summary.items() if k != "batches"})
    if summary["coverage"] < 1:
        result.status = "partial"
    return summary


@stage("validate_raw")
def validate_raw(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    """Contract checks on the raw layer. Returns complete=False for a partial ingestion."""
    contract = load_contract(settings)
    state = Checkpoint(settings.checkpoint_file).load()
    raw = read_raw(settings)
    committed_rows = sum(int(b["rows"]) for b in state["committed_batches"].values())
    report = validate_raw_frame(raw, contract, settings, run_id, committed_rows)
    total_batches = state.get("batches_in_source") or 0
    complete = bool(total_batches) and len(state["committed_batches"]) == total_batches
    report.check("all_batches_committed", "completeness",
                 "Every arrival batch of the source has been ingested (downstream needs full coverage)",
                 complete, observed=len(state["committed_batches"]), expected=total_batches, blocking=False)
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()
    result.counts["raw_rows"] = int(len(raw))
    result.details.update({"complete": complete, "quality": report.summary()})
    if not complete:
        result.status = "partial"
        log.event("partial_ingestion", f"Only {len(state['committed_batches'])} of {total_batches} batches "
                  "are committed; downstream stages wait for the remaining batches")
    return {"complete": complete, "raw_rows": int(len(raw)), "quality": report.summary()}


@stage("transform_staging")
def transform_staging(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    summary = build_staging(settings, run_id, log)
    result.counts.update({"staging_rows": summary["staging_rows"],
                          "quarantined_rows": summary["quarantined_rows"]})
    result.details.update(summary)
    return summary


@stage("build_curated")
def build_curated_stage(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    summary = build_curated(settings, run_id, log)
    result.counts.update({"curated_rows": summary["curated_rows"],
                          "curated_excluded_rows": summary["curated_excluded_rows"]})
    result.details.update(summary)
    return summary


@stage("load_postgres")
def load_postgres_stage(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    summary = load_postgres(settings, run_id, log)
    for table in summary["tables"]:
        result.details[table["table"]] = {k: v for k, v in table.items() if k != "table"}
    result.details["audit"] = summary["audit"]
    return summary


@stage("validate_curated")
def validate_curated(settings: Settings, run_id: str, log: StageLogger, result: StageResult,
                     check_database: bool = True, **_: Any) -> dict:
    """Re-check the PUBLISHED curated file and reconcile it with PostgreSQL."""
    if not curated_path(settings).is_file():
        raise DataQualityError("No curated data. Run `python -m src.cli build-curated` first.")
    curated = pd.read_parquet(curated_path(settings))
    staging = pd.read_parquet(staging_path(settings), columns=["source_row_key", "reading_ts", "row_hash"])
    excluded = len(pd.read_csv(settings.curated_dir / EXCLUSIONS_FILE))
    report = validate_curated_frame(curated, settings, run_id, staging_rows=len(staging), excluded_rows=excluded)
    report.layer = "curated_published"
    if check_database:
        with connect(settings, log) as conn:
            for table, frame in ((CURATED_TABLE, curated), (STAGING_TABLE, staging)):
                in_db, in_file = table_fingerprint(conn, settings, table), frame_fingerprint(frame)
                name = table.split(".")[0]
                report.check(f"db_{name}_row_count", "consistency", f"{table} has the same row count as the file",
                             in_db["row_count"] == in_file["row_count"],
                             observed=in_db["row_count"], expected=in_file["row_count"])
                report.check(f"db_{name}_no_duplicate_keys", "uniqueness",
                             f"{table} has no duplicate reading_ts",
                             in_db["row_count"] == in_db["distinct_reading_ts"],
                             observed=in_db["row_count"] - in_db["distinct_reading_ts"], expected=0)
                report.check(f"db_{name}_content_matches_file", "consistency",
                             f"{table} content digest equals the Parquet file digest",
                             in_db["row_hash_digest"] == in_file["row_hash_digest"],
                             observed=in_db["row_hash_digest"][:16], expected=in_file["row_hash_digest"][:16])
            missing = scalar(conn, settings, "curated_missing_in_staging")
            report.check("db_referential_curated_in_staging", "consistency",
                         "Every curated reading exists in staging (referential rule)",
                         missing == 0, observed=missing, expected=0, failing_rows=missing)
            mismatches = scalar(conn, settings, "curated_feature_mismatches")
            report.check("db_sql_recomputed_features", "consistency",
                         "Derived features recomputed in SQL equal the stored values",
                         mismatches == 0, observed=mismatches, expected=0, failing_rows=mismatches)
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()
    result.counts["curated_rows"] = int(len(curated))
    result.details["quality"] = report.summary()
    return {"curated_rows": int(len(curated)), "database_checked": check_database, "quality": report.summary()}


@stage("benchmark_storage")
def benchmark_storage(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    """Optional experiment (not part of run-all): CSV vs JSONL vs Parquet vs PostgreSQL."""
    report = run_benchmark(settings, run_id, log)
    result.details["formats"] = {r["format"]: {"size_bytes": r["size_bytes"], "read_s_median": r["read_s_median"]}
                                 for r in report["results"]}
    return report


@stage("partition_demo")
def partition_demo_stage(settings: Settings, run_id: str, log: StageLogger, result: StageResult,
                         year: int | None = None, month: int | None = None,
                         check_database: bool = True, **_: Any) -> dict:
    """Optional: read one partition only and verify its row count."""
    summary = partition_demo(settings, run_id, log, year=year, month=month, check_database=check_database)
    result.details.update(summary)
    return summary


def run_stage(name: str, settings: Settings, run_id: str, **params: Any) -> dict:
    """Run one registered stage with logging, timing and run-record bookkeeping."""
    if name not in STAGES:
        raise ConfigError(f"unknown stage {name!r}; known stages: {', '.join(STAGES)}")
    with track_stage(settings, run_id, name) as (log, result):
        return STAGES[name](settings, run_id, log, result, **params)


def _finish_run(settings: Settings, run_id: str, status: str, use_database: bool) -> dict:
    """Store the final run status and copy the run history into PostgreSQL (best effort)."""
    record = RunRecord(settings, run_id).set_status(status)
    if use_database:
        log = setup_logging(settings, run_id, "finish_run")
        try:
            with connect(settings, log, attempts=1) as conn:
                sync_audit(conn, settings)
        except Exception as exc:  # noqa: BLE001 - the run result must not depend on this
            log.event("audit_sync_skipped", f"Run record not synced to PostgreSQL: {exc}", level=logging.WARNING)
    return record


def run_all(settings: Settings, run_id: str, mode: str = "incremental", max_batches: int | None = None,
            fail_after_batches: int | None = None, skip_db: bool = False) -> dict:
    """Run every stage in order under one run id; stop early on a partial ingestion."""
    try:
        run_stage("ingest_source", settings, run_id, mode=mode, max_batches=max_batches,
                  fail_after_batches=fail_after_batches)
        raw_check = run_stage("validate_raw", settings, run_id)
        if not raw_check["complete"]:
            return _finish_run(settings, run_id, "partial", not skip_db)
        for name in PIPELINE_ORDER[2:]:
            if skip_db and name in DATABASE_STAGES:
                continue
            params = {"check_database": False} if skip_db and name == "validate_curated" else {}
            run_stage(name, settings, run_id, **params)
    except BaseException:
        _finish_run(settings, run_id, "failed", not skip_db)
        raise
    return _finish_run(settings, run_id, "success", not skip_db)
