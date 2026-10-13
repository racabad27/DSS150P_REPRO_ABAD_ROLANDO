"""Stage registry shared by the CLI and the Airflow DAG.

Each stage is a small function that calls the real logic in its module and
reports row counts. ``run_stage`` wraps it with logging, timing and the run
record, so a stage behaves the same whether it is started from the command
line or by Airflow. ``run_all`` runs the stages in order for one run id.
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import pyarrow.parquet as pq

from src.config import Settings
from src.contract import load_contract
from src.errors import ConfigError, DataQualityError
from src.ingest import Checkpoint, ingest, read_raw
from src.observability import RunRecord, StageLogger, StageResult, track_stage
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
    "validate_curated",
]


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


@stage("validate_curated")
def validate_curated(settings: Settings, run_id: str, log: StageLogger, result: StageResult, **_: Any) -> dict:
    """Re-check the PUBLISHED curated file (what downstream will actually read)."""
    if not curated_path(settings).is_file():
        raise DataQualityError("No curated data. Run `python -m src.cli build-curated` first.")
    curated = pd.read_parquet(curated_path(settings))
    staging_rows = pq.ParquetFile(staging_path(settings)).metadata.num_rows
    excluded = len(pd.read_csv(settings.curated_dir / EXCLUSIONS_FILE))
    report = validate_curated_frame(curated, settings, run_id, staging_rows=staging_rows, excluded_rows=excluded)
    report.layer = "curated_published"
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()
    result.counts["curated_rows"] = int(len(curated))
    result.details["quality"] = report.summary()
    return {"curated_rows": int(len(curated)), "quality": report.summary()}


def run_stage(name: str, settings: Settings, run_id: str, **params: Any) -> dict:
    """Run one registered stage with logging, timing and run-record bookkeeping."""
    if name not in STAGES:
        raise ConfigError(f"unknown stage {name!r}; known stages: {', '.join(STAGES)}")
    with track_stage(settings, run_id, name) as (log, result):
        return STAGES[name](settings, run_id, log, result, **params)


def run_all(settings: Settings, run_id: str, mode: str = "incremental", max_batches: int | None = None,
            fail_after_batches: int | None = None) -> dict:
    """Run every stage in order under one run id; stop early on a partial ingestion."""
    record = RunRecord(settings, run_id)
    try:
        run_stage("ingest_source", settings, run_id, mode=mode, max_batches=max_batches,
                  fail_after_batches=fail_after_batches)
        raw_check = run_stage("validate_raw", settings, run_id)
        if not raw_check["complete"]:
            return record.set_status("partial")
        for name in PIPELINE_ORDER[2:]:
            run_stage(name, settings, run_id)
    except BaseException:
        record.set_status("failed")
        raise
    return record.set_status("success")
