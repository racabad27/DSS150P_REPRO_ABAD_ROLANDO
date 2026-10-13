"""Stage registry shared by the CLI and the Airflow DAG.

Each stage is a small function that calls the real logic in its module and
reports row counts. ``run_stage`` wraps it with logging, timing and the run
record, so a stage behaves the same whether it is started from the command
line or by Airflow.
"""

from __future__ import annotations

from typing import Any, Callable

from src.config import Settings
from src.errors import ConfigError
from src.ingest import ingest
from src.observability import StageLogger, StageResult, track_stage

StageFunction = Callable[..., dict]
STAGES: dict[str, StageFunction] = {}

# Order used by `run-all` (and mirrored by the Airflow DAG).
PIPELINE_ORDER = [
    "ingest_source",
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


def run_stage(name: str, settings: Settings, run_id: str, **params: Any) -> dict:
    """Run one registered stage with logging, timing and run-record bookkeeping."""
    if name not in STAGES:
        raise ConfigError(f"unknown stage {name!r}; known stages: {', '.join(STAGES)}")
    with track_stage(settings, run_id, name) as (log, result):
        return STAGES[name](settings, run_id, log, result, **params)
