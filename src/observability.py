"""Structured logging, run identifiers and per-stage run records.

Every log line carries the ``run_id`` and the ``stage`` so all output of one
pipeline run (CLI or Airflow) can be traced together:

* the console shows a short human-readable line;
* ``logs/pipeline.jsonl`` receives one JSON object per event (timestamp,
  run id, stage, event name, row counts, duration, error details);
* ``outputs/runs/<run_id>.json`` is the run record. Each stage writes its
  start/end time, status, duration and row counts into it, which gives the
  pipeline summary (raw, staging, curated, train, test, prediction counts).
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from src.config import Settings

LOGGER_NAME = "pipeline"
_SAFE_RUN_ID = re.compile(r"[^A-Za-z0-9_.-]+")


# ---------------------------------------------------------------------------
# time and run id helpers
# ---------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """UTC timestamp such as 2026-10-13T08:15:02.123Z (used in audit columns)."""
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_run_id(prefix: str = "run") -> str:
    """Create a unique, sortable run id, e.g. run_20261013T081502Z_3fa9c1."""
    return f"{prefix}_{utc_now():%Y%m%dT%H%M%SZ}_{secrets.token_hex(3)}"


def normalise_run_id(value: str) -> str:
    """Make any run id (Airflow uses ':' and '+') safe for file names and SQL."""
    cleaned = _SAFE_RUN_ID.sub("-", value.strip()).strip("-")
    if not cleaned:
        raise ValueError("run id is empty after normalisation")
    return cleaned[:120]


def resolve_run_id(explicit: str | None = None) -> str:
    """Use --run-id, then $PIPELINE_RUN_ID, otherwise generate a new id."""
    candidate = explicit or os.environ.get("PIPELINE_RUN_ID")
    return normalise_run_id(candidate) if candidate else new_run_id()


# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    """One JSON object per line for logs/pipeline.jsonl."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "run_id": getattr(record, "run_id", None),
            "stage": getattr(record, "stage", None),
            "event": getattr(record, "event", None),
            "message": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}) or {})
        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["error"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "traceback": self.formatException(record.exc_info),
            }
        return json.dumps(payload, default=str)


class ConsoleFormatter(logging.Formatter):
    """Short readable line: UTC time | level | stage | message key=value ..."""

    def format(self, record: logging.LogRecord) -> str:
        stamp = datetime.fromtimestamp(record.created, timezone.utc).strftime("%H:%M:%SZ")
        stage = getattr(record, "stage", None) or "-"
        fields = getattr(record, "fields", {}) or {}
        extras = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
        line = f"{stamp} | {record.levelname:<7} | {stage:<21} | {record.getMessage()}"
        if extras:
            line += f"  [{extras}]"
        if record.exc_info and record.levelno >= logging.ERROR:
            exc_type, exc_value, _ = record.exc_info
            line += f"\n    -> {exc_type.__name__ if exc_type else 'Error'}: {exc_value}"
        return line


class StageLogger(logging.LoggerAdapter):
    """Logger that automatically adds run_id/stage and supports key=value fields."""

    def process(self, msg: Any, kwargs: dict) -> tuple[Any, dict]:
        extra = dict(self.extra)
        extra.update(kwargs.pop("extra", {}) or {})
        kwargs["extra"] = extra
        return msg, kwargs

    def event(self, event: str, message: str, level: int = logging.INFO,
              exc_info: Any = None, **fields: Any) -> None:
        """Log a named event with structured fields (row counts, durations...)."""
        self.log(level, message, exc_info=exc_info, extra={"event": event, "fields": fields})


def setup_logging(settings: Settings, run_id: str, stage: str | None = None) -> StageLogger:
    """Configure console + JSON-file logging once per process and return a logger."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Airflow has its own root handlers; avoid double lines

    console_level = str(settings.get("logging.level", "INFO")).upper()
    if not any(getattr(h, "_pipeline_console", False) for h in logger.handlers):
        console = logging.StreamHandler(sys.stdout)
        console._pipeline_console = True  # type: ignore[attr-defined]
        console.setFormatter(ConsoleFormatter())
        logger.addHandler(console)
    for handler in logger.handlers:
        if getattr(handler, "_pipeline_console", False):
            handler.setLevel(console_level)

    log_file = settings.logs_dir / settings.get("logging.json_log_file", "pipeline.jsonl")
    if not any(getattr(h, "_pipeline_file", None) == str(log_file) for h in logger.handlers):
        # a different settings object (tests) may point to another folder
        for handler in [h for h in logger.handlers if getattr(h, "_pipeline_file", None)]:
            logger.removeHandler(handler)
            handler.close()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler._pipeline_file = str(log_file)  # type: ignore[attr-defined]
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)

    return StageLogger(logger, {"run_id": run_id, "stage": stage})


# ---------------------------------------------------------------------------
# run records (outputs/runs/<run_id>.json)
# ---------------------------------------------------------------------------
COUNT_KEYS = (
    "source_rows", "raw_rows", "raw_batches_committed", "staging_rows",
    "quarantined_rows", "curated_rows", "curated_excluded_rows",
    "train_rows", "test_rows", "prediction_rows",
)


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write JSON to a temp file then rename, so a crash never leaves half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


class RunRecord:
    """Small JSON document that every stage of a run updates.

    Airflow runs each task in its own process, so the record lives on disk
    (and is copied into PostgreSQL by the publish stage).
    """

    def __init__(self, settings: Settings, run_id: str):
        self.settings = settings
        self.run_id = run_id
        self.path = settings.runs_dir / f"{run_id}.json"

    def load(self) -> dict:
        if self.path.is_file():
            return json.loads(self.path.read_text(encoding="utf-8"))
        return {
            "pipeline_run_id": self.run_id,
            "status": "running",
            "trigger": os.environ.get("PIPELINE_TRIGGER", "cli"),
            "started_at_utc": utc_now_iso(),
            "finished_at_utc": None,
            "counts": {},
            "stages": {},
        }

    def save(self, data: dict) -> None:
        _atomic_write_json(self.path, data)

    def start_stage(self, stage: str) -> None:
        data = self.load()
        entry = data["stages"].get(stage, {})
        entry.update({
            "status": "running",
            "started_at_utc": utc_now_iso(),
            "finished_at_utc": None,
            "attempts": int(entry.get("attempts", 0)) + 1,
            "error": None,
        })
        data["stages"][stage] = entry
        data["status"] = "running"
        self.save(data)

    def finish_stage(self, stage: str, status: str, duration_s: float,
                     counts: dict, details: dict, error: str | None = None) -> None:
        data = self.load()
        entry = data["stages"].setdefault(stage, {})
        entry.update({
            "status": status,
            "finished_at_utc": utc_now_iso(),
            "duration_s": round(duration_s, 3),
            "counts": counts,
            "details": details,
            "error": error,
        })
        for key, value in counts.items():
            if key in COUNT_KEYS:
                data["counts"][key] = value
        if status == "failed":
            data["status"] = "failed"
            data["finished_at_utc"] = entry["finished_at_utc"]
        self.save(data)

    def set_status(self, status: str) -> dict:
        data = self.load()
        data["status"] = status
        data["finished_at_utc"] = utc_now_iso()
        self.save(data)
        return data


@dataclass
class StageResult:
    """Filled in by a stage: row counts and any extra details worth recording."""

    counts: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    status: str = "success"   # a stage may set "partial" or "skipped"


@contextmanager
def track_stage(settings: Settings, run_id: str, stage: str) -> Iterator[tuple[StageLogger, StageResult]]:
    """Log start/end of a stage, time it, and store the outcome in the run record."""
    log = setup_logging(settings, run_id, stage)
    record = RunRecord(settings, run_id)
    record.start_stage(stage)
    result = StageResult()
    started = time.perf_counter()
    log.event("stage_started", f"{stage} started")
    try:
        yield log, result
    except BaseException as exc:
        duration = time.perf_counter() - started
        record.finish_stage(stage, "failed", duration, result.counts, result.details,
                            error=f"{type(exc).__name__}: {exc}")
        log.event("stage_failed", f"{stage} failed after {duration:.2f}s",
                  level=logging.ERROR, exc_info=True, duration_s=round(duration, 3))
        raise
    duration = time.perf_counter() - started
    record.finish_stage(stage, result.status, duration, result.counts, result.details)
    log.event("stage_finished", f"{stage} finished ({result.status}) in {duration:.2f}s",
              duration_s=round(duration, 3), **result.counts)
