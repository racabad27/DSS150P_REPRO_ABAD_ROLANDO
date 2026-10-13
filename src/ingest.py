"""Raw ingestion: full refresh and checkpointed incremental batches.

How the incremental simulation works
------------------------------------
There is only one source file, so "data arriving over time" is simulated by
cutting the file into deterministic batches on the *reading timestamp*
(calendar quarters by default: 2021Q1 ... 2023Q4). Each run ingests the
batches that are not committed yet; ``--max-batches`` limits how many arrive
per run. The watermark is the latest reading timestamp that has been
committed. This is a lab simulation: the timestamps are real event times from
the data, but their "arrival" is driven by the command line, not by a source
system.

Why reruns cannot duplicate rows
--------------------------------
* every batch is written to its own fixed path
  ``data/raw/batch_id=<id>/fire_risk_raw.parquet``. A rerun REPLACES that file
  (write to a temp file, then atomic rename) instead of appending;
* a batch only counts as committed after the written file was read back and
  checked (row count, unique row keys, content checksum). Only then is the
  checkpoint file updated, again atomically;
* rows whose ``source_row_key`` is already in a committed batch are skipped.

If the process dies after writing a file but before updating the checkpoint,
the next run simply rewrites that same batch file: no duplicates either way.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import Settings
from src.contract import Contract, load_contract, verify_source
from src.errors import ConfigError, DataQualityError, SimulatedInterruption, SourceVerificationError
from src.observability import StageLogger, utc_now_iso

RAW_FILE_NAME = "fire_risk_raw.parquet"
BATCH_DIR_PREFIX = "batch_id="
UNPARSED_BATCH = "unparsed_timestamp"   # rows whose Date/Time cannot be read land here
FIELD_SEPARATOR = "\x1f"                 # ASCII unit separator, never appears in the data
NULL_TOKEN = "\\N"                       # how a missing value is spelled inside a row hash
AUDIT_COLUMNS = [
    "source_row_key", "record_hash", "batch_id", "pipeline_run_id",
    "ingested_at_utc", "source_file", "source_checksum",
]


# ---------------------------------------------------------------------------
# reading the source and cutting batches
# ---------------------------------------------------------------------------
def _as_text(series: pd.Series) -> pd.Series:
    """Text form of a column for hashing; missing values become the token \\N."""
    return series.astype("string").fillna(NULL_TOKEN)


def row_hashes(frame: pd.DataFrame) -> pd.Series:
    """SHA-256 of each row's values in text form (column order matters)."""
    columns = list(frame.columns)
    joined = _as_text(frame[columns[0]])
    for column in columns[1:]:                       # vectorised string concatenation
        joined = joined + FIELD_SEPARATOR + _as_text(frame[column])
    return joined.map(lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest()).astype(str)


def content_checksum(record_hashes: pd.Series) -> str:
    """One checksum for a set of rows: SHA-256 over their sorted row hashes."""
    digest = hashlib.sha256()
    for value in sorted(record_hashes):
        digest.update(value.encode("ascii"))
    return digest.hexdigest()


def read_source_text(settings: Settings, contract: Contract) -> pd.DataFrame:
    """Read the CSV keeping every value as text (no type guessing at this layer)."""
    frame = pd.read_csv(
        settings.source_file,
        dtype=str,
        keep_default_na=False,   # a blank stays "" instead of becoming NaN
        na_filter=False,
        sep=settings.get("source.delimiter", ","),
        encoding=settings.get("source.encoding", "utf-8"),
    )
    missing = [c for c in contract.timestamp["source_columns"] if c not in frame.columns]
    if missing:
        raise DataQualityError(
            f"schema_columns: the source has no {missing} column(s); batches are cut on the "
            "reading time, so ingestion cannot continue"
        )
    frame.insert(0, "source_row_key", np.arange(1, len(frame) + 1, dtype="int64"))
    frame["record_hash"] = row_hashes(frame.drop(columns=["source_row_key"]))
    return frame


def assign_batches(frame: pd.DataFrame, contract: Contract, frequency: str) -> tuple[pd.Series, pd.Series]:
    """Return (batch_id, reading_ts) for every row; unreadable times get UNPARSED_BATCH."""
    date_col, time_col = contract.timestamp["source_columns"]
    reading_ts = pd.to_datetime(frame[date_col] + " " + frame[time_col],
                                format=contract.timestamp["format"], errors="coerce")
    if frequency == "quarter":
        labels = reading_ts.dt.year.astype("Int64").astype(str) + "Q" + reading_ts.dt.quarter.astype("Int64").astype(str)
    elif frequency == "month":
        labels = reading_ts.dt.strftime("%Y-%m")
    else:
        raise ConfigError(f"unknown batch frequency {frequency!r}")
    batch_id = labels.where(reading_ts.notna(), UNPARSED_BATCH).astype(str)
    return batch_id, reading_ts


# ---------------------------------------------------------------------------
# checkpoint (state kept OUTSIDE the code, in state/ingestion_checkpoint.json)
# ---------------------------------------------------------------------------
class Checkpoint:
    """Small JSON file holding the watermark and the committed batches."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[str, Any]:
        if self.path.is_file():
            return json.loads(self.path.read_text(encoding="utf-8"))
        return self.empty()

    @staticmethod
    def empty() -> dict[str, Any]:
        return {
            "dataset_id": None,
            "source_checksum": None,
            "batch_frequency": None,
            "watermark_reading_ts": None,
            "last_committed_batch_id": None,
            "source_rows": None,
            "batches_in_source": None,
            "committed_batches": {},
            "updated_at_utc": None,
        }

    def save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        state["updated_at_utc"] = utc_now_iso()
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8", newline="\n")
        os.replace(tmp, self.path)   # atomic on the same filesystem


# ---------------------------------------------------------------------------
# raw layer helpers
# ---------------------------------------------------------------------------
def batch_path(settings: Settings, batch_id: str) -> Path:
    return settings.raw_dir / f"{BATCH_DIR_PREFIX}{batch_id}" / RAW_FILE_NAME


def _write_parquet_atomic(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    frame.to_parquet(tmp, index=False)
    os.replace(tmp, target)


def clear_raw_layer(settings: Settings) -> int:
    """Delete raw batch folders (full refresh). Only touches data/raw/batch_id=*."""
    removed = 0
    if settings.raw_dir.is_dir():
        for folder in settings.raw_dir.glob(f"{BATCH_DIR_PREFIX}*"):
            if folder.is_dir():
                shutil.rmtree(folder)
                removed += 1
    return removed


def read_raw(settings: Settings, columns: list[str] | None = None) -> pd.DataFrame:
    """Read every COMMITTED raw batch (listed in the checkpoint), ordered by row key."""
    state = Checkpoint(settings.checkpoint_file).load()
    frames = []
    for batch_id in sorted(state["committed_batches"]):
        path = batch_path(settings, batch_id)
        if not path.is_file():
            raise DataQualityError(f"checkpoint lists batch {batch_id} but {path} is missing; "
                                   "run `python -m src.cli ingest --mode full`")
        frames.append(pd.read_parquet(path, columns=columns))
    if not frames:
        return pd.DataFrame(columns=columns or [])
    raw = pd.concat(frames, ignore_index=True)
    return raw.sort_values("source_row_key", kind="stable").reset_index(drop=True) \
        if "source_row_key" in raw.columns else raw


def raw_status(settings: Settings) -> dict[str, Any]:
    """Row/duplicate counts of the committed raw layer (used by `status` and tests)."""
    state = Checkpoint(settings.checkpoint_file).load()
    raw = read_raw(settings, columns=["source_row_key", "record_hash", "batch_id"])
    return {
        "committed_batches": len(state["committed_batches"]),
        "last_committed_batch_id": state["last_committed_batch_id"],
        "watermark_reading_ts": state["watermark_reading_ts"],
        "raw_rows": int(len(raw)),
        "distinct_source_row_keys": int(raw["source_row_key"].nunique()) if len(raw) else 0,
        "duplicate_source_row_keys": int(raw["source_row_key"].duplicated().sum()) if len(raw) else 0,
        "duplicate_record_hashes": int(raw["record_hash"].duplicated().sum()) if len(raw) else 0,
        "rows_per_batch": raw.groupby("batch_id").size().astype(int).to_dict() if len(raw) else {},
    }


# ---------------------------------------------------------------------------
# the ingestion stage
# ---------------------------------------------------------------------------
def ingest(
    settings: Settings,
    run_id: str,
    log: StageLogger,
    mode: str = "incremental",
    max_batches: int | None = None,
    fail_after_batches: int | None = None,
) -> dict[str, Any]:
    """Ingest pending batches into data/raw/ and advance the checkpoint.

    mode="incremental": only batches that are not committed yet.
    mode="full":        full refresh - clear raw + checkpoint, ingest everything.
    fail_after_batches: failure drill - stop on purpose after N committed batches.
    """
    if mode not in {"incremental", "full"}:
        raise ConfigError(f"ingest mode must be 'incremental' or 'full', got {mode!r}")
    if max_batches is not None and max_batches < 1:
        raise ConfigError("--max-batches must be at least 1")

    contract = load_contract(settings)
    source_info = verify_source(settings, log)
    frequency = settings.get("ingestion.batch_frequency")
    checkpoint = Checkpoint(settings.checkpoint_file)
    state = checkpoint.load()

    if mode == "full":
        removed = clear_raw_layer(settings)
        state = Checkpoint.empty()
        checkpoint.save(state)
        log.event("full_refresh", "Full refresh: raw layer and checkpoint reset", removed_batches=removed)
    else:
        if state["source_checksum"] and state["source_checksum"] != source_info["sha256"]:
            raise SourceVerificationError(
                "The source file changed since the last checkpoint. Run a full refresh: "
                "python -m src.cli ingest --mode full")
        if state["batch_frequency"] and state["batch_frequency"] != frequency:
            raise ConfigError(
                f"Batch frequency changed ({state['batch_frequency']} -> {frequency}). "
                "Run a full refresh: python -m src.cli ingest --mode full")

    source = read_source_text(settings, contract)
    batch_ids, reading_ts = assign_batches(source, contract, frequency)
    source["batch_id"] = batch_ids
    ordered = sorted(set(batch_ids) - {UNPARSED_BATCH}) + ([UNPARSED_BATCH] if UNPARSED_BATCH in set(batch_ids) else [])

    state["source_rows"] = int(len(source))
    state["batches_in_source"] = len(ordered)
    committed = state["committed_batches"]
    pending = [b for b in ordered if b not in committed]
    watermark = pd.Timestamp(state["watermark_reading_ts"]) if state["watermark_reading_ts"] else None
    for batch_id in pending:
        if batch_id != UNPARSED_BATCH and watermark is not None:
            first_ts = reading_ts[batch_ids == batch_id].min()
            if first_ts <= watermark:
                raise DataQualityError(
                    f"batch {batch_id} starts at {first_ts}, at or before the watermark {watermark}; "
                    "the checkpoint does not match the source. Run --mode full.")
    if max_batches:
        pending = pending[:max_batches]

    log.event("ingest_plan", f"{len(ordered)} batches in source, {len(committed)} already committed, "
              f"{len(pending)} to ingest now", mode=mode, frequency=frequency,
              watermark=state["watermark_reading_ts"])

    committed_keys = set(read_raw(settings, columns=["source_row_key"])["source_row_key"]) if committed else set()
    ingested_at = utc_now_iso()
    written: list[dict[str, Any]] = []
    skipped_rows = 0

    for position, batch_id in enumerate(pending, start=1):
        part = source[source["batch_id"] == batch_id].copy()
        already = part["source_row_key"].isin(committed_keys)
        skipped_rows += int(already.sum())
        part = part[~already]
        part["pipeline_run_id"] = run_id
        part["ingested_at_utc"] = ingested_at
        part["source_file"] = settings.source_file.name
        part["source_checksum"] = source_info["sha256"]
        source_columns = [c for c in source.columns if c not in AUDIT_COLUMNS]
        part = part[source_columns + AUDIT_COLUMNS].sort_values("source_row_key")

        target = batch_path(settings, batch_id)
        _write_parquet_atomic(part, target)

        # read the file back before trusting it
        check = pd.read_parquet(target, columns=["source_row_key", "record_hash"])
        expected_checksum = content_checksum(part["record_hash"])
        if len(check) != len(part) or check["source_row_key"].duplicated().any() \
                or content_checksum(check["record_hash"]) != expected_checksum:
            raise DataQualityError(f"verification of the written raw batch {batch_id} failed")

        batch_ts = reading_ts[part.index]
        entry = {
            "rows": int(len(part)),
            "min_reading_ts": None if batch_id == UNPARSED_BATCH else str(batch_ts.min()),
            "max_reading_ts": None if batch_id == UNPARSED_BATCH else str(batch_ts.max()),
            "content_sha256": expected_checksum,
            "file": target.relative_to(settings.data_dir).as_posix(),
            "committed_at_utc": utc_now_iso(),
            "pipeline_run_id": run_id,
        }
        state["committed_batches"][batch_id] = entry
        state.update({
            "dataset_id": contract.raw["dataset_id"],
            "source_checksum": source_info["sha256"],
            "batch_frequency": frequency,
            "last_committed_batch_id": batch_id,
        })
        if entry["max_reading_ts"] and (state["watermark_reading_ts"] is None
                                         or entry["max_reading_ts"] > state["watermark_reading_ts"]):
            state["watermark_reading_ts"] = entry["max_reading_ts"]
        checkpoint.save(state)       # advance ONLY after the batch was verified
        committed_keys.update(part["source_row_key"].tolist())
        written.append({"batch_id": batch_id, **entry})
        log.event("batch_committed", f"Batch {batch_id} committed ({len(part):,} rows)",
                  batch_id=batch_id, rows=len(part), watermark=state["watermark_reading_ts"])

        if fail_after_batches and position == fail_after_batches and position < len(pending):
            raise SimulatedInterruption(
                f"Failure drill: run stopped on purpose after {position} committed batch(es). "
                "Rerun the same command to resume from the checkpoint.")

    if not written and state["committed_batches"]:
        checkpoint.save(state)       # keep source_rows / batches_in_source up to date
    total_rows = sum(b["rows"] for b in state["committed_batches"].values())
    summary = {
        "mode": mode,
        "batches_in_source": len(ordered),
        "batches_written_this_run": len(written),
        "batches_committed_total": len(state["committed_batches"]),
        "rows_written_this_run": sum(b["rows"] for b in written),
        "already_committed_rows_skipped": skipped_rows,
        "raw_rows_total": total_rows,
        "source_rows": int(len(source)),
        "coverage": round(total_rows / len(source), 6) if len(source) else 0.0,
        "watermark_reading_ts": state["watermark_reading_ts"],
        "source_sha256": source_info["sha256"],
        "batches": written,
    }
    manifest_path = settings.raw_dir / "_runs" / f"{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"pipeline_run_id": run_id, "ingested_at_utc": ingested_at,
                                         "source_file": settings.source_file.name, **summary}, indent=2),
                             encoding="utf-8", newline="\n")
    if not written:
        log.event("nothing_to_ingest", "No pending batches - raw layer already up to date",
                  raw_rows=total_rows)
    return summary
