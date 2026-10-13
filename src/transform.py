"""raw -> staging -> curated transformations.

Staging  = the raw rows made *technically* clean: values parsed with the types
           and formats from the data contract, column names in snake_case,
           Date + Time combined into ``reading_ts``, exact duplicates dropped,
           and every row that breaks a contract rule moved to quarantine with a
           reason code. Nothing about the business meaning changes here, so
           the 2,000 rows with a blank ``Workers`` stay (blank is allowed).

Curated  = the stable, model-ready business dataset: the baseline rule
           (complete cases only) is applied, the derived features of the
           original notebook are added, and rows are ordered by time. Rows
           excluded by the business rule are logged, not silently lost.

Each layer is validated IN MEMORY first and only written ("published") when
its blocking checks pass (write-audit-publish), using an atomic file rename.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads

from src.config import Settings
from src.contract import Contract, load_contract
from src.errors import DataQualityError
from src.ingest import Checkpoint, read_raw, row_hashes
from src.observability import StageLogger, utc_now_iso
from src.validate import (
    DERIVED_FEATURES, STAGING_BUSINESS_COLUMNS, validate_curated_frame, validate_staging_frame,
)

STAGING_FILE = "fire_risk_staging.parquet"
CURATED_FILE = "fire_risk_curated.parquet"
QUARANTINE_FILE = "rejected_rows.csv"
EXCLUSIONS_FILE = "curated_exclusions.csv"
PARTITIONED_DATASET = "fire_risk_curated"


def staging_path(settings: Settings) -> Path:
    return settings.staging_dir / STAGING_FILE


def curated_path(settings: Settings) -> Path:
    return settings.curated_dir / CURATED_FILE


def quarantine_path(settings: Settings) -> Path:
    return settings.quarantine_dir / QUARANTINE_FILE


def partitioned_path(settings: Settings) -> Path:
    return settings.partitioned_dir / PARTITIONED_DATASET


def write_parquet_atomic(frame: pd.DataFrame, target: Path) -> None:
    """Write to a temp file and rename, so readers never see half a file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    frame.to_parquet(tmp, index=False)
    os.replace(tmp, target)


# ---------------------------------------------------------------------------
# staging
# ---------------------------------------------------------------------------
def standardise_rows(raw: pd.DataFrame, contract: Contract) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse every raw value with the contract rules.

    Returns the typed frame (same index as ``raw``) and a frame of issues
    with one line per (row, problem): row, reason_code, rule_id, detail.
    """
    typed = pd.DataFrame(index=raw.index)
    problems: list[tuple[pd.Series, str, str, str]] = []

    date_col, time_col = contract.timestamp["source_columns"]
    fmt = contract.timestamp["format"]
    reading_ts = pd.to_datetime(raw[date_col].str.strip() + " " + raw[time_col].str.strip(),
                                format=fmt, errors="coerce")
    problems.append((reading_ts.isna(), "INVALID_TIMESTAMP", "types_and_formats",
                     f"Date/Time does not match {fmt}"))
    cadence = int(contract.timestamp["cadence_minutes"])
    off_grid = reading_ts.notna() & ((reading_ts.dt.minute % cadence != 0) | (reading_ts.dt.second != 0))
    problems.append((off_grid, "OFF_CADENCE", "cadence", f"time is not on the {cadence}-minute grid"))
    typed["reading_ts"] = reading_ts

    for spec in contract.value_columns():
        text = raw[spec.name].astype(str).str.strip()
        blank = text.eq("")
        if not spec.nullable:
            problems.append((blank, "MISSING_REQUIRED", "required_values", f"{spec.name} is empty"))
        if spec.type == "category":
            allowed = [str(v) for v in spec.allowed_values or ()]
            problems.append((~blank & ~text.isin(allowed), "INVALID_CATEGORY", "allowed_categories",
                             f"{spec.name} is not one of {allowed}"))
            typed[spec.staging_name] = text.where(~blank)
            continue
        number = pd.to_numeric(text.where(~blank), errors="coerce")
        problems.append((~blank & number.isna(), "INVALID_TYPE", "types_and_formats",
                         f"{spec.name} is not a number"))
        if spec.type == "integer":
            problems.append((number.notna() & (number % 1 != 0), "INVALID_TYPE", "types_and_formats",
                             f"{spec.name} is not a whole number"))
        if spec.allowed_values is not None:
            allowed_numbers = [float(v) for v in spec.allowed_values]
            problems.append((number.notna() & ~number.isin(allowed_numbers), "INVALID_CATEGORY",
                             "allowed_categories", f"{spec.name} is not one of {list(spec.allowed_values)}"))
        if spec.min is not None and spec.max is not None:
            problems.append((number.notna() & ((number < spec.min) | (number > spec.max)), "OUT_OF_RANGE",
                             "value_ranges", f"{spec.name} is outside [{spec.min}, {spec.max}]"))
        typed[spec.staging_name] = number

    threshold = contract.rule("accident_matches_risk")["risk_threshold"]
    risk, accident = typed["risk"], typed["accident"]
    mismatch = risk.notna() & accident.notna() & (((risk > threshold) & (accident != 1))
                                                  | ((risk < threshold) & (accident != 0)))
    problems.append((mismatch, "ACCIDENT_RISK_MISMATCH", "accident_matches_risk",
                     f"Accident does not agree with Risk vs {threshold}"))

    frames = [pd.DataFrame({"row": raw.index[mask.to_numpy()], "reason_code": code,
                            "rule_id": rule, "detail": detail})
              for mask, code, rule, detail in problems if bool(mask.any())]
    issues = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["row", "reason_code", "rule_id", "detail"])
    return typed, issues


def _quarantine_frame(raw: pd.DataFrame, issues: pd.DataFrame, contract: Contract, run_id: str) -> pd.DataFrame:
    """One line per rejected row: the original text values plus why it was rejected."""
    columns = ["source_row_key", "batch_id", *contract.source_columns, "record_hash",
               "reason_code", "rule_ids", "reason_detail", "quarantine_stage",
               "pipeline_run_id", "quarantined_at_utc"]
    if issues.empty:
        return pd.DataFrame(columns=columns)
    grouped = issues.groupby("row", sort=True).agg(
        reason_code=("reason_code", "first"),
        rule_ids=("rule_id", lambda s: ";".join(dict.fromkeys(s))),
        reason_detail=("detail", lambda s: "; ".join(dict.fromkeys(s))),
    )
    rejected = raw.loc[grouped.index, ["source_row_key", "batch_id", *contract.source_columns, "record_hash"]].copy()
    rejected = rejected.join(grouped)
    rejected["quarantine_stage"] = "staging"
    rejected["pipeline_run_id"] = run_id
    rejected["quarantined_at_utc"] = utc_now_iso()
    return rejected[columns].sort_values("source_row_key")


def build_staging(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    """raw -> staging (+ quarantine). Publishes only if the staging checks pass."""
    contract = load_contract(settings)
    raw = read_raw(settings)
    if raw.empty:
        raise DataQualityError("The raw layer is empty. Run `python -m src.cli ingest` first.")

    typed, issues = standardise_rows(raw, contract)
    valid = typed.drop(index=issues["row"].unique())

    # exact duplicates (identical raw content) are dropped, keeping the first
    exact_duplicate = raw.loc[valid.index, "record_hash"].duplicated(keep="first")
    dropped_duplicates = int(exact_duplicate.sum())
    valid = valid[~exact_duplicate.to_numpy()]
    # same reading time but different values: we cannot know which is right
    conflict = valid["reading_ts"].duplicated(keep=False)
    if conflict.any():
        conflict_rows = valid.index[conflict.to_numpy()]
        issues = pd.concat([issues, pd.DataFrame({
            "row": conflict_rows, "reason_code": "DUPLICATE_KEY_CONFLICT", "rule_id": "unique_business_key",
            "detail": "another row has the same reading time but different values"})], ignore_index=True)
        valid = valid.drop(index=conflict_rows)

    staging = valid.copy()
    for spec in contract.value_columns():
        if spec.type == "integer":
            staging[spec.staging_name] = staging[spec.staging_name].astype("Int64" if spec.nullable else "int64")
    staging.insert(0, "source_row_key", raw.loc[staging.index, "source_row_key"].astype("int64"))
    staging["record_hash"] = raw.loc[staging.index, "record_hash"]
    staging["batch_id"] = raw.loc[staging.index, "batch_id"]
    staging["ingested_at_utc"] = pd.to_datetime(raw.loc[staging.index, "ingested_at_utc"], utc=True)
    staging = staging.sort_values("reading_ts", kind="stable").reset_index(drop=True)
    staging["row_hash"] = row_hashes(staging[STAGING_BUSINESS_COLUMNS])
    staging["pipeline_run_id"] = run_id
    staging["processed_at_utc"] = pd.Timestamp.now(tz="UTC")

    rejected = _quarantine_frame(raw, issues, contract, run_id)
    quarantine_path(settings).parent.mkdir(parents=True, exist_ok=True)
    rejected.to_csv(quarantine_path(settings), index=False)
    if len(rejected):
        log.event("rows_quarantined", f"{len(rejected):,} row(s) moved to quarantine",
                  rows=len(rejected), reasons=rejected["reason_code"].value_counts().to_dict())

    report = validate_staging_frame(staging, contract, settings, run_id, raw_rows=len(raw),
                                    quarantined_rows=len(rejected), dropped_duplicates=dropped_duplicates)
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()          # nothing is published if a blocking check failed

    write_parquet_atomic(staging, staging_path(settings))
    return {
        "raw_rows": int(len(raw)),
        "staging_rows": int(len(staging)),
        "quarantined_rows": int(len(rejected)),
        "exact_duplicates_dropped": dropped_duplicates,
        "quality": report.summary(),
        "staging_file": staging_path(settings).relative_to(settings.data_dir).as_posix(),
    }


# ---------------------------------------------------------------------------
# curated
# ---------------------------------------------------------------------------
def add_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derived features of the original notebook (cells 8 and 11), plus partition keys.

    No statistics are learned from the data here (only row-by-row arithmetic),
    so computing them before the train/test split cannot leak information.
    """
    out = frame.copy()
    ts = out["reading_ts"]
    out["reading_year"] = ts.dt.year.astype("int64")
    out["reading_month"] = ts.dt.month.astype("int64")
    out["hour_of_day"] = ts.dt.hour.astype("int64")
    out["day_of_week"] = ts.dt.dayofweek.astype("int64")          # Monday = 0, Sunday = 6
    out["is_weekend"] = out["day_of_week"].isin([5, 6]).astype("int64")
    out["month"] = ts.dt.month.astype("int64")
    out["workers_sparks_interaction"] = out["workers"].astype("float64") * out["sparks"].astype("float64")
    out["temp_pressure_interaction"] = out["temp"] * out["pressure"]
    out["vibration_speed_interaction"] = out["vibration"] * out["speed"]
    return out


CURATED_COLUMNS = (
    ["source_row_key", "reading_ts", "reading_year", "reading_month"]
    + STAGING_BUSINESS_COLUMNS[1:]
    + DERIVED_FEATURES
    + ["record_hash", "row_hash", "batch_id", "pipeline_run_id", "processed_at_utc"]
)


def write_partitions(curated: pd.DataFrame, settings: Settings) -> dict[str, Any]:
    """Write data/partitioned/fire_risk_curated/reading_year=YYYY/reading_month=M/.

    Written to a temporary folder first and swapped in at the end, so a rerun
    replaces the partitions instead of adding files next to old ones.
    """
    base = settings.partitioned_dir
    base.mkdir(parents=True, exist_ok=True)
    final, tmp, old = partitioned_path(settings), base / f".{PARTITIONED_DATASET}.tmp", base / f".{PARTITIONED_DATASET}.old"
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.rmtree(old, ignore_errors=True)
    table = pa.Table.from_pandas(curated, preserve_index=False)
    partition_columns = list(settings.get("curated.partition_columns"))
    pads.write_dataset(table, tmp, format="parquet", partitioning=partition_columns,
                       partitioning_flavor="hive", basename_template="part-{i}.parquet",
                       preserve_order=True, existing_data_behavior="error")
    if final.exists():
        os.replace(final, old)
    os.replace(tmp, final)
    shutil.rmtree(old, ignore_errors=True)
    files = sorted(final.rglob("*.parquet"))
    return {"partition_columns": partition_columns, "partitions": len(files),
            "path": final.relative_to(settings.data_dir).as_posix()}


def build_curated(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    """staging -> curated (+ exclusion log + partitioned copy)."""
    if not staging_path(settings).is_file():
        raise DataQualityError("No staging data. Run `python -m src.cli transform` first.")
    staging = pd.read_parquet(staging_path(settings))

    required = list(settings.get("curated.required_complete_columns") or [])
    excluded_mask = staging[required].isna().any(axis=1) if required else pd.Series(False, index=staging.index)
    exclusions = staging.loc[excluded_mask, ["source_row_key", "reading_ts", "record_hash"]].copy()
    exclusions["rule"] = "baseline listwise deletion: " + ", ".join(required) + " must not be empty"
    exclusions["pipeline_run_id"] = run_id

    curated = staging.loc[~excluded_mask].copy()
    curated["workers"] = curated["workers"].astype("int64")
    curated = add_features(curated)
    curated = curated.sort_values("reading_ts", kind="stable").reset_index(drop=True)
    curated["row_hash"] = row_hashes(curated[STAGING_BUSINESS_COLUMNS + DERIVED_FEATURES])
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = pd.Timestamp.now(tz="UTC")
    curated = curated[CURATED_COLUMNS]

    report = validate_curated_frame(curated, settings, run_id, staging_rows=len(staging),
                                    excluded_rows=int(excluded_mask.sum()))
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()

    write_parquet_atomic(curated, curated_path(settings))
    settings.curated_dir.mkdir(parents=True, exist_ok=True)
    exclusions.to_csv(settings.curated_dir / EXCLUSIONS_FILE, index=False)
    partitions = write_partitions(curated, settings)
    log.event("curated_published", f"Curated dataset published ({len(curated):,} rows, "
              f"{len(exclusions):,} excluded by the baseline rule)", partitions=partitions["partitions"])
    return {
        "staging_rows": int(len(staging)),
        "curated_rows": int(len(curated)),
        "curated_excluded_rows": int(len(exclusions)),
        "quality": report.summary(),
        "partitions": partitions,
        "curated_file": curated_path(settings).relative_to(settings.data_dir).as_posix(),
    }


def raw_coverage(settings: Settings) -> tuple[int, int]:
    """(rows committed in raw, rows expected from the source) for coverage checks."""
    state = Checkpoint(settings.checkpoint_file).load()
    committed = sum(int(b["rows"]) for b in state["committed_batches"].values())
    return committed, load_contract(settings).expected_rows
