"""Data-quality checks for every layer.

A check is a small function that compares an *observed* value with an
*expected* one and returns a CheckResult (pass / warn / fail). Checks are
grouped in suites per layer:

    raw       schema, uniqueness of row keys, source identity, coverage
    staging   schema, completeness, uniqueness, validity, consistency,
              quarantine threshold, row conservation
    curated   completeness, derived-feature consistency, ordering, exclusions

Blocking checks stop the pipeline (DataQualityError, exit code 4) BEFORE the
layer is published, so bad data cannot silently reach the model. Every report
is saved to outputs/quality/ (and later copied to PostgreSQL).
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from src.config import Settings
from src.contract import Contract, load_contract, load_manifest
from src.errors import DataQualityError
from src.ingest import AUDIT_COLUMNS as RAW_AUDIT_COLUMNS
from src.observability import StageLogger, utc_now_iso

STAGING_BUSINESS_COLUMNS = [
    "reading_ts", "factory", "region", "shift", "workers", "exp", "training", "temp",
    "pressure", "humidity", "vibration", "speed", "age", "service_days", "gas", "sparks",
    "alarm", "risk", "accident",
]
DERIVED_FEATURES = [
    "hour_of_day", "day_of_week", "is_weekend", "month",
    "workers_sparks_interaction", "temp_pressure_interaction", "vibration_speed_interaction",
]


@dataclass
class CheckResult:
    check_id: str
    layer: str
    category: str
    description: str
    status: str                 # pass | warn | fail
    blocking: bool
    observed: Any = None
    expected: Any = None
    failing_rows: int = 0


@dataclass
class QualityReport:
    layer: str
    run_id: str
    results: list[CheckResult] = field(default_factory=list)
    created_at_utc: str = field(default_factory=utc_now_iso)

    def check(self, check_id: str, category: str, description: str, passed: bool,
              observed: Any = None, expected: Any = None, failing_rows: int = 0,
              blocking: bool = True) -> CheckResult:
        """Record one check. Non-blocking failures become warnings."""
        status = "pass" if passed else ("fail" if blocking else "warn")
        result = CheckResult(f"{self.layer}.{check_id}", self.layer, category, description,
                             status, blocking, _plain(observed), _plain(expected), int(failing_rows))
        self.results.append(result)
        return result

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "fail"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "warn"]

    def summary(self) -> dict[str, int]:
        return {"checks": len(self.results), "passed": sum(r.status == "pass" for r in self.results),
                "warnings": len(self.warnings), "failed": len(self.failures)}

    def to_dict(self) -> dict:
        return {"layer": self.layer, "pipeline_run_id": self.run_id, "created_at_utc": self.created_at_utc,
                "summary": self.summary(), "results": [asdict(r) for r in self.results]}

    def save(self, settings: Settings) -> None:
        settings.quality_dir.mkdir(parents=True, exist_ok=True)
        text = json.dumps(self.to_dict(), indent=2, default=str)
        (settings.quality_dir / f"{self.run_id}_{self.layer}.json").write_text(text, encoding="utf-8")
        (settings.quality_dir / f"latest_{self.layer}.json").write_text(text, encoding="utf-8")

    def log_results(self, log: StageLogger) -> None:
        for result in self.results:
            if result.status != "pass":
                log.event("quality_check", f"{result.status.upper()}: {result.check_id} - {result.description}",
                          level=logging.ERROR if result.status == "fail" else logging.WARNING,
                          observed=result.observed, expected=result.expected,
                          failing_rows=result.failing_rows)
        log.event("quality_summary", f"{self.layer} quality checks: {self.summary()}", **self.summary())

    def raise_if_failed(self) -> None:
        if self.failures:
            lines = [f"- {r.check_id}: observed {r.observed}, expected {r.expected}" for r in self.failures]
            raise DataQualityError(f"{len(self.failures)} blocking {self.layer} check(s) failed:\n" + "\n".join(lines))


def _plain(value: Any) -> Any:
    """Make numpy/pandas values JSON friendly."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    return value


# ---------------------------------------------------------------------------
# raw layer
# ---------------------------------------------------------------------------
def validate_raw_frame(raw: pd.DataFrame, contract: Contract, settings: Settings,
                       run_id: str, committed_rows: int) -> QualityReport:
    report = QualityReport("raw", run_id)
    expected_cols = contract.source_columns
    present = [c for c in raw.columns if c in expected_cols]
    missing = [c for c in expected_cols if c not in raw.columns]
    unexpected = [c for c in raw.columns if c not in expected_cols and c not in RAW_AUDIT_COLUMNS]
    report.check("schema_columns", "schema",
                 "Raw data has exactly the 20 contract columns in the contract order",
                 not missing and not unexpected and present == expected_cols,
                 observed={"missing": missing, "unexpected": unexpected}, expected=expected_cols)
    report.check("audit_columns", "schema", "Every raw row carries the ingestion audit columns",
                 all(c in raw.columns for c in RAW_AUDIT_COLUMNS),
                 observed=[c for c in RAW_AUDIT_COLUMNS if c not in raw.columns], expected=RAW_AUDIT_COLUMNS)
    dup_keys = int(raw["source_row_key"].duplicated().sum()) if "source_row_key" in raw else -1
    report.check("unique_source_row_key", "uniqueness", "Each source row was ingested once",
                 dup_keys == 0, observed=dup_keys, expected=0, failing_rows=max(dup_keys, 0))
    report.check("row_count_matches_checkpoint", "completeness",
                 "Raw files contain exactly the rows the checkpoint says were committed",
                 len(raw) == committed_rows, observed=len(raw), expected=committed_rows)
    manifest_sha = load_manifest(settings)["file"]["sha256"]
    if "source_checksum" in raw:
        wrong = int((raw["source_checksum"] != manifest_sha).sum())
        report.check("source_identity", "freshness_applicability",
                     "Every raw row came from the source snapshot in the manifest",
                     wrong == 0, observed=wrong, expected=0, failing_rows=wrong,
                     blocking=settings.verify_checksum)
    expected_rows = contract.expected_rows
    report.check("row_count", "completeness", f"Raw layer holds the expected {expected_rows:,} rows",
                 len(raw) == expected_rows, observed=len(raw), expected=expected_rows,
                 blocking=contract.on_violation("row_count") == "fail")
    return report



# ---------------------------------------------------------------------------
# staging layer (dataset-level checks after row-level quarantine)
# ---------------------------------------------------------------------------
def validate_staging_frame(staging: pd.DataFrame, contract: Contract, settings: Settings, run_id: str,
                           raw_rows: int, quarantined_rows: int, dropped_duplicates: int) -> QualityReport:
    report = QualityReport("staging", run_id)
    missing = [c for c in STAGING_BUSINESS_COLUMNS if c not in staging.columns]
    report.check("schema_columns", "schema", "Staging has every business column",
                 not missing, observed=missing, expected=[])

    for spec in contract.value_columns():
        col = spec.staging_name
        if col not in staging:
            continue
        nulls = int(staging[col].isna().sum())
        if not spec.nullable:
            report.check(f"not_null.{col}", "completeness", f"{col} is never empty",
                         nulls == 0, observed=nulls, expected=0, failing_rows=nulls)
        elif spec.max_null_rate is not None:
            rate = nulls / len(staging) if len(staging) else 0.0
            report.check(f"null_rate.{col}", "completeness",
                         f"{col} is empty in at most {spec.max_null_rate:.0%} of rows",
                         rate <= spec.max_null_rate, observed=round(rate, 6), expected=spec.max_null_rate,
                         blocking=contract.on_violation("workers_null_rate") == "fail")
        values = staging[col].dropna()
        if spec.allowed_values is not None:
            bad = int((~values.isin(list(spec.allowed_values))).sum())
            report.check(f"allowed_values.{col}", "validity", f"{col} only uses {list(spec.allowed_values)}",
                         bad == 0, observed=bad, expected=0, failing_rows=bad)
        if spec.min is not None and spec.max is not None and len(values):
            bad = int(((values < spec.min) | (values > spec.max)).sum())
            report.check(f"range.{col}", "validity", f"{col} within [{spec.min}, {spec.max}]",
                         bad == 0, observed=[values.min(), values.max()], expected=[spec.min, spec.max],
                         failing_rows=bad)

    dup_ts = int(staging["reading_ts"].duplicated().sum())
    report.check("unique_business_key", "uniqueness", "reading_ts is unique (one reading per slot)",
                 dup_ts == 0, observed=dup_ts, expected=0, failing_rows=dup_ts)
    dup_keys = int(staging["source_row_key"].duplicated().sum())
    report.check("unique_source_row_key", "uniqueness", "source_row_key is unique",
                 dup_keys == 0, observed=dup_keys, expected=0, failing_rows=dup_keys)

    cadence = contract.timestamp["cadence_minutes"]
    off_grid = int(((staging["reading_ts"].dt.minute % cadence != 0) | (staging["reading_ts"].dt.second != 0)).sum())
    report.check("cadence", "consistency", f"Readings sit on the {cadence}-minute grid",
                 off_grid == 0, observed=off_grid, expected=0, failing_rows=off_grid)

    threshold = contract.rule("accident_matches_risk")["risk_threshold"]
    mismatch = int((((staging["risk"] > threshold) & (staging["accident"] != 1))
                    | ((staging["risk"] < threshold) & (staging["accident"] != 0))).sum())
    report.check("accident_matches_risk", "consistency",
                 f"Accident is 1 above Risk {threshold} and 0 below it (business rule)",
                 mismatch == 0, observed=mismatch, expected=0, failing_rows=mismatch)

    max_rate = float(settings.get("staging.max_quarantine_rate"))
    rate = quarantined_rows / raw_rows if raw_rows else 0.0
    report.check("quarantine_threshold", "completeness",
                 f"At most {max_rate:.0%} of raw rows are quarantined",
                 rate <= max_rate, observed=round(rate, 6), expected=max_rate, failing_rows=quarantined_rows)

    balance = len(staging) + quarantined_rows + dropped_duplicates
    report.check("row_conservation", "completeness",
                 "staging + quarantined + dropped duplicates = raw rows (nothing lost silently)",
                 balance == raw_rows, observed=balance, expected=raw_rows)

    ts = contract.timestamp
    first, last = str(staging["reading_ts"].min()), str(staging["reading_ts"].max())
    report.check("time_coverage", "freshness_applicability",
                 "Readings cover the period declared in the contract",
                 first == ts["expected_first"] and last == ts["expected_last"],
                 observed=[first, last], expected=[ts["expected_first"], ts["expected_last"]],
                 blocking=contract.on_violation("time_coverage") == "fail")
    return report


# ---------------------------------------------------------------------------
# curated layer
# ---------------------------------------------------------------------------
def validate_curated_frame(curated: pd.DataFrame, settings: Settings, run_id: str,
                           staging_rows: int, excluded_rows: int) -> QualityReport:
    report = QualityReport("curated", run_id)
    needed = STAGING_BUSINESS_COLUMNS + DERIVED_FEATURES
    missing = [c for c in needed if c not in curated.columns]
    report.check("schema_columns", "schema", "Curated has all business and derived feature columns",
                 not missing, observed=missing, expected=[])
    if missing:
        return report

    nulls = int(curated[needed].isna().sum().sum())
    report.check("no_missing_values", "completeness",
                 "No missing values in any model input or target column (complete cases)",
                 nulls == 0, observed=nulls, expected=0, failing_rows=nulls)
    report.check("row_balance", "completeness", "curated rows = staging rows - excluded rows",
                 len(curated) == staging_rows - excluded_rows,
                 observed=len(curated), expected=staging_rows - excluded_rows)
    report.check("unique_business_key", "uniqueness", "reading_ts is unique",
                 not curated["reading_ts"].duplicated().any(),
                 observed=int(curated["reading_ts"].duplicated().sum()), expected=0)
    report.check("ordered_by_time", "consistency",
                 "Rows are strictly ordered by reading_ts (the split depends on it)",
                 bool(curated["reading_ts"].is_monotonic_increasing), observed=None, expected="increasing")

    ts = curated["reading_ts"]
    checks = {
        "hour_of_day": curated["hour_of_day"].equals(ts.dt.hour.astype(curated["hour_of_day"].dtype)),
        "day_of_week": curated["day_of_week"].equals(ts.dt.dayofweek.astype(curated["day_of_week"].dtype)),
        "is_weekend": bool((curated["is_weekend"] == curated["day_of_week"].isin([5, 6]).astype(int)).all()),
        "month": curated["month"].equals(ts.dt.month.astype(curated["month"].dtype)),
        "workers_sparks_interaction": bool(np.array_equal(curated["workers_sparks_interaction"].to_numpy(float),
                                                          (curated["workers"].astype(float) * curated["sparks"].astype(float)).to_numpy())),
        "temp_pressure_interaction": bool(np.array_equal(curated["temp_pressure_interaction"].to_numpy(float),
                                                         (curated["temp"] * curated["pressure"]).to_numpy(float))),
        "vibration_speed_interaction": bool(np.array_equal(curated["vibration_speed_interaction"].to_numpy(float),
                                                           (curated["vibration"] * curated["speed"]).to_numpy(float))),
    }
    for name, ok in checks.items():
        report.check(f"derived.{name}", "consistency", f"{name} recomputed from its inputs matches",
                     ok, observed=ok, expected=True)
    return report
