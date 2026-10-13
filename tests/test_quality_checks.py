"""Data-quality assertions: each check must catch an intentionally broken dataset.

A clean staging/curated dataset is built once from the synthetic sample with
the real pipeline code, then copies of it are broken in one specific way and
the matching check has to FAIL (lab acceptance test A14).
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.contract import load_contract
from src.pipeline import run_stage
from src.train import chronological_split, encode, feature_matrix, validate_model_input
from src.transform import curated_path, staging_path
from src.validate import validate_curated_frame, validate_staging_frame


@pytest.fixture(scope="module")
def clean_layers(tmp_path_factory):
    from tests.conftest import build_workspace

    settings = build_workspace(tmp_path_factory.mktemp("quality"))
    for stage in ("ingest_source", "validate_raw", "transform_staging", "build_curated"):
        run_stage(stage, settings, "test_quality")
    return settings, pd.read_parquet(staging_path(settings)), pd.read_parquet(curated_path(settings))


def _staging_report(settings, frame, quarantined=0):
    return validate_staging_frame(frame, load_contract(settings), settings, "t", raw_rows=len(frame) + quarantined,
                                  quarantined_rows=quarantined, dropped_duplicates=0)


def _failed(report):
    return {r.check_id for r in report.failures}


def test_clean_staging_passes_every_check(clean_layers):
    settings, staging, _ = clean_layers
    assert _staging_report(settings, staging).failures == []


def test_schema_check_detects_missing_column(clean_layers):
    settings, staging, _ = clean_layers
    assert "staging.schema_columns" in _failed(_staging_report(settings, staging.drop(columns=["pressure"])))


def test_completeness_check_detects_missing_target(clean_layers):
    settings, staging, _ = clean_layers
    broken = staging.copy()
    broken.loc[broken.index[:3], "risk"] = None
    assert "staging.not_null.risk" in _failed(_staging_report(settings, broken))


def test_uniqueness_check_detects_duplicate_business_key(clean_layers):
    settings, staging, _ = clean_layers
    broken = staging.copy()
    broken.loc[broken.index[1], "reading_ts"] = broken.loc[broken.index[0], "reading_ts"]
    assert "staging.unique_business_key" in _failed(_staging_report(settings, broken))


def test_validity_checks_detect_range_and_category_errors(clean_layers):
    settings, staging, _ = clean_layers
    broken = staging.copy()
    broken.loc[broken.index[0], "temp"] = 500.0
    broken.loc[broken.index[1], "factory"] = "Factory_Z"
    failed = _failed(_staging_report(settings, broken))
    assert {"staging.range.temp", "staging.allowed_values.factory"} <= failed


def test_business_rule_detects_accident_risk_mismatch(clean_layers):
    settings, staging, _ = clean_layers
    broken = staging.copy()
    low = broken.index[broken["risk"] < 75][0]
    broken.loc[low, "accident"] = 1
    assert "staging.accident_matches_risk" in _failed(_staging_report(settings, broken))


def test_quarantine_threshold_stops_a_run_with_too_many_bad_rows(clean_layers):
    settings, staging, _ = clean_layers
    report = _staging_report(settings, staging, quarantined=int(len(staging) * 0.05))
    assert "staging.quarantine_threshold" in _failed(report)


def test_curated_check_detects_a_wrong_derived_feature(clean_layers):
    settings, staging, curated = clean_layers
    broken = curated.copy()
    broken.loc[0, "temp_pressure_interaction"] += 1.0
    excluded = len(staging) - len(curated)
    report = validate_curated_frame(broken, settings, "t", staging_rows=len(staging), excluded_rows=excluded)
    assert "curated.derived.temp_pressure_interaction" in _failed(report)


def test_predictive_integrity_detects_leakage_and_overlap(clean_layers):
    settings, _, curated = clean_layers
    encoded = encode(curated, settings)
    train, test = chronological_split(encoded, 0.8)
    X_train, y_train = feature_matrix(train, settings)
    X_test, y_test = feature_matrix(test, settings)
    clean = validate_model_input(settings, "t", train, test, X_train, X_test, y_train, y_test)
    assert clean.failures == []

    leaky_train, leaky_test = X_train.assign(accident=train["accident"].to_numpy()), X_test.assign(accident=0.0)
    leaky = validate_model_input(settings, "t", train, test, leaky_train, leaky_test, y_train, y_test)
    assert "model_input.no_target_leakage" in _failed(leaky)

    overlapping_test = pd.concat([test, train.tail(1)])
    X_overlap, y_overlap = feature_matrix(overlapping_test, settings)
    overlap = validate_model_input(settings, "t", train, overlapping_test, X_train, X_overlap, y_train, y_overlap)
    assert {"model_input.no_train_test_overlap", "model_input.train_before_test"} <= _failed(overlap)
