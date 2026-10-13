"""Integration tests: several modules working together across file/database boundaries."""

from __future__ import annotations

import json
import subprocess
import sys

import pandas as pd
import pytest

from src.config import PROJECT_ROOT
from src.errors import DataQualityError, ReproducibilityError, SimulatedInterruption
from src.ingest import Checkpoint, raw_status
from src.pipeline import run_stage
from src.transform import curated_path, staging_path
from tests.conftest import build_workspace, make_sample_frame

pytestmark = pytest.mark.integration
FILE_STAGES = ("ingest_source", "validate_raw", "transform_staging", "build_curated")


def test_interrupted_ingestion_resumes_without_duplicates(workspace):
    with pytest.raises(SimulatedInterruption):
        run_stage("ingest_source", workspace, "run_a", fail_after_batches=1)
    state = Checkpoint(workspace.checkpoint_file).load()
    assert list(state["committed_batches"]) == ["2021-01"]          # only the first batch counts

    summary = run_stage("ingest_source", workspace, "run_b")           # same command again
    assert summary["batches_written_this_run"] == 2                  # resumed at 2021-02
    status = raw_status(workspace)
    assert status["raw_rows"] == summary["source_rows"] == 8640
    assert status["duplicate_source_row_keys"] == 0

    again = run_stage("ingest_source", workspace, "run_c")             # nothing pending
    assert again["batches_written_this_run"] == 0
    assert raw_status(workspace)["raw_rows"] == 8640


def test_full_refresh_rebuilds_the_same_raw_layer(workspace):
    run_stage("ingest_source", workspace, "run_a")
    before = raw_status(workspace)
    run_stage("ingest_source", workspace, "run_b", mode="full")
    after = raw_status(workspace)
    assert before["rows_per_batch"] == after["rows_per_batch"]
    assert after["duplicate_record_hashes"] == 0


def _expected_from(settings) -> None:
    """Write an expected_metrics.json whose baseline is the first run's results."""
    metrics = json.loads((settings.outputs_dir / "metrics.json").read_text())
    split = json.loads(settings.file_setting("reproducibility.split_manifest_file").read_text())
    checks = [{"id": f"test.{m}.{k}", "source": "first run", "model": m, "kind": "test_metric", "key": k,
               "baseline": v, "tolerance": 0.0005, "blocking": True, "primary": (m, k) == ("random_forest", "rmse")}
              for m, values in metrics["test"].items() for k, v in values.items()]
    baseline_rows = {"n_train": split["n_train"], "n_test": split["n_test"],
                     "train_start": split["train_period"]["start"], "train_end": split["train_period"]["end"],
                     "test_start": split["test_period"]["start"], "test_end": split["test_period"]["end"],
                     "train_keys_sha256": split["train_keys_sha256"], "test_keys_sha256": split["test_keys_sha256"]}
    expected = {"baseline_id": "integration-test", "tolerance_policy": {}, "checks": checks,
                "split": {"baseline_rows": baseline_rows}}
    settings.file_setting("reproducibility.expected_metrics_file").write_text(json.dumps(expected))


def test_pipeline_is_deterministic_and_the_reproducibility_gate_works(workspace):
    for stage in FILE_STAGES:
        run_stage(stage, workspace, "run_1")
    run_stage("validate_curated", workspace, "run_1", check_database=False)
    run_stage("train_model", workspace, "run_1")
    run_stage("evaluate_model", workspace, "run_1")
    first_metrics = (workspace.outputs_dir / "metrics.json").read_bytes()
    first_split = workspace.file_setting("reproducibility.split_manifest_file").read_bytes()
    first_predictions = (workspace.outputs_dir / "predictions.csv").read_bytes()

    _expected_from(workspace)
    report = run_stage("reproducibility_check", workspace, "run_1")
    assert report["status"] == "PASS" and report["split_equivalence"]["equivalent"]

    # second full rebuild of the same data -> byte-identical model outputs
    for stage in FILE_STAGES:
        run_stage(stage, workspace, "run_2")
    run_stage("train_model", workspace, "run_2")
    run_stage("evaluate_model", workspace, "run_2")
    assert (workspace.outputs_dir / "metrics.json").read_bytes() == first_metrics
    assert workspace.file_setting("reproducibility.split_manifest_file").read_bytes() == first_split
    assert (workspace.outputs_dir / "predictions.csv").read_bytes() == first_predictions

    # a materially different baseline must make the gate fail
    path = workspace.file_setting("reproducibility.expected_metrics_file")
    expected = json.loads(path.read_text())
    expected["checks"][0]["baseline"] += 1.0
    path.write_text(json.dumps(expected))
    with pytest.raises(ReproducibilityError):
        run_stage("reproducibility_check", workspace, "run_2")


def test_missing_column_is_stopped_before_staging(tmp_path):
    frame = make_sample_frame().drop(columns=["Pressure"])
    settings = build_workspace(tmp_path, frame=frame)
    run_stage("ingest_source", settings, "bad_run")             # raw keeps whatever arrived
    with pytest.raises(DataQualityError, match="schema_columns"):
        run_stage("validate_raw", settings, "bad_run")
    assert not staging_path(settings).exists()                 # nothing reached staging
    assert not curated_path(settings).exists()


def test_too_many_invalid_rows_publish_nothing(tmp_path):
    frame = make_sample_frame()
    frame.loc[frame.index[:200], "Factory"] = "Factory_Z"      # ~2.3% invalid > 1% threshold
    settings = build_workspace(tmp_path, frame=frame)
    run_stage("ingest_source", settings, "bad_run")
    run_stage("validate_raw", settings, "bad_run")
    with pytest.raises(DataQualityError, match="quarantine_threshold"):
        run_stage("transform_staging", settings, "bad_run")
    assert not staging_path(settings).exists()
    rejected = pd.read_csv(settings.quarantine_dir / "rejected_rows.csv")
    assert len(rejected) == 200 and set(rejected["reason_code"]) == {"INVALID_CATEGORY"}


@pytest.mark.db
def test_postgres_load_is_rerun_safe(pg_workspace):
    for stage in FILE_STAGES:
        run_stage(stage, pg_workspace, "db_run")
    first = run_stage("load_postgres", pg_workspace, "db_run")
    second = run_stage("load_postgres", pg_workspace, "db_run_again")   # e.g. an Airflow retry
    for table in second["tables"]:
        assert (table["inserted"], table["updated"], table["deleted"]) == (0, 0, 0)
        assert table["unchanged"] == table["snapshot_rows"]
    assert first["tables"][1]["inserted"] == len(pd.read_parquet(curated_path(pg_workspace)))
    report = run_stage("validate_curated", pg_workspace, "db_run")       # file <-> database reconciliation
    assert report["quality"]["failed"] == 0


def test_cli_entry_point_runs():
    result = subprocess.run([sys.executable, "-m", "src.cli", "--help"], cwd=PROJECT_ROOT,
                            capture_output=True, text=True)
    assert result.returncode == 0
    assert "run-all" in result.stdout
