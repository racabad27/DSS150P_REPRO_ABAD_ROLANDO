"""DAG structure test (runs where Airflow is installed, e.g. inside the Airflow container).

    docker compose exec airflow python -m pytest tests/test_dag.py
"""

from __future__ import annotations

import importlib.util

import pytest

from src.config import PROJECT_ROOT

pytest.importorskip("airflow", reason="Airflow is only installed in the Airflow image")

EXPECTED_ORDER = [
    "ingest_source", "validate_raw", "transform_staging", "build_curated", "load_postgres",
    "validate_curated", "train_model", "evaluate_model", "reproducibility_check", "publish_outputs",
]


@pytest.fixture(scope="module")
def dag():
    path = PROJECT_ROOT / "dags" / "predictive_repro_pipeline.py"
    spec = importlib.util.spec_from_file_location("predictive_repro_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_dag_is_a_simple_chain_in_the_lab_order(dag):
    assert set(dag.task_ids) == set(EXPECTED_ORDER)
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:]):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_dag_scheduling_and_failure_settings(dag):
    assert dag.schedule is None or str(dag.schedule) in {"None", ""} or dag.timetable.summary == "None"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.get_task("load_postgres").retries == 3
    assert dag.get_task("reproducibility_check").retries == 0
    assert dag.get_task("train_model").execution_timeout is not None
    assert set(dag.params.keys()) >= {"ingest_mode", "max_batches", "fail_after_batches"}
