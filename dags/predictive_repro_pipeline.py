"""Airflow DAG for the reproducible fire-risk pipeline.

The DAG only orchestrates. Every task calls ONE stage of ``src/pipeline.py``
(the same functions the CLI uses), so there is a single copy of the
transformation and modelling logic in the repository.

    ingest_source -> validate_raw -> transform_staging -> build_curated -> load_postgres
      -> validate_curated -> train_model -> evaluate_model -> reproducibility_check -> publish_outputs

Scheduling: ``schedule=None`` (manual trigger). The source is a static,
checksum-pinned snapshot, so there is nothing new to process on a timetable;
a run is started when someone wants to rebuild or verify the data product.
``catchup=False`` means Airflow never back-fills old dates, and
``max_active_runs=1`` because all runs share the same local files and checkpoint.

One pipeline run id is used by every task of a DAG run: ``af_<airflow run id>``
(normalised for file names), so logs, files, run records and the PostgreSQL
audit rows of one DAG run can all be traced together.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG, Param

# The project is mounted at /opt/project in the Airflow container (see docker-compose.yml).
PROJECT_ROOT = os.environ.get("PIPELINE_PROJECT_ROOT", "/opt/project")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DAG_ID = "predictive_repro_pipeline"
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# helpers (imports of src.* happen inside the functions so the scheduler can
# parse this file quickly and without the heavy ML libraries)
# ---------------------------------------------------------------------------
def pipeline_run_id(context: dict) -> str:
    from src.observability import normalise_run_id

    return normalise_run_id(f"af_{context['dag_run'].run_id}")


def run_pipeline_stage(stage: str, **context: Any) -> dict:
    """Run one pipeline stage for this DAG run and return a small summary (XCom)."""
    from src.config import load_settings
    from src.pipeline import run_stage

    os.environ["PIPELINE_TRIGGER"] = "airflow"
    settings = load_settings()
    run_id = pipeline_run_id(context)
    params = context["params"]
    kwargs: dict[str, Any] = {}
    if stage == "ingest_source":
        first_try = context["ti"].try_number == 1
        kwargs = {
            # a retry continues from the checkpoint instead of restarting a full refresh
            "mode": params["ingest_mode"] if first_try else "incremental",
            "max_batches": params["max_batches"],
            # failure drill: only the FIRST try is interrupted, so the automatic
            # retry shows that ingestion resumes from the checkpoint
            "fail_after_batches": params["fail_after_batches"] if first_try else None,
        }
    summary = run_stage(stage, settings, run_id, **kwargs)
    log.info("stage %s finished for pipeline run %s", stage, run_id)
    return {k: v for k, v in summary.items() if isinstance(v, (str, int, float, bool)) or v is None}


def raw_layer_complete(**context: Any) -> bool:
    """validate_raw as a gate: blocking failures raise, a partial ingestion skips the rest."""
    from src.config import load_settings
    from src.pipeline import run_stage

    os.environ["PIPELINE_TRIGGER"] = "airflow"
    result = run_stage("validate_raw", load_settings(), pipeline_run_id(context))
    if not result["complete"]:
        log.warning("Not all arrival batches are ingested yet (max_batches was set); "
                    "downstream tasks are skipped until the raw layer is complete.")
    return bool(result["complete"])


def publish_and_finalize(**context: Any) -> dict:
    """Last task: deliver the outputs, then close the run record and sync the audit tables."""
    from src.config import load_settings
    from src.pipeline import finalize_run

    summary = run_pipeline_stage("publish_outputs", **context)
    finalize_run(load_settings(), pipeline_run_id(context), "success")
    return summary


def _report_problem(context: dict, final: bool) -> None:
    """Shared by the failure and retry callbacks. Must never raise."""
    try:
        from src.config import load_settings
        from src.pipeline import finalize_run
        from src.storage import record_error

        ti = context["ti"]
        error = context.get("exception") or RuntimeError("unknown error")
        run_id = pipeline_run_id(context)
        log.error("Task %s %s (try %s) in pipeline run %s: %s: %s", ti.task_id,
                  "FAILED" if final else "will be retried", ti.try_number, run_id, type(error).__name__, error)
        settings = load_settings()
        record_error(settings, run_id, ti.task_id, ti.try_number, error)
        if final:
            finalize_run(settings, run_id, "failed")
    except Exception as callback_error:  # noqa: BLE001
        log.error("failure callback could not record the error: %s", callback_error)


def on_task_failure(context: dict) -> None:
    _report_problem(context, final=True)


def on_task_retry(context: dict) -> None:
    _report_problem(context, final=False)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "dss150p",
    "retries": 2,                                # stages are idempotent, so retrying is safe
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

with DAG(
    dag_id=DAG_ID,
    description="Reproduce the IIoT fire-risk Predictive Analytics project as a data pipeline",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dss150p", "reproducibility", "fire-risk"],
    params={
        "ingest_mode": Param("incremental", type="string", enum=["incremental", "full"],
                             description="incremental = only new batches; full = full refresh of the raw layer"),
        "max_batches": Param(None, type=["null", "integer"], minimum=1,
                             description="ingest at most N new quarterly batches (simulates data arriving)"),
        "fail_after_batches": Param(None, type=["null", "integer"], minimum=1,
                                    description="failure drill: stop ingestion after N batches on the first try"),
    },
) as dag:
    ingest_source = PythonOperator(
        task_id="ingest_source", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "ingest_source"}, execution_timeout=timedelta(minutes=10))

    validate_raw = ShortCircuitOperator(
        task_id="validate_raw", python_callable=raw_layer_complete, retries=1)

    transform_staging = PythonOperator(
        task_id="transform_staging", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "transform_staging"}, execution_timeout=timedelta(minutes=10))

    build_curated = PythonOperator(
        task_id="build_curated", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "build_curated"}, execution_timeout=timedelta(minutes=10))

    load_postgres = PythonOperator(
        task_id="load_postgres", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "load_postgres"}, execution_timeout=timedelta(minutes=10),
        retries=3, retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=2))

    validate_curated = PythonOperator(
        task_id="validate_curated", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "validate_curated"}, retries=1)

    train_model = PythonOperator(
        task_id="train_model", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "train_model"}, retries=1, execution_timeout=timedelta(minutes=30))

    evaluate_model = PythonOperator(
        task_id="evaluate_model", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "evaluate_model"}, retries=1, execution_timeout=timedelta(minutes=45))

    reproducibility_check = PythonOperator(
        task_id="reproducibility_check", python_callable=run_pipeline_stage,
        op_kwargs={"stage": "reproducibility_check"},
        retries=0)                                # deterministic: a retry would give the same answer

    publish_outputs = PythonOperator(
        task_id="publish_outputs", python_callable=publish_and_finalize,
        execution_timeout=timedelta(minutes=10))

    (ingest_source >> validate_raw >> transform_staging >> build_curated >> load_postgres
     >> validate_curated >> train_model >> evaluate_model >> reproducibility_check >> publish_outputs)
