"""Evidence of Airflow runs: a success, a rerun, an interrupted run and a failed-then-recovered run.

The lab asks to "inspect and document at least one successful DAG run and one
intentionally failed/recovered run". This script does it the same way every
time. Run it INSIDE the Airflow container, where the Airflow CLI and REST API,
the project code and the PostgreSQL connection are all available:

    docker compose up -d --build
    docker compose exec airflow python scripts/airflow_evidence.py

It triggers four DAG runs one after another, waits for each one and writes
docs/evidence/airflow_runs.md (task states and tries, pipeline run records,
structured log lines and PostgreSQL audit rows).

  demo_1_first_run        normal run: every batch that is not ingested yet is ingested
  demo_2_rerun            the same DAG again: nothing new, 0 rows inserted/updated/deleted
  demo_3_interrupt_retry  ingest_mode=full and fail_after_batches=2: the first try stops
                          on purpose after two batches; Airflow retries and the retry
                          continues from the checkpoint instead of starting over
  demo_4_missing_source   the source CSV is hidden: ingest_source fails on every try and
                          nothing downstream runs. The file is put back, the failed tasks
                          are cleared (like the "Clear" button in the UI) and the SAME
                          DAG run then finishes successfully.

For the first run to really start from nothing, reset first (docs/runbook.md):
    docker compose down -v
    docker compose run --rm pipeline python -m src.cli clean --yes
    docker compose up -d
The script takes about 20 minutes. The source file is always put back, even if
the script is stopped with Ctrl+C.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import Settings, load_settings          # noqa: E402
from src.ingest import raw_status                       # noqa: E402
from src.observability import normalise_run_id          # noqa: E402
from src.storage import connect                         # noqa: E402

DAG_ID = "predictive_repro_pipeline"
API_URL = os.environ.get("AIRFLOW_EVIDENCE_API_URL", "http://localhost:8080")
OUTPUT = ROOT / "docs" / "evidence" / "airflow_runs.md"
TASK_ORDER = ["ingest_source", "validate_raw", "transform_staging", "build_curated", "load_postgres",
              "validate_curated", "train_model", "evaluate_model", "reproducibility_check", "publish_outputs"]
# outputs that must stay byte-identical across all four runs (they are committed to Git)
WATCHED_OUTPUTS = ["outputs/metrics.json", "outputs/predictions.csv", "outputs/feature_importance.csv",
                   "metadata/split_manifest.json"]


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
class AirflowApi:
    """Tiny client for the Airflow 3 REST API (the same API the web UI uses)."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        # local setup: SIMPLE_AUTH_MANAGER_ALL_ADMINS=true hands out a token without a password
        with urllib.request.urlopen(f"{self.base_url}/auth/token", timeout=30) as response:
            self.token = json.load(response)["access_token"]

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        request = urllib.request.Request(
            f"{self.base_url}/api/v2{path}", method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def dag_run(self, run_id: str) -> dict | None:
        try:
            return self.call("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

    def task_instances(self, run_id: str) -> list[dict]:
        items = self.call("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances?limit=100")
        return sorted(items["task_instances"], key=lambda ti: TASK_ORDER.index(ti["task_id"]))

    def clear_failed_tasks(self, run_id: str) -> list[str]:
        """Same as selecting the run in the UI and pressing Clear -> 'only failed'."""
        body = {"dry_run": False, "dag_run_id": run_id, "only_failed": True, "reset_dag_runs": True}
        result = self.call("POST", f"/dags/{DAG_ID}/clearTaskInstances", body)
        return [ti["task_id"] for ti in result.get("task_instances", [])]


def airflow_cli(*args: str) -> str:
    done = subprocess.run(["airflow", *args], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"`airflow {' '.join(args)}` failed:\n{done.stderr[-2000:]}")
    return done.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "missing"


def md_table(headers: list[str], rows: list[list]) -> list[str]:
    def cell(value) -> str:
        return "" if value is None else " ".join(str(value).split()).replace("|", "\\|")
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return lines


def query(settings: Settings, sql: str, params: tuple = ()) -> tuple[list[str], list[list]]:
    with connect(settings, attempts=3) as conn:
        cursor = conn.execute(sql, params)
        return [c.name for c in cursor.description], [list(row) for row in cursor.fetchall()]


def pipeline_run_id(dag_run_id: str) -> str:
    """The id every task of a DAG run uses for files, logs and audit rows (see the DAG)."""
    return normalise_run_id(f"af_{dag_run_id}")


def run_record(settings: Settings, dag_run_id: str) -> dict:
    path = settings.runs_dir / f"{pipeline_run_id(dag_run_id)}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def load_changes(record: dict) -> dict[str, tuple]:
    """(inserted, updated, deleted) per table from the load_postgres stage of a run record."""
    details = record.get("stages", {}).get("load_postgres", {}).get("details", {})
    return {table: (d["inserted"], d["updated"], d["deleted"]) for table, d in details.items()
            if isinstance(d, dict) and "inserted" in d}


def log_lines(settings: Settings, dag_run_id: str, stage: str = "ingest_source") -> list[str]:
    """Lines of logs/pipeline.jsonl (our structured log) for one run and stage."""
    path = settings.logs_dir / "pipeline.jsonl"
    wanted = pipeline_run_id(dag_run_id)
    lines = []
    for raw_line in (path.read_text(encoding="utf-8").splitlines() if path.is_file() else []):
        try:
            entry = json.loads(raw_line)
        except ValueError:
            continue
        if entry.get("run_id") == wanted and entry.get("stage") == stage:
            text = f"{entry['ts'][11:23]}Z {entry['level']:<7} {entry['message'].splitlines()[0]}"
            if entry.get("error"):
                text += f"   <- {entry['error']['type']}"
            lines.append(text)
    return lines


@dataclass
class Scenario:
    name: str
    title: str
    purpose: str
    conf: dict
    run_id: str = ""
    states: list[str] = field(default_factory=list)      # DAG run state(s) observed
    tasks_before_fix: list[dict] = field(default_factory=list)
    tasks: list[dict] = field(default_factory=list)
    checks: dict[str, bool] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# running the scenarios
# ---------------------------------------------------------------------------
def unique_run_id(api: AirflowApi, name: str) -> str:
    if api.dag_run(name) is None:
        return name
    return f"{name}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"   # the script was run before


def trigger_and_wait(api: AirflowApi, scenario: Scenario, timeout_min: int = 45) -> str:
    print(f"-> triggering {scenario.run_id} conf={json.dumps(scenario.conf)}", flush=True)
    airflow_cli("dags", "trigger", DAG_ID, "--run-id", scenario.run_id, "--conf", json.dumps(scenario.conf))
    return wait_for(api, scenario.run_id, timeout_min)


def wait_for(api: AirflowApi, run_id: str, timeout_min: int = 45) -> str:
    deadline = time.monotonic() + timeout_min * 60
    while time.monotonic() < deadline:
        state = (api.dag_run(run_id) or {}).get("state")
        if state in ("success", "failed"):
            print(f"   {run_id}: {state}", flush=True)
            return state
        time.sleep(10)
    raise TimeoutError(f"DAG run {run_id} did not finish within {timeout_min} minutes")


def by_task(tasks: list[dict]) -> dict[str, dict]:
    return {ti["task_id"]: ti for ti in tasks}


def errors_for(settings: Settings, dag_run_id: str) -> list[list]:
    _, rows = query(settings, "SELECT stage, try_number, error_type, left(error_message, 80) "
                              "FROM audit.pipeline_errors WHERE pipeline_run_id = %s ORDER BY error_id",
                    (pipeline_run_id(dag_run_id),))
    return rows


def main() -> int:
    settings = load_settings()
    api = AirflowApi(API_URL)
    airflow_cli("dags", "unpause", DAG_ID)
    started = datetime.now(timezone.utc)
    hashes_before = {name: sha256(ROOT / name) for name in WATCHED_OUTPUTS}
    raw_before = raw_status(settings)

    s1 = Scenario("demo_1_first_run", "1. First run", "Normal run with the default parameters "
                  "(incremental ingestion of every batch that is not committed yet).", {})
    s2 = Scenario("demo_2_rerun", "2. Second run (rerun safety)", "The same DAG again with the same "
                  "source. Nothing new may be ingested and the database load must change 0 rows.", {})
    s3 = Scenario("demo_3_interrupt_retry", "3. Interrupted ingestion, recovered by the automatic retry",
                  "Full refresh with a failure drill: the first try of ingest_source stops on purpose after "
                  "two committed batches. Airflow retries the task (retries=2, 20 s delay) and the retry "
                  "continues from the checkpoint.", {"ingest_mode": "full", "fail_after_batches": 2})
    s4 = Scenario("demo_4_missing_source", "4. Failed run, fixed and recovered",
                  "The source CSV is moved away before the run. ingest_source fails on all three tries, "
                  "so nothing downstream runs and nothing is published. Then the file is put back and only "
                  "the failed tasks of the SAME DAG run are cleared, so the run continues to the end.", {})

    # 1 and 2 ------------------------------------------------------------------------------
    for scenario in (s1, s2):
        scenario.run_id = unique_run_id(api, scenario.name)
        scenario.states.append(trigger_and_wait(api, scenario))
        scenario.tasks = api.task_instances(scenario.run_id)

    # 3 ------------------------------------------------------------------------------------
    s3.run_id = unique_run_id(api, s3.name)
    s3.states.append(trigger_and_wait(api, s3))
    s3.tasks = api.task_instances(s3.run_id)

    # 4 ------------------------------------------------------------------------------------
    s4.run_id = unique_run_id(api, s4.name)
    source = settings.source_file
    hidden = source.with_name(source.name + ".hidden_for_drill")
    try:
        os.replace(source, hidden)
        print(f"   source hidden as {hidden.name}", flush=True)
        s4.states.append(trigger_and_wait(api, s4))
        s4.tasks_before_fix = api.task_instances(s4.run_id)
        hashes_after_failure = {name: sha256(ROOT / name) for name in WATCHED_OUTPUTS}
    finally:
        if hidden.exists() and not source.exists():
            os.replace(hidden, source)
            print("   source file put back", flush=True)
    cleared = api.clear_failed_tasks(s4.run_id)
    s4.notes.append(f"Cleared {len(cleared)} task instance(s) of the same DAG run: {', '.join(cleared)}.")
    print(f"   cleared {len(cleared)} failed task(s) of {s4.run_id}", flush=True)
    s4.states.append(wait_for(api, s4.run_id))
    s4.tasks = api.task_instances(s4.run_id)

    # checks ---------------------------------------------------------------------------------
    for scenario in (s1, s2, s3, s4):
        changes = load_changes(run_record(settings, scenario.run_id))
        scenario.notes.append("load_postgres result per table (inserted, updated, deleted): "
                              + ", ".join(f"`{table}` {value}" for table, value in changes.items()))
    for scenario in (s1, s2, s3):
        scenario.checks["DAG run finished with state success"] = scenario.states[-1] == "success"
        scenario.checks["all 10 tasks succeeded"] = all(ti["state"] == "success" for ti in scenario.tasks)

    record2 = run_record(settings, s2.run_id)
    ingest2 = record2.get("stages", {}).get("ingest_source", {}).get("details", {})
    changed2 = load_changes(record2)
    s2.checks["no new batch was ingested (0 written)"] = ingest2.get("batches_written_this_run") == 0
    s2.checks["database load inserted/updated/deleted 0 rows in every table"] = (
        bool(changed2) and all(v == (0, 0, 0) for v in changed2.values()))

    record3 = run_record(settings, s3.run_id)
    ingest3 = record3.get("stages", {}).get("ingest_source", {})
    errors3 = errors_for(settings, s3.run_id)
    s3.checks["ingest_source needed 2 tries"] = by_task(s3.tasks)["ingest_source"]["try_number"] == 2
    s3.checks["the first try was recorded in audit.pipeline_errors (SimulatedInterruption)"] = any(
        row[0] == "ingest_source" and row[2] == "SimulatedInterruption" for row in errors3)
    batches_total = ingest3.get("details", {}).get("batches_in_source")
    written_by_retry = ingest3.get("details", {}).get("batches_written_this_run")
    s3.checks["the retry wrote only the remaining batches (total - 2)"] = (
        batches_total is not None and written_by_retry == batches_total - 2)
    changed3 = load_changes(record3)
    s3.checks["after the full refresh the load changed 0 rows (staging and curated rebuilt identically)"] = (
        bool(changed3) and all(v == (0, 0, 0) for v in changed3.values()))
    s3.notes.append(f"Run record: ingest_source attempts = {ingest3.get('attempts')}, batches written by the "
                    f"retry = {written_by_retry} of {batches_total}.")

    first = by_task(s4.tasks_before_fix)
    errors4 = errors_for(settings, s4.run_id)
    record4 = run_record(settings, s4.run_id)
    s4.checks["the run failed before the fix"] = s4.states[0] == "failed"
    s4.checks["ingest_source failed on all 3 tries"] = (first["ingest_source"]["state"] == "failed"
                                                         and first["ingest_source"]["try_number"] == 3)
    s4.checks["no downstream task ran (all upstream_failed)"] = all(
        first[t]["state"] == "upstream_failed" for t in TASK_ORDER[1:])
    s4.checks["published outputs unchanged while the run was broken"] = hashes_after_failure == hashes_before
    s4.checks["3 errors recorded in audit.pipeline_errors (SourceVerificationError)"] = (
        sum(row[2] == "SourceVerificationError" for row in errors4) == 3)
    s4.checks["after the fix the SAME DAG run succeeded"] = s4.states[-1] == "success" and all(
        ti["state"] == "success" for ti in s4.tasks)
    s4.checks["pipeline run record ends with status success"] = record4.get("status") == "success"
    s4.notes.append(f"Run record: ingest_source attempts = "
                    f"{record4.get('stages', {}).get('ingest_source', {}).get('attempts')} "
                    "(3 failed tries + 1 after the fix), same pipeline_run_id throughout.")

    raw_after = raw_status(settings)
    duplicate_cols, duplicate_rows = query(settings, "SELECT * FROM audit.v_duplicate_check ORDER BY table_name")
    hashes_after = {name: sha256(ROOT / name) for name in WATCHED_OUTPUTS}
    overall = {
        "raw layer: 0 duplicate source_row_keys after all runs": raw_after["duplicate_source_row_keys"] == 0,
        "PostgreSQL: 0 duplicate business keys (reading_ts) in staging and curated": all(
            row[2] == 0 for row in duplicate_rows),
        "metrics.json, predictions.csv, feature_importance.csv and split_manifest.json byte-identical "
        "before and after the four runs": hashes_after == hashes_before,
    }

    # report ---------------------------------------------------------------------------------
    scenarios = [s1, s2, s3, s4]
    all_ok = all(all(s.checks.values()) for s in scenarios) and all(overall.values())
    version = airflow_cli("version").strip().splitlines()[-1]
    lines = [
        "# Evidence: Airflow DAG runs", "",
        f"Generated by `docker compose exec airflow python scripts/airflow_evidence.py` on "
        f"{started:%Y-%m-%d %H:%M} UTC (Airflow {version}, LocalExecutor, Docker Compose). "
        "All times are UTC. Every DAG run uses one pipeline run id, `af_<DAG run id>`, for its files, "
        "log lines and audit rows.", "",
        f"Raw layer before the first run: {raw_before['committed_batches']} committed batches, "
        f"{raw_before['raw_rows']:,} rows.", "",
        "## Summary", "",
    ]
    lines += md_table(["DAG run", "conf", "state", "what it shows", "checks"],
                      [[f"`{s.run_id}`", f"`{json.dumps(s.conf)}`" if s.conf else "defaults",
                        " → ".join(s.states), s.title.split(". ", 1)[1],
                        f"{sum(s.checks.values())}/{len(s.checks)} ✔"] for s in scenarios])
    lines += ["", "Dependency chain (from the DAG, checked by `tests/test_dag.py`):", "",
              "`" + " → ".join(TASK_ORDER) + "`", ""]

    for s in scenarios:
        record = run_record(settings, s.run_id)
        conf = f" --conf '{json.dumps(s.conf)}'" if s.conf else ""
        lines += [f"## {s.title}", "", s.purpose, "",
                  f"Triggered with `airflow dags trigger {DAG_ID} --run-id {s.run_id}{conf}`", "",
                  f"DAG run `{s.run_id}` · pipeline run id `{pipeline_run_id(s.run_id)}` · "
                  f"final pipeline status **{record.get('status', '?')}**", ""]
        if s.tasks_before_fix:
            lines += ["Task states **before** the fix:", ""]
            lines += md_table(["task", "state", "tries"],
                              [[ti["task_id"], ti["state"], ti["try_number"]] for ti in s.tasks_before_fix])
            lines += ["", "Task states **after** putting the file back and clearing the failed tasks:", ""]
        lines += md_table(["task", "state", "tries", "duration (s)"],
                          [[ti["task_id"], ti["state"], ti["try_number"],
                            f"{ti['duration']:.1f}" if ti.get("duration") is not None else ""]
                           for ti in s.tasks])
        if s in (s3, s4):
            lines += ["", "Structured log of `ingest_source` (`logs/pipeline.jsonl`, all tries):", "", "```text"]
            lines += log_lines(settings, s.run_id) or ["(no log lines found)"]
            lines += ["```", "", "`audit.pipeline_errors` rows of this run:", ""]
            lines += md_table(["stage", "try", "error type", "message (first 80 characters)"],
                              errors_for(settings, s.run_id))
        lines += [""] + [f"- {note}" for note in s.notes]
        lines += [f"- {'✔' if ok else '✘'} {name}" for name, ok in s.checks.items()] + [""]

    run_ids = [pipeline_run_id(s.run_id) for s in scenarios]
    overview_cols, overview_rows = query(
        settings, "SELECT pipeline_run_id, status, trigger, duration_s, raw_rows, staging_rows, curated_rows, "
                  "train_rows, test_rows, prediction_rows, failed_checks FROM audit.v_run_overview "
                  "WHERE pipeline_run_id = ANY(%s) ORDER BY started_at_utc", (run_ids,))
    lines += ["## PostgreSQL after the four runs", "", "`SELECT ... FROM audit.v_run_overview`:", ""]
    lines += md_table(overview_cols, overview_rows)
    lines += ["", "`SELECT * FROM audit.v_duplicate_check`:", ""]
    lines += md_table(duplicate_cols, duplicate_rows)
    lines += ["", "## Overall", ""]
    lines += [f"- {'✔' if ok else '✘'} {name}" for name, ok in overall.items()]
    lines += ["", "SHA-256 of the watched outputs after the runs:", ""]
    lines += md_table(["file", "sha256 (first 16)", "unchanged"],
                      [[f"`{n}`", hashes_after[n][:16], "yes" if hashes_after[n] == hashes_before[n] else "NO"]
                       for n in WATCHED_OUTPUTS])
    lines += ["", "## Screenshots", "",
              "Taken in the Airflow UI (http://localhost:8080) after the script finished.", "",
              "Graph view - the ten tasks and their dependencies:", "", "![Graph view](airflow_graph.png)", "",
              "Grid view - the four demo runs, one column per run:", "", "![Grid view](airflow_grid.png)", "",
              f"`ingest_source` in `{s4.run_id}` - tries 1-3 failed (source missing), try 4 succeeded "
              "after the fix:", "", "![Task tries](airflow_task_tries.png)", ""]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(ROOT)} - {'all checks passed' if all_ok else 'SOME CHECKS FAILED'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
