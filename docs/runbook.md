# Runbook — DSS150P fire-risk pipeline

This runbook is for whoever has to run, rerun or repair the pipeline (me, a
classmate or the instructor). Every command runs from the repository root.
The Docker commands are the main path. In a local virtual environment the same
`python -m src.cli ...` commands work without the
`docker compose run --rm pipeline` prefix.

## 0. Quick reference

| What | Where / how |
|---|---|
| Start the services | `docker compose up -d` (PostgreSQL + Airflow) |
| Airflow UI | http://localhost:8080 (the port is `AIRFLOW_HOST_PORT` in `.env`) |
| Whole pipeline without Airflow | `docker compose run --rm pipeline` |
| One pipeline command | `docker compose run --rm pipeline python -m src.cli <command>` |
| SQL shell | `docker compose exec postgres psql -U firerisk -d firerisk` |
| Pipeline log (one JSON object per line) | `logs/pipeline.jsonl` |
| Airflow task logs | `logs/airflow/dag_id=predictive_repro_pipeline/run_id=<run>/task_id=<task>/attempt=<n>.log` |
| Record of one run | `outputs/runs/<pipeline_run_id>.json` (status, counts, every stage with attempts) |
| Summary of the latest run | `outputs/run_summary.json` |
| Data-quality results | `outputs/quality/latest_<layer>.json` and `<run_id>_<layer>.json` |
| Rejected rows | `data/quarantine/rejected_rows.csv` (with `reason_code`) |
| Ingestion checkpoint | `state/ingestion_checkpoint.json` (`python -m src.cli status` prints it) |
| Run history in SQL | `audit.v_run_overview`, `audit.stage_runs`, `audit.pipeline_errors`, `audit.data_quality_results` |

**Run ids.** A CLI run gets `run_<UTC time>_<6 hex>` (or the value of
`--run-id`). An Airflow run uses `af_<DAG run id>` in every task. The same id
appears in the log lines, the run record, the quality reports, the audit
columns of the loaded rows and the audit tables, so one run can be traced
end to end.

## 1. Normal execution

### 1.1 With Airflow (the main way)

1. `docker compose up -d`, then wait until `docker compose ps` shows
   `postgres` and `airflow` as **healthy** (about a minute the first time).
2. Open the Airflow UI, open the DAG `predictive_repro_pipeline` and press
   **Trigger**. Or from a terminal:

   ```bash
   docker compose exec airflow airflow dags trigger predictive_repro_pipeline
   ```

3. Follow the run in the **Grid** view. All ten tasks should turn green in
   about 4–5 minutes. Most of that time is `evaluate_model`, which runs the
   5-fold cross-validation of three models.

Runtime parameters (Trigger → *Run with config*, or `--conf '{...}'`):

| Parameter | Default | Use |
|---|---|---|
| `ingest_mode` | `incremental` | `full` rebuilds the raw layer and the checkpoint from scratch |
| `max_batches` | none | ingest at most N new quarterly batches ("data arriving"); the rest of the DAG is skipped until the raw layer is complete |
| `fail_after_batches` | none | failure drill: the first try of `ingest_source` stops after N batches and the automatic retry resumes |

The DAG has `schedule=None` on purpose. The source is a frozen, checksum-pinned
snapshot, so nothing new appears on a timetable. A run is started when someone
wants to rebuild or verify the data product. `catchup=False` and
`max_active_runs=1` stop old dates from being back-filled and two runs from
writing the same files at the same time.

### 1.2 Without Airflow

```bash
docker compose run --rm pipeline                                   # run-all, incremental
docker compose run --rm pipeline python -m src.cli run-all --mode full
python -m src.cli run-all --skip-db                                # local venv, no PostgreSQL
```

Single stages, in pipeline order: `ingest`, `validate --layer raw`,
`transform`, `build-curated`, `load-postgres`, `validate --layer curated`,
`train`, `evaluate`, `repro-check`, `publish`. Every stage reads the
published files of the stage before it, so after fixing a problem you can
continue from the stage that failed.

### 1.3 How to tell that a run is healthy

| Check | Expected value |
|---|---|
| Exit code / DAG state | `0` / `success` |
| Raw | 100,000 rows in 12 quarterly batches, 0 duplicate `source_row_key` |
| Staging | 100,000 rows, 0 quarantined |
| Curated | 98,000 rows (2,000 rows without `Workers` are listed in `data/curated/curated_exclusions.csv`) |
| Model input | 78,400 train rows, 19,600 test rows, split at 2023-04-14 05:15 |
| Predictions | 19,600 test rows in `outputs/predictions.csv`, 58,800 rows (3 models) in `serving.risk_predictions` |
| Reproducibility | `PASS`: 130 passed, 0 failed, 26 accepted deviations; Random Forest RMSE 3.235651 vs 3.24 |
| Duplicates | `SELECT * FROM audit.v_duplicate_check;` shows 0 for both tables |

```bash
docker compose run --rm pipeline python -m src.cli status
docker compose exec postgres psql -U firerisk -d firerisk -c "SELECT * FROM audit.v_run_overview ORDER BY started_at_utc DESC LIMIT 5;"
docker compose exec postgres psql -U firerisk -d firerisk -c "SELECT * FROM audit.v_duplicate_check;"
```

A quick reproducibility check with Git: after a successful run, `git status`
should only list `outputs/run_summary.json`, `outputs/reproducibility_check.json`
and `outputs/reproducibility_summary.md` as modified, because they contain the
run id and time. If `outputs/metrics.json`, `outputs/predictions.csv` or
`metadata/split_manifest.json` show up as modified, something in the data,
code or environment is different. Start with section 3.

## 2. Exit codes

The CLI returns a specific exit code for each kind of problem. In Airflow the
same error makes the task fail; the message is in the task log and in
`audit.pipeline_errors`.

| Code | Error | Meaning | First thing to do |
|---|---|---|---|
| 0 | — | success | — |
| 1 | unexpected error | a bug or an unplanned situation | read the traceback in `logs/pipeline.jsonl` or the task log |
| 2 | `ConfigError` | a setting in `config/settings.yml` or `.env` is invalid | fix the value named in the message |
| 3 | `SourceVerificationError` | the source file is missing or is not the file in the manifest | section 3.1 |
| 4 | `DataQualityError` | a blocking data-quality check failed; nothing was published | section 3.3 |
| 5 | `DatabaseError` | PostgreSQL could not be reached after 5 attempts | section 3.2 |
| 6 | `ReproducibilityError` | metrics are outside the tolerance or the split changed | section 3.4 |
| 75 | `SimulatedInterruption` | only the failure drill uses this | rerun the same command; it resumes |
| 130 | — | stopped with Ctrl+C | rerun the same command |

## 3. Common failures

### 3.1 Source problems (exit 3)

| Message | Cause | Fix |
|---|---|---|
| `Source file not found` | the CSV is not in `data/source/` | download it as described in `data/source/README.md`, then `python -m src.cli verify-source` |
| `... does not match the source manifest` | a different file or a modified copy, for example one opened and saved in Excel | get the original file again. **Do not** edit the checksum in the manifest to make the error go away, because that changes the identity of the dataset |
| `The source file changed since the last checkpoint` | the raw layer was built from a different (verified) file | if the new file is intentional, rebuild with `ingest --mode full` (Airflow: `ingest_mode=full`); otherwise restore the original file |

A new version of the dataset is never a silent change. It needs a new
manifest and contract checksum and a new baseline. Only after that should
anyone run a full refresh.

### 3.2 Database and Docker problems (exit 5 and start-up errors)

| Symptom | Cause | Fix |
|---|---|---|
| `Set POSTGRES_PASSWORD in .env` when running `docker compose` | there is no `.env` file | `cp .env.example .env` (PowerShell: `Copy-Item .env.example .env`) |
| `PostgreSQL not reachable ... after 5 attempts` | the database container is down, or local runs use the wrong host/port | `docker compose ps`, `docker compose logs postgres`. A local venv run needs `POSTGRES_HOST=localhost` and `POSTGRES_PORT` = the host port from `.env` |
| `password authentication failed` | `POSTGRES_PASSWORD` was changed after the first start, but the database volume keeps the old password | put the old value back, or reset the database (section 6) |
| `port is already allocated` | another program already uses 5432 or 8080 | set `POSTGRES_HOST_PORT` / `AIRFLOW_HOST_PORT` in `.env`, then `docker compose up -d` |
| `PermissionError` on `data/`, `outputs/` or `logs/` (Linux/WSL) | the containers write as a different user | set `AIRFLOW_UID` in `.env` to the output of `id -u`, run `docker compose up -d --force-recreate`, and fix old files once with `sudo chown -R $(id -u) data outputs state logs metadata` |
| a task or container ends with exit code 137 | Docker ran out of memory during training | give Docker Desktop at least 4 GB of memory |

### 3.3 Data-quality failures (exit 4)

The pipeline uses *write-audit-publish*: a new staging or curated file is
written to a temporary name, checked, and only then renamed over the
published file. When a blocking check fails, the previously published files
stay in place and the model never sees the bad data.

1. Open `outputs/quality/latest_<layer>.json`. Every failed check shows what
   was observed and what was expected.
2. If rows were quarantined, `data/quarantine/rejected_rows.csv` lists each
   row with its raw values and a `reason_code` (`INVALID_CATEGORY`,
   `OUT_OF_RANGE`, `INVALID_TIMESTAMP`, ...).
3. Fix the cause (usually the source file or a setting) and rerun. More than
   1% quarantined rows (`staging.max_quarantine_rate`) always stops the run.

A special case is `checkpoint lists batch ... but ... is missing`: someone
deleted raw files by hand. See section 5.

### 3.4 Reproducibility failures (exit 6)

1. `python -m src.cli env-check --strict` checks that every package matches
   `requirements.txt`. scikit-learn 1.9 and xgboost 3.3 are known to change
   the results; see `docs/evidence/dependency_sweep.md`.
2. In `outputs/reproducibility_check.json`, look at `split_equivalence`. If the
   train/test keys differ from the baseline, the data or the split changed,
   not the model.
3. Run `git diff metadata/expected_metrics.json config/settings.yml`. The
   baseline and the model settings must not be edited to make a check pass.

### 3.5 Airflow problems

| Symptom | Fix |
|---|---|
| The DAG is not in the list | `docker compose exec airflow airflow dags list-import-errors` |
| The DAG is paused | `docker compose exec airflow airflow dags unpause predictive_repro_pipeline` |
| A run stays *queued* | another run is still active (`max_active_runs=1`), or the scheduler stopped: `docker compose logs airflow` |
| `validate_raw` is skipped (pink) and everything after it too | the run used `max_batches`, so the raw layer is not complete yet. Trigger again without it |

## 4. Restart and rerun procedures

### 4.1 Why rerunning is always safe

| Stage | What a rerun does |
|---|---|
| `ingest_source` | skips every batch already in the checkpoint; nothing is appended twice. `--mode full` deletes and rebuilds the raw layer |
| `transform_staging`, `build_curated` | rebuild the layer from the complete raw layer and replace the published file in one atomic step |
| `load_postgres` | merges a snapshot on the business key: changed rows are updated, new rows inserted, rows that disappeared deleted, identical rows left alone. A second run reports 0 inserted / 0 updated / 0 deleted |
| `train_model`, `evaluate_model` | deterministic (fixed seeds, fixed order, single-threaded prediction): the same files byte for byte |
| `publish_outputs` | upserts on `(model_name, source_row_key)` and `(run, model, set, metric)` |

### 4.2 Rerun everything

Trigger the DAG again, or `docker compose run --rm pipeline`. There is no
need to clean anything first.

### 4.3 Rerun from the stage that failed

After fixing the cause, run the failed stage and the ones after it. To keep
the manual commands under one run id, pass the same `--run-id`:

```bash
docker compose run --rm pipeline python -m src.cli load-postgres --run-id fix_2026_10_12
docker compose run --rm pipeline python -m src.cli validate --layer curated --run-id fix_2026_10_12
docker compose run --rm pipeline python -m src.cli train --run-id fix_2026_10_12
docker compose run --rm pipeline python -m src.cli evaluate --run-id fix_2026_10_12
docker compose run --rm pipeline python -m src.cli repro-check --run-id fix_2026_10_12
docker compose run --rm pipeline python -m src.cli publish --run-id fix_2026_10_12
```

`run-all` is usually just as quick, because ingestion skips everything that is
already committed.

### 4.4 In Airflow: retry, clear, re-trigger

* **Automatic retries.** `ingest_source`, `transform_staging`,
  `build_curated` and `publish_outputs` retry twice after 20 seconds; the
  validation, training and evaluation tasks retry once; `load_postgres`
  retries 3 times with growing waits (up to 2 minutes);
  `reproducibility_check` never retries, because the same inputs give the same
  answer. Every retry is logged and written to `audit.pipeline_errors`.
* **After fixing the cause, clear the failed task** so the same DAG run goes
  on: open the run → click the red task → **Clear task** (keep *Downstream*
  and *Only failed* selected). The run keeps its id and its run record, which
  then shows the extra attempts. `scripts/airflow_evidence.py` does exactly
  this in scenario 4.
* **Or trigger a new run.** That is also fine; the failed run stays in the
  history as evidence.
* **Do not use "Mark success"**. It skips the work, so nothing would be
  rebuilt or published.

### 4.5 Interrupted runs

A crash, Ctrl+C, a stopped container or a laptop going to sleep is handled the
same way as the failure drill: run the same command (or clear the task)
again. Ingestion continues after the last committed batch, and every other
stage rebuilds its output from scratch.

## 5. Checkpoint recovery

`state/ingestion_checkpoint.json` records the source checksum, the batch
frequency, the watermark (latest committed `reading_ts`) and one entry per
committed batch (rows, time range, content hash, file, run id). A batch is
written to a temporary file, renamed into place, **read back and verified**,
and only then added to the checkpoint. The checkpoint itself is replaced
atomically, so it never points to a half-written batch.

```bash
docker compose run --rm pipeline python -m src.cli status
```

| Situation | What you see | What to do |
|---|---|---|
| The run stopped in the middle of a batch | that batch is not in the checkpoint | rerun `ingest`. The batch is written again from the start |
| The checkpoint file was deleted, the raw files are still there | `status` shows 0 batches | rerun `ingest`. Each batch is rewritten in its own folder (`batch_id=YYYYQn`), so no duplicates appear (tested) |
| Raw files were deleted, the checkpoint still lists them | exit 4: `checkpoint lists batch ... but ... is missing` | `docker compose run --rm pipeline python -m src.cli ingest --mode full` |
| The source file or the batch frequency changed | exit 3 or exit 2 with "Run a full refresh" | if the change is intended: `ingest --mode full`; otherwise undo the change |
| You want to replay the incremental demonstration | — | `reset-raw --yes`, then `ingest --max-batches 2`, `ingest --fail-after-batches 2`, `ingest` (or `python scripts/demo_incremental_ingestion.py`) |

The raw layer can always be rebuilt from the immutable source, so a full
refresh is the safe answer to any inconsistency.

## 6. Database reset and rebuild

**Everything (pipeline tables and Airflow history):**

```bash
docker compose down -v      # -v also deletes the PostgreSQL volume
docker compose up -d
```

Then trigger the DAG or run `docker compose run --rm pipeline`. The schemas
and tables are created again from `sql/schema.sql` and filled from the
curated files. The Airflow run history is lost, but the run records in
`outputs/runs/` remain, and the next run copies them back into the `audit`
tables.

**Only the pipeline tables (keeps the Airflow history):**

```bash
docker compose exec postgres psql -U firerisk -d firerisk -c "DROP SCHEMA IF EXISTS staging, curated, serving, audit, bench CASCADE;"
docker compose run --rm pipeline
```

Airflow uses its own database (`airflow`) on the same server, so this does not
touch it. Without Docker, use the same `DROP SCHEMA` statement in your local
`psql`.

Afterwards, check `audit.v_duplicate_check` (0 duplicates) and
`audit.v_run_overview` (the new run with status `success`).

## 7. Rollback and cleanup

* **A bad code change.** Every output is derived from the immutable source,
  so rolling back means running the previous version: `git revert <commit>`
  (keeps the history), or test an older version with
  `git switch --detach <commit>`. Rebuild the images when `requirements.txt`
  or a Dockerfile changed (`docker compose build`), then rerun the pipeline.
  Outputs and tables are rebuilt by the old code.
* **Bad published outputs.** The published outputs (`outputs/metrics.json`,
  `predictions.csv`, `feature_importance.csv`, `metadata/split_manifest.json`,
  ...) are versioned in Git. `git checkout -- outputs/ metadata/split_manifest.json`
  restores the last good committed version. Rerun the pipeline on the good
  commit to bring PostgreSQL back in line.
* **Bad files in the data layers.** `python -m src.cli clean --yes` and a new
  run (or just `ingest --mode full` followed by `run-all`).
* **Cleanup when you are done:**

  ```bash
  python -m src.cli clean --yes   # generated layers, checkpoint, run records, model files, pipeline logs
  docker compose down             # stop the containers (add -v to also delete the database)
  docker image rm dss150p-fire-risk-pipeline:1.0 dss150p-fire-risk-airflow:1.0
  docker builder prune            # optional: free the Docker build cache
  ```

  Airflow task logs are in `logs/airflow/` (git-ignored) and can be deleted
  by hand.
* **Never delete** `data/source/industrial_fire_risk_data.csv`, `metadata/`,
  or your `.env` (it holds your passwords). `clean` leaves all three alone.

## 8. Practising failures

| Drill | Command | Evidence |
|---|---|---|
| Interrupted ingestion and resume | `docker compose run --rm pipeline python scripts/demo_incremental_ingestion.py` | `docs/evidence/incremental_ingestion.md` |
| Six failure scenarios + recovery | `docker compose run --rm pipeline python scripts/failure_drill.py` | `docs/evidence/failure_drills.md` |
| Airflow success, rerun, retry and clear-and-recover | `docker compose exec airflow python scripts/airflow_evidence.py` | `docs/evidence/airflow_runs.md` |

The drills only use temporary copies of the source (in `outputs/drills/`,
git-ignored) or put the original file back themselves. The failure drill and
the Airflow script finish by checking that `outputs/metrics.json` did not
change.
