# DSS150P — Reproducible Fire-Risk Prediction Pipeline

Individual laboratory work for **DSS150P Fundamentals of Data Engineering**
(Integrated Laboratory Activity #4). The goal is to rebuild last term's
Predictive Analytics project, *"Integrating IIoT Telemetry and Machine Health
Indicators for Dynamic Fire Hazard Assessment"*, as a reproducible data
engineering pipeline: same source data, same model results, but runnable on any
machine from a clean clone.

> Status: work in progress. The repository is built milestone by milestone;
> the full documentation is added in the final milestones.

## 1. Get the source data

Place the CSV as described in [`data/source/README.md`](data/source/README.md)
and copy the environment template:

```bash
cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
```

## 2. Run with Docker (recommended)

```bash
docker compose up -d postgres                  # database, waits until healthy
docker compose build pipeline                  # pipeline image (Python 3.12, pinned packages)
docker compose run --rm pipeline               # runs: python -m src.cli run-all
docker compose run --rm pipeline python -m src.cli status
```

`run-all` ends with the model stages: training from the curated PostgreSQL
table, evaluation, the reproducibility check against
`metadata/expected_metrics.json` and publishing. The side-by-side comparison
with the original paper is written to `outputs/reproducibility_summary.md`.

Look at the loaded tables (the second run must report 0 inserted / 0 updated):

```bash
docker compose exec postgres psql -U firerisk -d firerisk -c "SELECT * FROM audit.v_duplicate_check;"
docker compose exec postgres psql -U firerisk -d firerisk -c "SELECT * FROM audit.v_run_overview;"
```

Storage benchmark and partition demo (results in `docs/benchmark_report.md`):

```bash
docker compose run --rm pipeline python -m src.cli benchmark
docker compose run --rm pipeline python -m src.cli partition-demo --year 2022 --month 6
```

## Orchestrate with Apache Airflow

```bash
docker compose up -d --build        # postgres + airflow (first build takes a few minutes)
```

Open http://localhost:8080 (no login in this local setup), open the DAG
`predictive_repro_pipeline` and press *Trigger*. Or from the terminal:

```bash
docker compose exec airflow airflow dags trigger predictive_repro_pipeline
docker compose exec airflow airflow dags trigger predictive_repro_pipeline \
    --conf '{"ingest_mode": "full", "fail_after_batches": 2}'      # failure drill: the retry resumes
```

## 3. Run with a local virtual environment

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -m src.cli env-check     # Python/package versions and the resolved config
python -m src.cli run-all       # needs PostgreSQL on POSTGRES_HOST/PORT from .env
python -m src.cli run-all --skip-db   # same pipeline without the database steps
```

## Tests and failure drills

```bash
docker compose run --rm pipeline pytest                     # unit + integration + data-quality tests
docker compose exec -w /opt/project airflow python -m pytest tests/test_dag.py   # DAG structure
docker compose run --rm pipeline python scripts/failure_drill.py                 # failure injection evidence
```

## Repository layout (target)

| Folder | Purpose |
|---|---|
| `config/` | `settings.yml` — every path, seed and model parameter |
| `src/` | pipeline modules (one module per stage) |
| `data/` | source / raw / staging / curated / partitioned / quarantine layers |
| `metadata/` | source manifest, profile, data contract, expected metrics, split manifest |
| `outputs/` | metrics, predictions, model card, run records |
| `docs/` | architecture, lineage, runbook, benchmark and reproducibility reports |
