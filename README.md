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

## 3. Run with a local virtual environment

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -m src.cli env-check     # Python/package versions and the resolved config
python -m src.cli run-all       # ingest -> validate -> staging -> curated
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
