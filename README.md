# DSS150P — Reproducible Fire-Risk Prediction Pipeline

Individual laboratory work for **DSS150P Fundamentals of Data Engineering**
(Integrated Laboratory Activity #4). The goal is to rebuild last term's
Predictive Analytics project, *"Integrating IIoT Telemetry and Machine Health
Indicators for Dynamic Fire Hazard Assessment"*, as a reproducible data
engineering pipeline: same source data, same model results, but runnable on any
machine from a clean clone.

## Quick start (local Python)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
python -m src.cli env-check     # checks Python/package versions and the config
```

Place the source CSV as described in [`data/source/README.md`](data/source/README.md).

## Repository layout (target)

| Folder | Purpose |
|---|---|
| `config/` | `settings.yml` — every path, seed and model parameter |
| `src/` | pipeline modules (one module per stage) |
| `data/` | source / raw / staging / curated / partitioned / quarantine layers |
| `metadata/` | source manifest, profile, data contract, expected metrics, split manifest |
| `outputs/` | metrics, predictions, model card, run records |
| `docs/` | architecture, lineage, runbook, benchmark and reproducibility reports |
