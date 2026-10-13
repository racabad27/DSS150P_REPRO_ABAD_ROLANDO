"""Shared test fixtures.

Tests never touch the real data layers or the real source file. Each test that
needs data gets its own temporary "workspace": a small synthetic CSV in the
same format as the Kaggle file (3 months, 8,640 readings, ~2% blank Workers),
its own copy of the manifest and data contract (with the sample's checksum),
and settings that point every writable path into the temporary folder.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from src.config import PROJECT_ROOT, load_settings

SAMPLE_START = "2021-01-01 00:00:00"
SAMPLE_PERIODS = 90 * 96            # 1 Jan - 31 Mar 2021, one reading every 15 minutes


def make_sample_frame(periods: int = SAMPLE_PERIODS, seed: int = 7) -> pd.DataFrame:
    """Synthetic readings with the real column names, value ranges and rules."""
    rng = np.random.default_rng(seed)
    ts = pd.date_range(SAMPLE_START, periods=periods, freq="15min")
    temp = rng.uniform(10, 45, periods).round(2)
    pressure = np.where(rng.random(periods) < 0.005, rng.uniform(200, 230, periods),
                        rng.normal(22, 3, periods)).round(2)
    service_days = rng.integers(1, 365, periods)
    age = rng.integers(1, 20, periods)
    risk = np.clip(1.6 * temp + 0.08 * service_days + 0.45 * age + 0.015 * pressure
                   + rng.normal(0, 3, periods) - 15, 0, 100).round(2)
    workers = rng.integers(5, 50, periods).astype(float)
    workers[rng.random(periods) < 0.02] = np.nan
    frame = pd.DataFrame({
        "Date": ts.strftime("%Y-%m-%d"),
        "Time": ts.strftime("%H:%M:%S"),
        "Factory": rng.choice(["Factory_A", "Factory_B", "Factory_C", "Factory_D"], periods),
        "Region": rng.choice(["Industrial_Zone", "Rural", "Urban"], periods),
        "Shift": rng.choice(["Day", "Night"], periods),
        "Workers": workers,
        "Exp": rng.choice(["Junior", "Senior"], periods),
        "Training": rng.choice(["Yes", "No"], periods),
        "Temp": temp,
        "Pressure": pressure,
        "Humidity": rng.uniform(30, 80, periods).round(2),
        "Vibration": rng.uniform(0, 5, periods).round(2),
        "Speed": rng.uniform(1000, 4000, periods).round(2),
        "Age": age,
        "Service_Days": service_days,
        "Gas": rng.uniform(0, 10, periods).round(2),
        "Sparks": rng.integers(0, 5, periods),
        "Alarm": rng.choice(["On", "Off"], periods),
        "Risk": risk,
    })
    frame["Accident"] = (frame["Risk"] > 75).astype(int)
    return frame


def write_sample_csv(frame: pd.DataFrame, path: Path) -> None:
    """Write like the original file: CRLF line endings, Workers as 7.0 or blank."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\r\n")


def build_workspace(tmp_path: Path, frame: pd.DataFrame | None = None, extra: dict | None = None):
    """Create sample source + metadata in tmp_path and return matching Settings."""
    frame = make_sample_frame() if frame is None else frame
    source = tmp_path / "source" / "sample.csv"
    write_sample_csv(frame, source)
    data = source.read_bytes()
    sha = hashlib.sha256(data).hexdigest()

    contract = yaml.safe_load((PROJECT_ROOT / "metadata" / "data_contract.yml").read_text(encoding="utf-8"))
    contract["source"].update({"file_name": source.name, "sha256": sha, "expected_rows": len(frame)})
    ts = pd.to_datetime(frame["Date"] + " " + frame["Time"])
    contract["timestamp"].update({"expected_first": str(ts.min()), "expected_last": str(ts.max())})
    manifest = yaml.safe_load((PROJECT_ROOT / "metadata" / "source_manifest.yml").read_text(encoding="utf-8"))
    manifest["file"].update({"name": source.name, "sha256": sha, "size_bytes": len(data), "rows": len(frame)})
    meta = tmp_path / "metadata"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "data_contract.yml").write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    (meta / "source_manifest.yml").write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    overrides = {
        "paths": {"source_file": str(source), "data_dir": str(tmp_path / "data"),
                  "outputs_dir": str(tmp_path / "outputs"), "state_dir": str(tmp_path / "state"),
                  "logs_dir": str(tmp_path / "logs")},
        "source": {"contract_file": str(meta / "data_contract.yml"),
                   "manifest_file": str(meta / "source_manifest.yml"),
                   "profile_markdown": str(meta / "source_profile.md"),
                   "profile_json": str(meta / "source_profile.json"),
                   "verify_checksum": True},
        "ingestion": {"batch_frequency": "month"},
        "model": {"n_jobs": 1, "input_source": "parquet",
                  "models": {"linear_regression": {}, "random_forest": {"n_estimators": 10},
                             "xgboost": {"n_estimators": 20}}},
        "reproducibility": {"split_manifest_file": str(meta / "split_manifest.json"),
                            "expected_metrics_file": str(meta / "expected_metrics.json")},
    }
    if extra:
        from src.config import _deep_merge
        overrides = _deep_merge(overrides, extra)
    return load_settings(overrides=overrides, use_env_file=False)


@pytest.fixture
def workspace(tmp_path):
    """Settings for an isolated workspace with the default synthetic sample."""
    return build_workspace(tmp_path)


@pytest.fixture
def sample_frame():
    return make_sample_frame()


@pytest.fixture
def pg_workspace(tmp_path):
    """Workspace whose database settings point to a throw-away PostgreSQL database.

    Uses the server from the POSTGRES_* environment variables (inside Docker
    Compose that is the `postgres` service). Skipped when no server is reachable
    or the user may not create databases.
    """
    psycopg = pytest.importorskip("psycopg")
    from src.config import load_env_file

    load_env_file(PROJECT_ROOT / ".env")
    base = build_workspace(tmp_path)
    params = {k: v for k, v in base.database().items() if v not in (None, "")}
    params["connect_timeout"] = 3
    name = f"firerisk_test_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(**params, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    try:
        admin.execute(f'CREATE DATABASE "{name}"')
    except psycopg.Error as exc:
        admin.close()
        pytest.skip(f"cannot create a test database: {exc}")
    settings = build_workspace(tmp_path / "pg", extra={"database": {"dbname": name}})
    try:
        yield settings
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()
