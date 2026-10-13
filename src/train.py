"""Model input, deterministic split and training (reproduces the old notebook).

Model input
-----------
The curated table already holds the notebook's numeric and derived features.
Here the categorical columns get the notebook's encodings:

* binary maps from settings.yml (Training Yes=1, Alarm On=1, Exp Senior=1);
* one-hot columns with the first category dropped for factory, region and
  shift. The categories come from the DATA CONTRACT, not from the data, so the
  encoding is fixed before any split and cannot leak test information. It is
  the same result as ``pd.get_dummies(..., drop_first=True)`` in the notebook.

The matrix columns are then put in the exact order of ``model.features``
(the notebook's order). Tree models break ties between equally good splits by
column position, so the order is part of the reproduction.

Split
-----
Rows are ordered by ``reading_ts``; the first ``int(n * 0.8)`` rows train and
the rest test (78,400 / 19,600). The identity of both subsets is written to
metadata/split_manifest.json as SHA-256 digests of their reading timestamps,
so it can be compared with the baseline rows of the original project.
"""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from src.config import Settings
from src.contract import load_contract
from src.errors import ConfigError, DataQualityError
from src.observability import StageLogger
from src.storage import CURATED_TABLE, connect, frame_fingerprint
from src.transform import curated_path
from src.validate import QualityReport

# XGBoost is saved in its own JSON format: a joblib/pickle file of an
# XGBRegressor written inside Airflow referenced the `dill` package (Airflow
# imports it and it hooks into pickling), so it could not be loaded by the
# pipeline image. The native format is portable and independent of pickle.
MODEL_FILES = {"linear_regression": "linear_regression.joblib",
               "random_forest": "random_forest.joblib",
               "xgboost": "xgboost.json"}
TRAIN_FILE = "train.parquet"
TEST_FILE = "test.parquet"
KEY_COLUMNS = ["source_row_key", "reading_ts"]


# ---------------------------------------------------------------------------
# reading the curated data
# ---------------------------------------------------------------------------
def load_curated(settings: Settings, log: StageLogger, input_source: str | None = None) -> pd.DataFrame:
    """Curated rows ordered by time, from PostgreSQL (default) or the Parquet file."""
    source = input_source or settings.get("model.input_source")
    if source == "postgres":
        with connect(settings, log) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {CURATED_TABLE} ORDER BY reading_ts, source_row_key")
                names = [d.name for d in cur.description]
                frame = pd.DataFrame(cur.fetchall(), columns=names)
        frame["reading_ts"] = pd.to_datetime(frame["reading_ts"])
    elif source == "parquet":
        frame = pd.read_parquet(curated_path(settings))
    else:
        raise ConfigError(f"model.input_source must be 'postgres' or 'parquet', got {source!r}")
    frame = frame.sort_values(["reading_ts", "source_row_key"], kind="stable").reset_index(drop=True)
    log.event("model_input_loaded", f"Loaded {len(frame):,} curated rows from {source}", source=source)
    return frame


# ---------------------------------------------------------------------------
# encodings and the feature matrix
# ---------------------------------------------------------------------------
def encode(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Add the notebook's binary and one-hot encodings (categories from the contract)."""
    contract = load_contract(settings)
    by_staging_name = {spec.staging_name: spec for spec in contract.columns}
    encoding = settings.get("model.encoding")
    out = frame.copy()
    for column, mapping in encoding["binary"].items():
        values = out[column].map(mapping)
        if values.isna().any():
            unknown = sorted(set(out.loc[values.isna(), column].astype(str)))
            raise DataQualityError(f"{column} has values without an encoding: {unknown}")
        out[f"{column}_encoded"] = values.astype("int64")
    for column in encoding["one_hot_drop_first"]:
        categories = sorted(str(v) for v in by_staging_name[column].allowed_values)
        unknown = set(out[column].astype(str)) - set(categories)
        if unknown:
            raise DataQualityError(f"{column} has categories outside the contract: {sorted(unknown)}")
        for value in categories[1:]:          # first category (alphabetical) is the reference
            out[f"{column}_{value.lower()}"] = (out[column] == value).astype("int64")
    return out


def feature_matrix(encoded: pd.DataFrame, settings: Settings) -> tuple[pd.DataFrame, pd.Series]:
    """X with the configured features in the configured order (float64), and y."""
    features = list(settings.get("model.features"))
    missing = [f for f in features if f not in encoded.columns]
    if missing:
        raise DataQualityError(f"feature columns missing from the model input: {missing}")
    X = encoded[features].astype("float64")
    y = encoded[settings.get("model.target")].astype("float64")
    return X, y


def chronological_split(frame: pd.DataFrame, train_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """First int(n * fraction) rows (by time) = train, the rest = test."""
    ordered = frame.sort_values(["reading_ts", "source_row_key"], kind="stable").reset_index(drop=True)
    n_train = int(len(ordered) * train_fraction)
    return ordered.iloc[:n_train].copy(), ordered.iloc[n_train:].copy()


# ---------------------------------------------------------------------------
# fingerprints for the split manifest
# ---------------------------------------------------------------------------
def key_digest(timestamps: pd.Series) -> str:
    """Same definition as the baseline: SHA-256 of sorted ISO timestamps, one per line."""
    text = "\n".join(sorted(pd.to_datetime(timestamps).dt.strftime("%Y-%m-%dT%H:%M:%S")))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def matrix_digest(X: pd.DataFrame, y: pd.Series) -> str:
    """SHA-256 of the exact float64 bytes the model sees (features, then target)."""
    digest = hashlib.sha256()
    digest.update(",".join(X.columns).encode("utf-8"))
    digest.update(np.ascontiguousarray(X.to_numpy(dtype="float64")).tobytes())
    digest.update(np.ascontiguousarray(y.to_numpy(dtype="float64")).tobytes())
    return digest.hexdigest()


def _iso(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M:%S")


def build_split_manifest(settings: Settings, curated: pd.DataFrame, train: pd.DataFrame, test: pd.DataFrame,
                         X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame,
                         y_test: pd.Series) -> dict[str, Any]:
    """Everything needed to prove which rows trained and tested the model.

    Deliberately contains no run ids or clock times, so the same data always
    produces a byte-identical file (a quick determinism check in Git).
    """
    n_splits = int(settings.get("model.cross_validation.n_splits"))
    full = pd.concat([train, test], ignore_index=True)
    folds = []
    for number, (fit_idx, val_idx) in enumerate(TimeSeriesSplit(n_splits=n_splits).split(full), start=1):
        folds.append({
            "fold": number,
            "train_rows": int(len(fit_idx)),
            "validation_rows": int(len(val_idx)),
            "train_end": _iso(full["reading_ts"].iloc[fit_idx[-1]]),
            "validation_start": _iso(full["reading_ts"].iloc[val_idx[0]]),
            "validation_end": _iso(full["reading_ts"].iloc[val_idx[-1]]),
        })
    return {
        "split_method": "chronological holdout",
        "order_by": "reading_ts (ties broken by source_row_key)",
        "rule": "train = first int(n * train_fraction) rows by time, test = the remaining rows",
        "train_fraction": float(settings.get("model.split.train_fraction")),
        "random_seed": int(settings.get("model.random_seed")),
        "n_total": int(len(curated)),
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "train_period": {"start": _iso(train["reading_ts"].min()), "end": _iso(train["reading_ts"].max())},
        "test_period": {"start": _iso(test["reading_ts"].min()), "end": _iso(test["reading_ts"].max())},
        "boundary": "first test reading = " + _iso(test["reading_ts"].min()),
        "train_source_row_keys": [int(train["source_row_key"].min()), int(train["source_row_key"].max())],
        "test_source_row_keys": [int(test["source_row_key"].min()), int(test["source_row_key"].max())],
        "key_definition": "sha256 of the sorted reading timestamps (YYYY-MM-DDTHH:MM:SS) joined by newlines",
        "train_keys_sha256": key_digest(train["reading_ts"]),
        "test_keys_sha256": key_digest(test["reading_ts"]),
        "target": settings.get("model.target"),
        "features": list(X_train.columns),
        "train_matrix_sha256": matrix_digest(X_train, y_train),
        "test_matrix_sha256": matrix_digest(X_test, y_test),
        "curated_row_hash_digest": frame_fingerprint(curated)["row_hash_digest"],
        "cross_validation": {"method": "TimeSeriesSplit", "n_splits": n_splits,
                             "data": "all curated rows in time order", "folds": folds},
    }


# ---------------------------------------------------------------------------
# models
# ---------------------------------------------------------------------------
def make_model(name: str, settings: Settings):
    """Model with the baseline hyperparameters; the seed comes from settings.yml."""
    params = dict(settings.get(f"model.models.{name}") or {})
    seed = int(settings.get("model.random_seed"))
    n_jobs = settings.get("model.n_jobs")
    if name == "linear_regression":
        return LinearRegression(**params)
    if name == "random_forest":
        return RandomForestRegressor(random_state=seed, n_jobs=n_jobs, **params)
    if name == "xgboost":
        return XGBRegressor(random_state=seed, n_jobs=None if n_jobs in (None, -1) else n_jobs, **params)
    raise ConfigError(f"unknown model {name!r}")


def predict(model, X: pd.DataFrame) -> np.ndarray:
    """Predict deterministically.

    A Random Forest that predicts with several threads adds up the trees in
    whatever order the threads finish, which changes the last digits of the
    result between runs. Predicting with one thread fixes the order.
    """
    if isinstance(model, RandomForestRegressor):
        model.set_params(n_jobs=1)
    return model.predict(X)


def set_global_seeds(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def model_path(settings: Settings, name: str) -> Path:
    return settings.model_dir / MODEL_FILES[name]


def save_model(model, path: Path) -> None:
    if isinstance(model, XGBRegressor):
        model.save_model(path)                      # native JSON format
    else:
        joblib.dump(model, path, compress=3)


def load_model(name: str, path: Path):
    if name == "xgboost":
        model = XGBRegressor()
        model.load_model(path)
        return model
    return joblib.load(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# predictive-integrity checks
# ---------------------------------------------------------------------------
def validate_model_input(settings: Settings, run_id: str, train: pd.DataFrame, test: pd.DataFrame,
                         X_train: pd.DataFrame, X_test: pd.DataFrame,
                         y_train: pd.Series, y_test: pd.Series) -> QualityReport:
    report = QualityReport("model_input", run_id)
    features = list(settings.get("model.features"))
    leakage = [c for c in set(settings.get("model.leakage_columns") or []) | {settings.get("model.target")}
               if c in X_train.columns]
    report.check("no_target_leakage", "predictive_integrity",
                 "Neither the target nor target-derived columns (accident) are features",
                 not leakage, observed=leakage, expected=[])
    report.check("feature_order", "predictive_integrity",
                 "Feature columns are exactly the configured 26 in the notebook order",
                 list(X_train.columns) == features and list(X_test.columns) == features,
                 observed=len(X_train.columns), expected=len(features))
    finite = bool(np.isfinite(X_train.to_numpy()).all() and np.isfinite(X_test.to_numpy()).all()
                  and np.isfinite(y_train.to_numpy()).all() and np.isfinite(y_test.to_numpy()).all())
    report.check("finite_values", "completeness", "No missing or infinite values in X or y",
                 finite, observed=finite, expected=True)
    overlap = len(set(train["source_row_key"]) & set(test["source_row_key"]))
    report.check("no_train_test_overlap", "predictive_integrity", "No reading is in both train and test",
                 overlap == 0, observed=overlap, expected=0, failing_rows=overlap)
    report.check("train_before_test", "predictive_integrity",
                 "Every training reading is earlier than every test reading",
                 train["reading_ts"].max() < test["reading_ts"].min(),
                 observed=[str(train["reading_ts"].max()), str(test["reading_ts"].min())], expected="train < test")
    in_range = bool(y_train.between(0, 100).all() and y_test.between(0, 100).all())
    report.check("target_range", "validity", "Risk stays within 0-100", in_range, observed=in_range, expected=True)
    return report


# ---------------------------------------------------------------------------
# the training stage
# ---------------------------------------------------------------------------
def train_models(settings: Settings, run_id: str, log: StageLogger, input_source: str | None = None) -> dict[str, Any]:
    seed = int(settings.get("model.random_seed"))
    set_global_seeds(seed)
    curated = load_curated(settings, log, input_source)
    encoded = encode(curated, settings)
    train, test = chronological_split(encoded, float(settings.get("model.split.train_fraction")))
    X_train, y_train = feature_matrix(train, settings)
    X_test, y_test = feature_matrix(test, settings)

    report = validate_model_input(settings, run_id, train, test, X_train, X_test, y_train, y_test)
    report.save(settings)
    report.log_results(log)
    report.raise_if_failed()

    # persist the exact model input (keys + features + target) for auditing
    settings.model_input_dir.mkdir(parents=True, exist_ok=True)
    target = settings.get("model.target")
    for frame, X, y, name in ((train, X_train, y_train, TRAIN_FILE), (test, X_test, y_test, TEST_FILE)):
        pd.concat([frame[KEY_COLUMNS].reset_index(drop=True), X.reset_index(drop=True),
                   y.rename(target).reset_index(drop=True)], axis=1).to_parquet(
            settings.model_input_dir / name, index=False)

    manifest = build_split_manifest(settings, curated, train, test, X_train, y_train, X_test, y_test)
    manifest_path = settings.file_setting("reproducibility.split_manifest_file")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    log.event("split_written", f"Chronological split: {len(train):,} train / {len(test):,} test, "
              f"boundary {manifest['test_period']['start']}", train_rows=len(train), test_rows=len(test))

    settings.model_dir.mkdir(parents=True, exist_ok=True)
    card: dict[str, Any] = {
        "target": target,
        "features": list(X_train.columns),
        "training_rows": int(len(X_train)),
        "train_matrix_sha256": manifest["train_matrix_sha256"],
        "random_seed": seed,
        "libraries": {"scikit-learn": sklearn.__version__, "xgboost": xgboost.__version__,
                      "pandas": pd.__version__, "numpy": np.__version__},
        "note": "Model files are not committed (the Random Forest file is about 150 MB); "
                "rerun `python -m src.cli train` to recreate them, then compare the SHA-256 below. "
                "scikit-learn models are joblib files, XGBoost uses its native JSON format.",
        "models": {},
    }
    for name in settings.get("model.models"):
        model = make_model(name, settings)
        model.fit(X_train, y_train)
        path = model_path(settings, name)
        save_model(model, path)
        params = {k: v for k, v in model.get_params().items() if not callable(v)}
        card["models"][name] = {
            "class": f"{type(model).__module__}.{type(model).__name__}",
            "hyperparameters": json.loads(json.dumps(params, default=str)),
            "file": path.relative_to(settings.outputs_dir).as_posix(),
            "file_sha256": _sha256(path),
            "file_size_bytes": path.stat().st_size,
        }
        log.event("model_trained", f"{name} trained and saved ({path.stat().st_size / 1e6:.1f} MB)", model=name)
    (settings.model_dir / "model_card.json").write_text(json.dumps(card, indent=2) + "\n",
                                                        encoding="utf-8", newline="\n")
    return {"train_rows": int(len(train)), "test_rows": int(len(test)), "models": list(card["models"]),
            "split_manifest": manifest_path.relative_to(settings.root).as_posix() if settings.root in manifest_path.parents
            else str(manifest_path)}


def load_model_input(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The train/test files written by train_models (keys + features + target)."""
    train_path, test_path = settings.model_input_dir / TRAIN_FILE, settings.model_input_dir / TEST_FILE
    if not train_path.is_file() or not test_path.is_file():
        raise DataQualityError("No model input found. Run `python -m src.cli train` first.")
    return pd.read_parquet(train_path), pd.read_parquet(test_path)
