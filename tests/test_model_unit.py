"""Unit tests: encodings, feature order, chronological split, reproducibility logic."""

from __future__ import annotations

import pandas as pd
import pytest

from src.evaluate import _reproduced_value
from src.train import chronological_split, encode, feature_matrix, key_digest


def _curated_like(n: int = 12) -> pd.DataFrame:
    ts = pd.date_range("2021-01-01", periods=n, freq="15min")
    factories = ["Factory_A", "Factory_B", "Factory_C", "Factory_D"]
    return pd.DataFrame({
        "source_row_key": range(1, n + 1), "reading_ts": ts,
        "factory": [factories[i % 4] for i in range(n)],
        "region": [["Industrial_Zone", "Rural", "Urban"][i % 3] for i in range(n)],
        "shift": [["Day", "Night"][i % 2] for i in range(n)],
        "training": [["Yes", "No"][i % 2] for i in range(n)],
        "alarm": [["On", "Off"][i % 2] for i in range(n)],
        "exp": [["Junior", "Senior"][i % 2] for i in range(n)],
        **{c: [float(i) for i in range(n)] for c in [
            "workers", "temp", "pressure", "humidity", "vibration", "speed", "age", "service_days", "gas",
            "sparks", "hour_of_day", "day_of_week", "is_weekend", "month", "workers_sparks_interaction",
            "temp_pressure_interaction", "vibration_speed_interaction"]},
        "risk": [float(i) for i in range(n)],
    })


def test_encoding_matches_pandas_get_dummies_drop_first(workspace):
    frame = _curated_like()
    encoded = encode(frame, workspace)
    dummies = pd.get_dummies(frame[["factory", "region", "shift"]], drop_first=True, dtype=int)
    dummies.columns = [c.lower() for c in dummies.columns]
    for column in dummies.columns:
        assert encoded[column].tolist() == dummies[column].tolist(), column
    assert encoded["training_encoded"].tolist() == [1 if v == "Yes" else 0 for v in frame["training"]]
    assert encoded["exp_encoded"].tolist() == [1 if v == "Senior" else 0 for v in frame["exp"]]


def test_feature_matrix_uses_the_configured_order(workspace):
    X, y = feature_matrix(encode(_curated_like(), workspace), workspace)
    assert list(X.columns) == list(workspace.get("model.features"))
    assert (X.dtypes == "float64").all()
    assert y.name == "risk"


def test_unknown_category_is_rejected(workspace):
    frame = _curated_like()
    frame.loc[0, "factory"] = "Factory_X"
    with pytest.raises(Exception, match="outside the contract"):
        encode(frame, workspace)


def test_chronological_split_is_time_ordered_and_disjoint():
    frame = _curated_like(10).sample(frac=1, random_state=1)   # shuffled on purpose
    train, test = chronological_split(frame, 0.8)
    assert (len(train), len(test)) == (8, 2)
    assert train["reading_ts"].max() < test["reading_ts"].min()
    assert not set(train["source_row_key"]) & set(test["source_row_key"])


def test_key_digest_does_not_depend_on_row_order():
    ts = pd.Series(pd.date_range("2021-01-01", periods=5, freq="15min"))
    assert key_digest(ts) == key_digest(ts.iloc[::-1])
    assert key_digest(ts) != key_digest(ts.iloc[:-1])


def test_reproduced_value_lookup_for_every_check_kind():
    metrics = {
        "test": {"random_forest": {"rmse": 3.2}},
        "cv": {"random_forest": {"average": {"mse": 15.9}, "folds": {"r2": [0.76, 0.94]}}},
        "feature_importance": {"random_forest": {"temp": 0.17}},
        "coefficients": {"linear_regression": {"temp": 1.62}},
    }
    assert _reproduced_value(metrics, {"model": "random_forest", "kind": "test_metric", "key": "rmse"}) == 3.2
    assert _reproduced_value(metrics, {"model": "random_forest", "kind": "cv_avg", "key": "mse"}) == 15.9
    assert _reproduced_value(metrics, {"model": "random_forest", "kind": "cv_fold", "key": "r2[1]"}) == 0.94
    assert _reproduced_value(metrics, {"model": "random_forest", "kind": "feature_importance", "key": "temp"}) == 0.17
    assert _reproduced_value(metrics, {"model": "linear_regression", "kind": "coefficient", "key": "temp"}) == 1.62
    assert _reproduced_value(metrics, {"model": "xgboost", "kind": "test_metric", "key": "rmse"}) is None
