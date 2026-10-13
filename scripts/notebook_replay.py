"""Replay the ORIGINAL notebook logic outside of the pipeline (forensics only).

This script copies the modelling cells of the old Colab notebook
(``Cleaning&FeatureEngineering_PredictiveProject_FINAL.ipynb``) as closely as
possible so the historical results can be re-created and compared with the
engineered pipeline. It is deliberately independent from ``src/``: if both the
pipeline and this replay agree, the pipeline did not change the modelling
semantics.

Only two things differ from the notebook:
  * the date format is a parameter, because the notebook read an Excel
    re-saved copy (DD/MM/YYYY) while the authoritative source uses YYYY-MM-DD;
  * cross-validation uses ``cross_validate`` (one fit per fold, all metrics at
    once) instead of three ``cross_val_score`` calls. The numbers are identical
    because the models are seeded; it is just three times faster.

I used this script to find out which library versions produced the numbers in
the paper and in the notebook (see docs/evidence/dependency_sweep.md):

    python scripts/notebook_replay.py --models xgb
    python scripts/notebook_replay.py --models lr,rf,xgb --cv
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_validate

ROOT = Path(__file__).resolve().parents[1]


def notebook_feature_frame(source: Path, date_format: str) -> tuple[pd.DataFrame, pd.Series]:
    """Cells 2-13 of the original notebook, unchanged except for the date format."""
    df = pd.read_csv(source)
    df = df.dropna(subset=["Workers"])                                   # cell 3
    df["DateTime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format=date_format)
    df["Hour_of_Day"] = df["DateTime"].dt.hour                           # cell 8
    df["Day_of_Week"] = df["DateTime"].dt.dayofweek
    df["Is_Weekend"] = df["Day_of_Week"].isin([5, 6]).astype(int)
    df["Month"] = df["DateTime"].dt.month
    df = df.drop(columns=["Date", "Time", "DateTime"])
    df["Training_Encoded"] = df["Training"].map({"Yes": 1, "No": 0})    # cell 10
    df["Alarm_Encoded"] = df["Alarm"].map({"On": 1, "Off": 0})
    df["Exp_Encoded"] = df["Exp"].map({"Junior": 0, "Senior": 1})
    df = pd.get_dummies(df, columns=["Factory", "Region", "Shift"], drop_first=True, dtype=int)
    df = df.drop(columns=["Training", "Alarm", "Exp"])
    df["Workers_Sparks_Interaction"] = df["Workers"] * df["Sparks"]      # cell 11
    df["Temp_Pressure_Interaction"] = df["Temp"] * df["Pressure"]
    df["Vibration_Speed_Interaction"] = df["Vibration"] * df["Speed"]
    df = df.drop(columns=["Accident"])                                   # cell 13
    return df.drop(columns="Risk"), df["Risk"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default=str(ROOT / "data/source/industrial_fire_risk_data.csv"))
    parser.add_argument("--date-format", default="%Y-%m-%d %H:%M:%S",
                        help="use '%%d/%%m/%%Y %%H:%%M:%%S' for the Excel re-saved copy")
    parser.add_argument("--models", default="lr,rf,xgb")
    parser.add_argument("--cv", action="store_true", help="also run the 5-fold TimeSeriesSplit")
    args = parser.parse_args()

    X, y = notebook_feature_frame(Path(args.source), args.date_format)
    split = int(len(X) * 0.8)                                            # cell 17
    X_train, X_test, y_train, y_test = X.iloc[:split], X.iloc[split:], y.iloc[:split], y.iloc[split:]
    models = {
        "lr": ("linear_regression", LinearRegression()),
        "rf": ("random_forest", RandomForestRegressor(n_estimators=100, random_state=42)),
        "xgb": ("xgboost", xgb.XGBRegressor(n_estimators=100, random_state=42)),
    }
    report: dict = {
        "versions": {"pandas": pd.__version__, "numpy": np.__version__,
                     "scikit-learn": sklearn.__version__, "xgboost": xgb.__version__},
        "rows": {"total": len(X), "train": len(X_train), "test": len(X_test)},
        "models": {},
    }
    for key in args.models.split(","):
        name, model = models[key.strip()]
        started = time.perf_counter()
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        mse = mean_squared_error(y_test, pred)
        entry = {
            "test": {"mse": mse, "rmse": float(np.sqrt(mse)),
                     "mae": mean_absolute_error(y_test, pred), "r2": r2_score(y_test, pred)},
            "fit_seconds": round(time.perf_counter() - started, 1),
        }
        if hasattr(model, "feature_importances_"):
            imp = pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=False)
            entry["top_importances"] = {k: round(float(v), 6) for k, v in imp.head(5).items()}
        if args.cv:
            cv = cross_validate(model, X, y, cv=TimeSeriesSplit(n_splits=5),
                                scoring=("r2", "neg_mean_absolute_error", "neg_mean_squared_error"))
            fold_mse = -cv["test_neg_mean_squared_error"]
            entry["cv"] = {
                "r2_folds": cv["test_r2"].round(8).tolist(),
                "mse_folds": fold_mse.round(8).tolist(),
                "avg_r2": float(cv["test_r2"].mean()),
                "avg_mse": float(fold_mse.mean()),
                "avg_rmse": float(np.sqrt(fold_mse).mean()),
                "avg_mae": float(-cv["test_neg_mean_absolute_error"].mean()),
            }
        report["models"][name] = entry
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
