"""Phase I: write metadata/expected_metrics.json (the machine-readable baseline).

The baseline is what the previous Predictive Analytics project REPORTED, so
most numbers below are transcribed by hand from the paper and from the saved
outputs of the notebook. Each value says where it came from. The only computed
part is the identity of the baseline train/test rows: the script re-applies the
notebook's own rules (drop rows with missing Workers, keep file order, first
80% = train) to the source CSV and stores a SHA-256 of each subset's reading
timestamps. The pipeline later proves it used exactly the same rows.

Run once from the repository root (needs the source CSV in data/source/):

    python scripts/capture_baseline.py
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/source/industrial_fire_risk_data.csv"
OUTPUT = ROOT / "metadata/expected_metrics.json"

TOL_2DP = 0.005    # value published with 2 decimals -> must agree at that precision
TOL_3DP = 0.0005   # value published with 3+ decimals -> 3-decimal agreement (lab default)

MODELS = ("linear_regression", "random_forest", "xgboost")

# --------------------------------------------------------------------------
# Paper: "Integrating IIoT Telemetry and Machine Health Indicators for Dynamic
# Fire Hazard Assessment" (Risma, Abad, Toliba). Baseline of record.
# --------------------------------------------------------------------------
PAPER_TABLE2_TEST = {  # Table 2, chronological test set
    "linear_regression": {"mse": 11.92, "rmse": 3.45, "mae": 2.75, "r2": 0.95},
    "random_forest": {"mse": 10.47, "rmse": 3.24, "mae": 2.57, "r2": 0.96},
    "xgboost": {"mse": 10.57, "rmse": 3.25, "mae": 2.59, "r2": 0.96},
}
PAPER_TABLE3_CV = {  # Table 3, 5-fold TimeSeriesSplit on all 98,000 rows
    "linear_regression": {"r2_folds": [0.92, 0.93, 0.93, 0.94, 0.94], "avg_r2": 0.93,
                          "avg_mse": 11.98, "avg_rmse": 3.46, "avg_mae": 2.76},
    "random_forest": {"r2_folds": [0.76, 0.94, 0.94, 0.95, 0.95], "avg_r2": 0.91,
                      "avg_mse": 15.93, "avg_rmse": 3.82, "avg_mae": 2.99},
    "xgboost": {"r2_folds": [0.75, 0.94, 0.94, 0.95, 0.95], "avg_r2": 0.90,
                "avg_mse": 17.34, "avg_rmse": 3.94, "avg_mae": 3.09},
}
PAPER_IMPORTANCE = {  # Table 4 (RF) and Section 6 text (XGBoost), 3 decimals
    "random_forest": {"temp_pressure_interaction": 0.487, "service_days": 0.285,
                      "temp": 0.171, "age": 0.028, "humidity": 0.003},
    "xgboost": {"temp_pressure_interaction": 0.474, "temp": 0.240, "service_days": 0.236},
}

# --------------------------------------------------------------------------
# Notebook "Cleaning&FeatureEngineering_PredictiveProject_FINAL.ipynb"
# (saved 2026-07-09). Printed outputs of cells 22, 25-27 and 30-32.
# --------------------------------------------------------------------------
NOTEBOOK_XGB_TEST = {"mse": 10.52, "rmse": 3.24, "mae": 2.58, "r2": 0.96}  # cell 22
NOTEBOOK_CV_FOLDS = {  # cells 25-27 (8 decimals)
    "linear_regression": {
        "r2": [0.92424222, 0.93022871, 0.9336818, 0.93934613, 0.94248897],
        "mae": [2.73638547, 2.78395615, 2.73659599, 2.77111508, 2.76289901],
        "mse": [11.81060138, 12.18760493, 11.77811217, 12.07783612, 12.04842502],
        "rmse": [3.43665555, 3.49107504, 3.43192543, 3.47531813, 3.47108413],
    },
    "random_forest": {
        "r2": [0.75975692, 0.93875337, 0.9414391, 0.94706865, 0.94964298],
        "mae": [4.61610032, 2.60496445, 2.56663612, 2.59069957, 2.58052678],
        "mse": [37.45378082, 10.69852315, 10.40041501, 10.54007176, 10.54967799],
        "rmse": [6.11994941, 3.2708597, 3.22496744, 3.24654767, 3.24802678],
    },
    "xgboost": {
        "r2": [0.71945524, 0.93622607, 0.94065007, 0.94636656, 0.94945954],
        "mae": [5.01057468, 2.66312977, 2.58861612, 2.61577758, 2.59345779],
        "mse": [43.73679351, 11.13998949, 10.54054743, 10.67987699, 10.58810769],
        "rmse": [6.61337989, 3.33766228, 3.24662092, 3.26800811, 3.25393726],
    },
}
NOTEBOOK_RF_IMPORTANCE = {  # cell 31, top 15 (6 decimals)
    "temp_pressure_interaction": 0.486817, "service_days": 0.284977, "temp": 0.170548,
    "age": 0.027955, "humidity": 0.003037, "gas": 0.003018, "pressure": 0.002944,
    "speed": 0.002863, "workers": 0.002403, "vibration": 0.002362, "hour_of_day": 0.002321,
    "vibration_speed_interaction": 0.002297, "workers_sparks_interaction": 0.002033,
    "day_of_week": 0.001255, "month": 0.001147,
}
NOTEBOOK_XGB_IMPORTANCE = {  # cell 32, top 15 (6 decimals)
    "temp_pressure_interaction": 0.454930, "temp": 0.264415, "service_days": 0.228868,
    "age": 0.034090, "pressure": 0.002431, "factory_factory_c": 0.000914, "month": 0.000897,
    "humidity": 0.000830, "vibration_speed_interaction": 0.000830, "hour_of_day": 0.000825,
    "region_urban": 0.000823, "day_of_week": 0.000819, "speed": 0.000807,
    "workers_sparks_interaction": 0.000792, "vibration": 0.000792,
}
NOTEBOOK_LR_COEFFICIENTS = {  # cell 30, top 15 by absolute value (6 decimals)
    "temp": 1.621547, "age": 0.466712, "service_days": 0.077849, "factory_factory_b": 0.075170,
    "factory_factory_d": 0.067795, "is_weekend": 0.032716, "alarm_encoded": 0.027728,
    "region_rural": 0.027226, "shift_night": -0.017620, "pressure": 0.014962,
    "training_encoded": -0.011287, "region_urban": 0.010689, "exp_encoded": -0.008736,
    "factory_factory_c": 0.008628, "sparks": 0.007334,
}

NOTEBOOK_FEATURES = [  # X.columns of the notebook, in order (cell 17)
    "Workers", "Temp", "Pressure", "Humidity", "Vibration", "Speed", "Age", "Service_Days",
    "Gas", "Sparks", "Hour_of_Day", "Day_of_Week", "Is_Weekend", "Month", "Training_Encoded",
    "Alarm_Encoded", "Exp_Encoded", "Factory_Factory_B", "Factory_Factory_C",
    "Factory_Factory_D", "Region_Rural", "Region_Urban", "Shift_Night",
    "Workers_Sparks_Interaction", "Temp_Pressure_Interaction", "Vibration_Speed_Interaction",
]

XGB_NOTEBOOK_REASON = (
    "Produced by xgboost >= 3.3.0 in the final Colab run. The pipeline pins xgboost 3.2.0 "
    "to reproduce the paper (baseline of record). Replaying the notebook with xgboost "
    "3.3.0/3.4.1 reproduces these exact values, so the difference is a library version, "
    "not a data or preprocessing change."
)
XGB_TABLE3_REASON = (
    "Paper Table 3 mixes two runs: its XGBoost fold R2 values match xgboost <= 3.2.0 while "
    "its XGBoost averages (17.34 / 3.94 / 3.09) match xgboost >= 3.3.0. One environment "
    "cannot reproduce both; the pinned 3.2.0 reproduces Table 2 and the fold R2 values."
)


def key_hash(timestamps: pd.Series) -> str:
    """SHA-256 of the sorted reading timestamps (the business key) of a subset."""
    text = "\n".join(sorted(timestamps.dt.strftime("%Y-%m-%dT%H:%M:%S")))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def baseline_split() -> dict:
    """Re-apply the notebook's cleaning + split rules to identify the baseline rows."""
    df = pd.read_csv(SOURCE)
    df = df.dropna(subset=["Workers"])                     # notebook cell 3
    ts = pd.to_datetime(df["Date"] + " " + df["Time"], format="%Y-%m-%d %H:%M:%S")
    n_train = int(len(df) * 0.8)                           # notebook cell 17
    train, test = ts.iloc[:n_train], ts.iloc[n_train:]
    iso = "%Y-%m-%dT%H:%M:%S"
    return {
        "rule": "drop rows with missing Workers, keep source file order, first int(n*0.8) rows = train",
        "n_total": int(len(df)),
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "train_start": train.min().strftime(iso),
        "train_end": train.max().strftime(iso),
        "test_start": test.min().strftime(iso),
        "test_end": test.max().strftime(iso),
        "key_definition": "sha256 of the sorted reading timestamps (YYYY-MM-DDTHH:MM:SS) joined by newlines",
        "train_keys_sha256": key_hash(train),
        "test_keys_sha256": key_hash(test),
    }


def build_checks() -> list[dict]:
    checks: list[dict] = []

    def add(check_id, source, model, kind, key, baseline, tolerance, blocking=True, reason=None):
        entry = {"id": check_id, "source": source, "model": model, "kind": kind, "key": key,
                 "baseline": baseline, "tolerance": tolerance, "blocking": blocking}
        if reason:
            entry["accepted_deviation"] = reason
        if check_id == "paper.table2.random_forest.rmse":
            entry["primary"] = True
        checks.append(entry)

    for model, metrics in PAPER_TABLE2_TEST.items():
        for metric, value in metrics.items():
            add(f"paper.table2.{model}.{metric}", "paper Table 2", model, "test_metric",
                metric, value, TOL_2DP)
    for model, values in PAPER_TABLE3_CV.items():
        for i, value in enumerate(values["r2_folds"]):
            add(f"paper.table3.{model}.r2_fold{i + 1}", "paper Table 3", model, "cv_fold",
                f"r2[{i}]", value, TOL_2DP)
        for metric in ("avg_r2", "avg_mse", "avg_rmse", "avg_mae"):
            reason = XGB_TABLE3_REASON if model == "xgboost" and metric != "avg_r2" else None
            add(f"paper.table3.{model}.{metric}", "paper Table 3", model, "cv_avg",
                metric.removeprefix("avg_"), values[metric], TOL_2DP,
                blocking=reason is None, reason=reason)
    for model, features in PAPER_IMPORTANCE.items():
        for feature, value in features.items():
            add(f"paper.importance.{model}.{feature}", "paper Table 4 / Section 6", model,
                "feature_importance", feature, value, TOL_3DP)
    for metric, value in NOTEBOOK_XGB_TEST.items():
        add(f"notebook.test.xgboost.{metric}", "notebook cell 22", "xgboost", "test_metric",
            metric, value, TOL_2DP, blocking=False, reason=XGB_NOTEBOOK_REASON)
    for model, per_metric in NOTEBOOK_CV_FOLDS.items():
        for metric, values in per_metric.items():
            for i, value in enumerate(values):
                is_xgb = model == "xgboost"
                add(f"notebook.cv.{model}.{metric}_fold{i + 1}", "notebook cells 25-27", model,
                    "cv_fold", f"{metric}[{i}]", value, TOL_3DP, blocking=not is_xgb,
                    reason=XGB_NOTEBOOK_REASON if is_xgb else None)
    for feature, value in NOTEBOOK_RF_IMPORTANCE.items():
        add(f"notebook.importance.random_forest.{feature}", "notebook cell 31", "random_forest",
            "feature_importance", feature, value, TOL_3DP)
    for feature, value in NOTEBOOK_XGB_IMPORTANCE.items():
        add(f"notebook.importance.xgboost.{feature}", "notebook cell 32", "xgboost",
            "feature_importance", feature, value, TOL_3DP, blocking=False, reason=XGB_NOTEBOOK_REASON)
    for feature, value in NOTEBOOK_LR_COEFFICIENTS.items():
        add(f"notebook.coefficient.linear_regression.{feature}", "notebook cell 30",
            "linear_regression", "coefficient", feature, value, TOL_3DP)
    return checks


def main() -> None:
    expected = {
        "baseline_id": "pa-fire-risk-group-project",
        "captured_on": "2026-10-13",
        "description": (
            "Baseline of the previous Predictive Analytics group project. Values are copied "
            "from the paper (baseline of record) and from the saved notebook outputs."
        ),
        "sources": {
            "paper": "Risma, Abad, Toliba - Integrating IIoT Telemetry and Machine Health "
                     "Indicators for Dynamic Fire Hazard Assessment (Tables 2-4, Section 6)",
            "notebook": "Cleaning&FeatureEngineering_PredictiveProject_FINAL.ipynb (Colab, saved 2026-07-09)",
        },
        "task": {"type": "regression", "target": "Risk", "target_range": [0, 100]},
        "data": {
            "source_file": "industrial_fire_risk_data.csv",
            "source_sha256": "5db607c512fad903bcfb6e242e2dcfc6ba8f9de091becf6169e6ae5414f309ba",
            "rows_in_source": 100000,
            "rows_after_cleaning": 98000,
            "cleaning": "rows with a missing Workers value are dropped (2,000 rows); nothing else is removed",
            "outliers": "Pressure (508 IQR / 500 z-score) and Risk (24 / 9) outliers were kept on purpose",
        },
        "features": {
            "count": len(NOTEBOOK_FEATURES),
            "notebook_names_in_order": NOTEBOOK_FEATURES,
            "pipeline_name_rule": "pipeline feature name = notebook name in lower case",
            "excluded_columns": {"Risk": "target", "Accident": "outcome recorded after the fact (leakage)",
                                 "Date/Time": "replaced by derived temporal features"},
            "engineering": {
                "temporal": "Hour_of_Day, Day_of_Week (Mon=0), Is_Weekend (Sat/Sun), Month from Date+Time",
                "binary_maps": "Training Yes=1/No=0, Alarm On=1/Off=0, Exp Senior=1/Junior=0",
                "one_hot": "Factory, Region, Shift with drop_first=True (dropped: Factory_A, Industrial_Zone, Day)",
                "interactions": "Workers*Sparks, Temp*Pressure, Vibration*Speed",
                "scaling": "none",
            },
        },
        "split": {
            "method": "chronological holdout",
            "train_fraction": 0.8,
            "baseline_rows": baseline_split(),
        },
        "cross_validation": {
            "method": "TimeSeriesSplit",
            "n_splits": 5,
            "data": "all 98,000 cleaned rows (train and test together), file order",
            "averaging": "mean of the five fold values; average RMSE = mean of per-fold RMSE",
        },
        "models": {
            "linear_regression": {"class": "sklearn.linear_model.LinearRegression", "hyperparameters": {}},
            "random_forest": {"class": "sklearn.ensemble.RandomForestRegressor",
                              "hyperparameters": {"n_estimators": 100, "random_state": 42}},
            "xgboost": {"class": "xgboost.XGBRegressor",
                        "hyperparameters": {"n_estimators": 100, "random_state": 42}},
        },
        "random_seed": 42,
        "primary": {"model": "random_forest", "metric": "rmse", "eval_set": "test",
                    "baseline": 3.24, "check_id": "paper.table2.random_forest.rmse"},
        "tolerance_policy": {
            "reported_2_decimals": TOL_2DP,
            "reported_3_or_more_decimals": TOL_3DP,
            "rule": ("|reproduced - baseline| must not exceed half a unit of the last reported "
                     "decimal (0.005 for 2-decimal values, 0.0005 for values reported with 3 or "
                     "more decimals). This is stricter than the lab's recommended +/-0.01."),
            "blocking": "all checks marked blocking must pass, otherwise the pipeline fails",
            "accepted_deviation": "non-blocking checks are reported with the documented reason",
        },
        "reported": {
            "paper_table2_test": PAPER_TABLE2_TEST,
            "paper_table3_cv": PAPER_TABLE3_CV,
            "paper_feature_importance": PAPER_IMPORTANCE,
            "notebook_xgboost_test": NOTEBOOK_XGB_TEST,
            "notebook_cv_folds": NOTEBOOK_CV_FOLDS,
            "notebook_rf_importance": NOTEBOOK_RF_IMPORTANCE,
            "notebook_xgb_importance": NOTEBOOK_XGB_IMPORTANCE,
            "notebook_lr_coefficients": NOTEBOOK_LR_COEFFICIENTS,
        },
        "checks": build_checks(),
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
    blocking = sum(c["blocking"] for c in expected["checks"])
    print(f"Wrote {OUTPUT.relative_to(ROOT)} with {len(expected['checks'])} checks "
          f"({blocking} blocking, {len(expected['checks']) - blocking} accepted deviations)")
    print(json.dumps(expected["split"]["baseline_rows"], indent=2))


if __name__ == "__main__":
    main()
