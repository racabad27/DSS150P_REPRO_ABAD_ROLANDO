"""Evaluation, the reproducibility check and publishing of the model outputs.

evaluate_models      test-set metrics, 5-fold TimeSeriesSplit, importances,
                     coefficients -> outputs/metrics.json, predictions.csv,
                     feature_importance.csv
reproducibility_check compares outputs/metrics.json with
                     metadata/expected_metrics.json (156 checks) and the split
                     manifest with the baseline rows -> reproducibility_check.json
publish_outputs      predictions + metrics into PostgreSQL, final run summary

Metric definitions (same as the notebook): MSE, RMSE = sqrt(MSE), MAE and R²
on the chronological test set. For cross-validation the averages are the mean
of the five fold values, and the average RMSE is the mean of the per-fold RMSE
(not the square root of the average MSE), exactly as the notebook printed it.
"""

from __future__ import annotations

import json
import logging
import math
import re
from importlib import metadata
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

from src.config import Settings
from src.errors import DataQualityError, ReproducibilityError
from src.observability import RunRecord, StageLogger, utc_now_iso
from src.train import load_model, load_model_input, make_model, model_path, predict

DECIMALS = 10   # metrics are stored rounded so tiny float noise never changes the file


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    mse = mean_squared_error(y_true, y_pred)
    return {"mse": mse, "rmse": math.sqrt(mse), "mae": mean_absolute_error(y_true, y_pred),
            "r2": r2_score(y_true, y_pred)}


def _round(values: Any) -> Any:
    if isinstance(values, dict):
        return {k: _round(v) for k, v in values.items()}
    if isinstance(values, list):
        return [_round(v) for v in values]
    if isinstance(values, (float, np.floating)):
        return round(float(values), DECIMALS)
    return values


# ---------------------------------------------------------------------------
# evaluation
# ---------------------------------------------------------------------------
def evaluate_models(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    train, test = load_model_input(settings)
    features = list(settings.get("model.features"))
    target = settings.get("model.target")
    X_train, y_train = train[features], train[target]
    X_test, y_test = test[features], test[target]
    X_all = pd.concat([X_train, X_test], ignore_index=True)
    y_all = pd.concat([y_train, y_test], ignore_index=True)
    n_splits = int(settings.get("model.cross_validation.n_splits"))

    results: dict[str, Any] = {"test": {}, "cv": {}, "feature_importance": {}, "coefficients": {}}
    predictions = test[["source_row_key", "reading_ts"]].copy()
    predictions["actual_risk"] = y_test.to_numpy()

    for name in settings.get("model.models"):
        path = model_path(settings, name)
        if not path.is_file():
            raise DataQualityError(f"Model file {path} is missing. Run `python -m src.cli train` first.")
        model = load_model(name, path)
        pred = predict(model, X_test)
        predictions[f"pred_{name}"] = pred
        results["test"][name] = _metrics(y_test.to_numpy(), pred)
        log.event("test_metrics", f"{name}: RMSE {results['test'][name]['rmse']:.4f}, "
                  f"MAE {results['test'][name]['mae']:.4f}, R2 {results['test'][name]['r2']:.4f}", model=name)

        if hasattr(model, "feature_importances_"):
            results["feature_importance"][name] = dict(zip(features, map(float, model.feature_importances_)))
        if hasattr(model, "coef_"):
            results["coefficients"][name] = dict(zip(features, map(float, model.coef_)))
            results["coefficients"][name]["(intercept)"] = float(model.intercept_)

        # 5-fold walk-forward CV on all rows, a fresh model per fold (like cross_val_score)
        folds = {"r2": [], "mae": [], "mse": [], "rmse": []}
        for fit_idx, val_idx in TimeSeriesSplit(n_splits=n_splits).split(X_all):
            fold_model = make_model(name, settings)
            fold_model.fit(X_all.iloc[fit_idx], y_all.iloc[fit_idx])
            fold = _metrics(y_all.iloc[val_idx].to_numpy(), predict(fold_model, X_all.iloc[val_idx]))
            for key in folds:
                folds[key].append(fold[key])
        results["cv"][name] = {"folds": folds, "average": {k: float(np.mean(v)) for k, v in folds.items()}}
        log.event("cv_metrics", f"{name}: CV R2 folds " + ", ".join(f"{v:.4f}" for v in folds["r2"]), model=name)

    primary_model = settings.get("model.primary_model")
    primary_metric = settings.get("model.primary_metric")
    split_manifest = json.loads(settings.file_setting("reproducibility.split_manifest_file").read_text(encoding="utf-8"))
    metrics = {
        "target": target,
        "eval_set": "chronological test set (last 20% of readings)",
        "rows": {"train": int(len(train)), "test": int(len(test)), "cv_total": int(len(X_all))},
        "primary": {"model": primary_model, "metric": primary_metric,
                    "value": round(results["test"][primary_model][primary_metric], DECIMALS)},
        "metric_definitions": {
            "mse": "mean squared error", "rmse": "square root of MSE", "mae": "mean absolute error",
            "r2": "coefficient of determination",
            "cv_average": "mean of the 5 TimeSeriesSplit folds (average RMSE = mean of per-fold RMSE)",
        },
        "test": _round(results["test"]),
        "cv": _round(results["cv"]),
        "feature_importance": _round(results["feature_importance"]),
        "coefficients": _round(results["coefficients"]),
        "data_fingerprint": {k: split_manifest[k] for k in
                             ("train_keys_sha256", "test_keys_sha256", "train_matrix_sha256", "test_matrix_sha256")},
        "libraries": {pkg: metadata.version(pkg) for pkg in ("scikit-learn", "xgboost", "pandas", "numpy")},
    }
    settings.outputs_dir.mkdir(parents=True, exist_ok=True)
    (settings.outputs_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")

    predictions["reading_ts"] = pd.to_datetime(predictions["reading_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    predictions.to_csv(settings.outputs_dir / "predictions.csv", index=False, float_format="%.6f")

    rows = []
    for model_name, values in results["feature_importance"].items():
        ranked = sorted(values.items(), key=lambda item: -item[1])
        rows += [{"model": model_name, "rank": i, "feature": f, "importance": round(v, 6)}
                 for i, (f, v) in enumerate(ranked, start=1)]
    pd.DataFrame(rows).to_csv(settings.outputs_dir / "feature_importance.csv", index=False)
    return {"test_rows": int(len(test)), "prediction_rows": int(len(predictions)),
            "primary": metrics["primary"]}


# ---------------------------------------------------------------------------
# reproducibility check
# ---------------------------------------------------------------------------
def _reproduced_value(metrics: dict, check: dict) -> float | None:
    model, kind, key = check["model"], check["kind"], check["key"]
    try:
        if kind == "test_metric":
            return metrics["test"][model][key]
        if kind == "cv_avg":
            return metrics["cv"][model]["average"][key]
        if kind == "cv_fold":
            name, index = re.fullmatch(r"(\w+)\[(\d+)\]", key).groups()
            return metrics["cv"][model]["folds"][name][int(index)]
        if kind == "feature_importance":
            return metrics["feature_importance"][model][key]
        if kind == "coefficient":
            return metrics["coefficients"][model][key]
    except (KeyError, IndexError, AttributeError):
        return None
    raise ValueError(f"unknown check kind {kind!r}")


def _environment(settings: Settings) -> dict[str, Any]:
    pins = {}
    for line in (settings.root / "requirements.txt").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_.-]+)(\[[^\]]*\])?==([^\s;#]+)", line.strip())
        if match:
            pins[match.group(1)] = match.group(3)
    rows = {}
    for name, wanted in pins.items():
        try:
            installed = metadata.version(name)
        except metadata.PackageNotFoundError:
            installed = None
        rows[name] = {"pinned": wanted, "installed": installed, "matches": installed == wanted}
    return {"all_pins_match": all(r["matches"] for r in rows.values()), "packages": rows}


def reproducibility_check(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    expected = json.loads(settings.file_setting("reproducibility.expected_metrics_file").read_text(encoding="utf-8"))
    metrics = json.loads((settings.outputs_dir / "metrics.json").read_text(encoding="utf-8"))
    split = json.loads(settings.file_setting("reproducibility.split_manifest_file").read_text(encoding="utf-8"))

    checks = []
    for check in expected["checks"]:
        value = _reproduced_value(metrics, check)
        diff = None if value is None else abs(value - check["baseline"])
        within = diff is not None and diff <= check["tolerance"] + 1e-12
        if within:
            status = "pass"
        elif check["blocking"]:
            status = "fail"
        else:
            status = "accepted_deviation"
        checks.append({**check, "reproduced": value, "abs_diff": None if diff is None else round(diff, DECIMALS),
                       "status": status})

    baseline_rows = expected["split"]["baseline_rows"]
    split_checks = {
        "n_train": [split["n_train"], baseline_rows["n_train"]],
        "n_test": [split["n_test"], baseline_rows["n_test"]],
        "train_start": [split["train_period"]["start"], baseline_rows["train_start"]],
        "train_end": [split["train_period"]["end"], baseline_rows["train_end"]],
        "test_start": [split["test_period"]["start"], baseline_rows["test_start"]],
        "test_end": [split["test_period"]["end"], baseline_rows["test_end"]],
        "train_keys_sha256": [split["train_keys_sha256"], baseline_rows["train_keys_sha256"]],
        "test_keys_sha256": [split["test_keys_sha256"], baseline_rows["test_keys_sha256"]],
    }
    split_equivalent = all(a == b for a, b in split_checks.values())
    environment = _environment(settings)

    primary = next(c for c in checks if c.get("primary"))
    failed = [c for c in checks if c["status"] == "fail"]
    summary = {
        "checks": len(checks),
        "passed": sum(c["status"] == "pass" for c in checks),
        "failed": len(failed),
        "accepted_deviations": sum(c["status"] == "accepted_deviation" for c in checks),
    }
    status = "PASS" if not failed and split_equivalent else "FAIL"
    report = {
        "status": status,
        "pipeline_run_id": run_id,
        "checked_at_utc": utc_now_iso(),
        "baseline": expected["baseline_id"],
        "tolerance_policy": expected["tolerance_policy"],
        "primary": {k: primary[k] for k in ("model", "key", "baseline", "reproduced", "abs_diff", "tolerance", "status")},
        "summary": summary,
        "split_equivalence": {"equivalent": split_equivalent,
                              "fields": {k: {"reproduced": a, "baseline": b, "equal": a == b}
                                         for k, (a, b) in split_checks.items()}},
        "environment": environment,
        "checks": checks,
    }
    (settings.outputs_dir / "reproducibility_check.json").write_text(json.dumps(report, indent=2) + "\n",
                                                                    encoding="utf-8")
    (settings.outputs_dir / "reproducibility_summary.md").write_text(render_summary(report, metrics, expected),
                                                                    encoding="utf-8")
    log.event("reproducibility", f"Reproducibility {status}: {summary['passed']} passed, {summary['failed']} failed, "
              f"{summary['accepted_deviations']} accepted deviations; primary {primary['model']} "
              f"{primary['key'].upper()} {primary['reproduced']:.6f} vs baseline {primary['baseline']} "
              f"(|diff| {primary['abs_diff']:.6f} <= {primary['tolerance']})", **summary)
    if not environment["all_pins_match"]:
        log.event("environment_drift", "Installed packages differ from requirements.txt; results may drift",
                  level=logging.WARNING)
    if status == "FAIL":
        reasons = [f"- {c['id']}: reproduced {c['reproduced']} vs baseline {c['baseline']} "
                   f"(tolerance {c['tolerance']})" for c in failed[:10]]
        if not split_equivalent:
            reasons.append("- the train/test rows differ from the baseline rows (see split_equivalence)")
        raise ReproducibilityError("Reproduction is outside the declared tolerance:\n" + "\n".join(reasons))
    return report


def render_summary(report: dict, metrics: dict, expected: dict) -> str:
    """Side-by-side Markdown table of the baseline and the reproduced metrics."""
    table2 = expected["reported"]["paper_table2_test"]
    lines = [
        f"# Reproducibility summary — {report['status']}",
        "",
        f"Run `{report['pipeline_run_id']}`, checked {report['checked_at_utc']}. "
        f"{report['summary']['passed']} of {report['summary']['checks']} checks passed, "
        f"{report['summary']['failed']} failed, {report['summary']['accepted_deviations']} accepted deviations "
        f"(documented XGBoost version effects). Train/test rows identical to the baseline: "
        f"{'yes' if report['split_equivalence']['equivalent'] else 'NO'}.",
        "",
        "## Test set (paper Table 2 vs this pipeline)",
        "",
        "| Model | Metric | Baseline | Reproduced | Abs. diff | Tolerance | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for model, values in table2.items():
        for metric, base in values.items():
            got = metrics["test"][model][metric]
            diff = abs(got - base)
            lines.append(f"| {model} | {metric.upper()} | {base:.2f} | {got:.6f} | {diff:.6f} | 0.005 | "
                         f"{'pass' if diff <= 0.005 + 1e-12 else 'FAIL'} |")
    lines += ["", "## Cross-validation (paper Table 3 vs this pipeline)", "",
              "| Model | Avg R² | Avg MSE | Avg RMSE | Avg MAE | Fold R² (reproduced) |", "|---|---|---|---|---|---|"]
    for model, base in expected["reported"]["paper_table3_cv"].items():
        avg = metrics["cv"][model]["average"]
        folds = ", ".join(f"{v:.4f}" for v in metrics["cv"][model]["folds"]["r2"])
        lines.append(f"| {model} | {base['avg_r2']:.2f} → {avg['r2']:.4f} | {base['avg_mse']:.2f} → {avg['mse']:.4f} | "
                     f"{base['avg_rmse']:.2f} → {avg['rmse']:.4f} | {base['avg_mae']:.2f} → {avg['mae']:.4f} | {folds} |")
    deviations = [c for c in report["checks"] if c["status"] == "accepted_deviation"]
    if deviations:
        lines += ["", f"## Accepted deviations ({len(deviations)}, all XGBoost)", ""]
        by_reason: dict[str, list[dict]] = {}
        for check in deviations:
            by_reason.setdefault(check["accepted_deviation"], []).append(check)
        for reason, items in by_reason.items():
            lines += [f"**{len(items)} check(s).** {reason}", ""]
            lines += [f"- `{c['id']}`: baseline {c['baseline']}, reproduced {c['reproduced']:.6f}" for c in items[:5]]
            if len(items) > 5:
                lines.append(f"- ... and {len(items) - 5} more in reproducibility_check.json")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------
def publish_outputs(settings: Settings, run_id: str, log: StageLogger, use_database: bool = True) -> dict[str, Any]:
    """Deliver predictions and metrics to PostgreSQL and write the final run summary."""
    from src.storage import apply_schema, connect

    metrics = json.loads((settings.outputs_dir / "metrics.json").read_text(encoding="utf-8"))
    repro_path = settings.outputs_dir / "reproducibility_check.json"
    repro = json.loads(repro_path.read_text(encoding="utf-8")) if repro_path.is_file() else None
    predictions = pd.read_csv(settings.outputs_dir / "predictions.csv")
    _, test = load_model_input(settings)
    published: dict[str, Any] = {"prediction_rows": int(len(predictions))}

    if use_database:
        long = predictions.melt(id_vars=["source_row_key", "reading_ts", "actual_risk"],
                                var_name="model_name", value_name="predicted_risk")
        long["model_name"] = long["model_name"].str.removeprefix("pred_")
        with connect(settings, log) as conn:
            apply_schema(conn, settings)
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("DELETE FROM serving.risk_predictions WHERE NOT (model_name = ANY(%s))",
                            (sorted(long["model_name"].unique()),))
                cur.executemany(
                    """
                    INSERT INTO serving.risk_predictions (model_name, source_row_key, reading_ts, actual_risk,
                        predicted_risk, abs_error, pipeline_run_id, published_at_utc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (model_name, source_row_key) DO UPDATE SET reading_ts = EXCLUDED.reading_ts,
                        actual_risk = EXCLUDED.actual_risk, predicted_risk = EXCLUDED.predicted_risk,
                        abs_error = EXCLUDED.abs_error, pipeline_run_id = EXCLUDED.pipeline_run_id,
                        published_at_utc = EXCLUDED.published_at_utc
                    """,
                    [(r.model_name, int(r.source_row_key), r.reading_ts, float(r.actual_risk),
                      float(r.predicted_risk), abs(float(r.actual_risk) - float(r.predicted_risk)), run_id)
                     for r in long.itertuples(index=False)])
                metric_rows = [(run_id, model, "test", metric, float(value))
                               for model, values in metrics["test"].items() for metric, value in values.items()]
                metric_rows += [(run_id, model, "cv_average", metric, float(value))
                                for model, cv in metrics["cv"].items() for metric, value in cv["average"].items()]
                cur.executemany(
                    """
                    INSERT INTO audit.model_metrics (pipeline_run_id, model_name, eval_set, metric_name, metric_value)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_run_id, model_name, eval_set, metric_name)
                    DO UPDATE SET metric_value = EXCLUDED.metric_value
                    """, metric_rows)
                if repro:
                    cur.executemany(
                        """
                        INSERT INTO audit.reproducibility_results (pipeline_run_id, check_id, source, model_name,
                            kind, metric_key, baseline, reproduced, abs_diff, tolerance, status, blocking)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (pipeline_run_id, check_id) DO UPDATE SET reproduced = EXCLUDED.reproduced,
                            abs_diff = EXCLUDED.abs_diff, status = EXCLUDED.status
                        """,
                        [(run_id, c["id"], c["source"], c["model"], c["kind"], c["key"], c["baseline"],
                          c["reproduced"], c["abs_diff"], c["tolerance"], c["status"], c["blocking"])
                         for c in repro["checks"]])
            published.update({"serving_rows": int(len(long)), "metric_rows": len(metric_rows)})

    record = RunRecord(settings, run_id)
    data = record.load()
    data["counts"].update({"train_rows": int(metrics["rows"]["train"]), "test_rows": int(len(test)),
                           "prediction_rows": int(len(predictions))})
    record.save(data)
    log.event("outputs_published", f"Published {len(predictions):,} test predictions for "
              f"{len(metrics['test'])} models" + (" to PostgreSQL" if use_database else " (files only)"),
              **published)
    return published


def write_run_summary(settings: Settings, run_id: str) -> dict[str, Any]:
    """outputs/run_summary.json: the final status and counts of the latest run."""
    data = RunRecord(settings, run_id).load()
    metrics_path = settings.outputs_dir / "metrics.json"
    repro_path = settings.outputs_dir / "reproducibility_check.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.is_file() else {}
    repro = json.loads(repro_path.read_text(encoding="utf-8")) if repro_path.is_file() else None
    summary = {
        "pipeline_run_id": run_id,
        "status": data["status"],
        "trigger": data.get("trigger"),
        "started_at_utc": data["started_at_utc"],
        "finished_at_utc": data["finished_at_utc"],
        "counts": data["counts"],
        "primary_metric": metrics.get("primary"),
        "reproducibility": None if not repro or repro.get("pipeline_run_id") != run_id
        else {"status": repro["status"], **repro["summary"]},
        "stages": {name: {"status": s["status"], "duration_s": s.get("duration_s"), "attempts": s.get("attempts")}
                   for name, s in data["stages"].items()},
    }
    (settings.outputs_dir / "run_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
