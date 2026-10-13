# Reproducibility summary — PASS

Run `af_manual__2026-10-13T14-15-30.472887-00-00`, checked 2026-10-13T14:16:39.223Z. 130 of 156 checks passed, 0 failed, 26 accepted deviations (documented XGBoost version effects). Train/test rows identical to the baseline: yes.

## Test set (paper Table 2 vs this pipeline)

| Model | Metric | Baseline | Reproduced | Abs. diff | Tolerance | Result |
|---|---|---|---|---|---|---|
| linear_regression | MSE | 11.92 | 11.918664 | 0.001336 | 0.005 | pass |
| linear_regression | RMSE | 3.45 | 3.452342 | 0.002342 | 0.005 | pass |
| linear_regression | MAE | 2.75 | 2.749196 | 0.000804 | 0.005 | pass |
| linear_regression | R2 | 0.95 | 0.951875 | 0.001875 | 0.005 | pass |
| random_forest | MSE | 10.47 | 10.469440 | 0.000560 | 0.005 | pass |
| random_forest | RMSE | 3.24 | 3.235651 | 0.004349 | 0.005 | pass |
| random_forest | MAE | 2.57 | 2.573677 | 0.003677 | 0.005 | pass |
| random_forest | R2 | 0.96 | 0.957726 | 0.002274 | 0.005 | pass |
| xgboost | MSE | 10.57 | 10.566594 | 0.003406 | 0.005 | pass |
| xgboost | RMSE | 3.25 | 3.250630 | 0.000630 | 0.005 | pass |
| xgboost | MAE | 2.59 | 2.587040 | 0.002960 | 0.005 | pass |
| xgboost | R2 | 0.96 | 0.957334 | 0.002666 | 0.005 | pass |

## Cross-validation (paper Table 3 vs this pipeline)

| Model | Avg R² | Avg MSE | Avg RMSE | Avg MAE | Fold R² (reproduced) |
|---|---|---|---|---|---|
| linear_regression | 0.93 → 0.9340 | 11.98 → 11.9805 | 3.46 → 3.4612 | 2.76 → 2.7582 | 0.9242, 0.9302, 0.9337, 0.9393, 0.9425 |
| random_forest | 0.91 → 0.9073 | 15.93 → 15.9285 | 3.82 → 3.8221 | 2.99 → 2.9918 | 0.7598, 0.9388, 0.9414, 0.9471, 0.9496 |
| xgboost | 0.90 → 0.9037 | 17.34 → 16.5090 | 3.94 → 3.8798 | 3.09 → 3.0491 | 0.7461, 0.9359, 0.9410, 0.9464, 0.9494 |

## Accepted deviations (26, all XGBoost)

**3 check(s).** Paper Table 3 mixes two runs: its XGBoost fold R2 values match xgboost <= 3.2.0 while its XGBoost averages (17.34 / 3.94 / 3.09) match xgboost >= 3.3.0. One environment cannot reproduce both; the pinned 3.2.0 reproduces Table 2 and the fold R2 values.

- `paper.table3.xgboost.avg_mse`: baseline 17.34, reproduced 16.509041
- `paper.table3.xgboost.avg_rmse`: baseline 3.94, reproduced 3.879801
- `paper.table3.xgboost.avg_mae`: baseline 3.09, reproduced 3.049060

**23 check(s).** Produced by xgboost >= 3.3.0 in the final Colab run. The pipeline pins xgboost 3.2.0 to reproduce the paper (baseline of record). Replaying the notebook with xgboost 3.3.0/3.4.1 reproduces these exact values, so the difference is a library version, not a data or preprocessing change.

- `notebook.test.xgboost.mse`: baseline 10.52, reproduced 10.566594
- `notebook.test.xgboost.rmse`: baseline 3.24, reproduced 3.250630
- `notebook.test.xgboost.mae`: baseline 2.58, reproduced 2.587040
- `notebook.cv.xgboost.r2_fold1`: baseline 0.71945524, reproduced 0.746053
- `notebook.cv.xgboost.mae_fold1`: baseline 5.01057468, reproduced 4.790379
- ... and 18 more in reproducibility_check.json
