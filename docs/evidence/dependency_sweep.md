# Evidence: which library versions produced the historical numbers

**Question.** The paper (Table 2) reports XGBoost MSE **10.57**, but the saved
notebook output (cell 22) says **10.52**. Same data, same code, same seed —
so what changed?

**Method.** I replayed the original notebook logic with
[`scripts/notebook_replay.py`](../../scripts/notebook_replay.py) on the
authoritative CSV, changing **one library at a time** (Python 3.12,
pandas 3.0.5, numpy 2.5.3). Commands:

```bash
python scripts/notebook_replay.py --models xgb          # repeated per xgboost version
python scripts/notebook_replay.py --models xgb --cv
python scripts/notebook_replay.py --models rf           # repeated per scikit-learn version
```

## 1. XGBoost version (scikit-learn 1.8.0)

| xgboost | MSE | RMSE | MAE | R² | Top-3 importances (TPI / Temp / Service_Days) | Matches |
|---|---|---|---|---|---|---|
| 2.0.3 | 10.566594 | 3.250630 | 2.587040 | 0.957334 | 0.474229 / 0.239888 / 0.235767 | paper Table 2 + Section 6 |
| 2.1.4 | 10.566594 | 3.250630 | 2.587040 | 0.957334 | 0.474229 / 0.239888 / 0.235767 | paper |
| 3.0.5 | 10.566594 | 3.250630 | 2.587040 | 0.957334 | 0.474229 / 0.239888 / 0.235767 | paper |
| 3.1.3 | 10.566594 | 3.250630 | 2.587040 | 0.957334 | 0.474229 / 0.239888 / 0.235767 | paper |
| **3.2.0** | **10.566594** | **3.250630** | **2.587040** | **0.957334** | 0.474229 / 0.239888 / 0.235767 | **paper (pinned)** |
| 3.3.0 | 10.518032 | 3.243152 | 2.579469 | 0.957530 | 0.454930 / 0.264415 / 0.228868 | notebook cells 22 + 32 |
| 3.4.1 | 10.518032 | 3.243152 | 2.579469 | 0.957530 | 0.454930 / 0.264415 / 0.228868 | notebook |

The notebook's importances (0.454930, 0.264415, 0.228868) are matched to all
six printed decimals by xgboost ≥ 3.3.0, and the paper's (0.474, 0.240, 0.236)
by xgboost ≤ 3.2.0. Colab upgraded XGBoost between the run used for the paper
and the final notebook run.

## 2. XGBoost cross-validation (5-fold TimeSeriesSplit)

| Source | Fold R² | Avg MSE | Avg RMSE | Avg MAE | Avg R² | Fold-1 MSE |
|---|---|---|---|---|---|---|
| xgboost 3.2.0 | 0.746, 0.936, 0.941, 0.946, 0.949 | 16.509 | 3.880 | 3.049 | 0.904 | 39.590 |
| xgboost 3.3.0 | 0.719, 0.936, 0.941, 0.946, 0.949 | 17.337 | 3.944 | 3.094 | 0.898 | 43.737 |
| Paper Table 3 | 0.75, 0.94, 0.94, 0.95, 0.95 | 17.34 | 3.94 | 3.09 | 0.90 | 43.74 (Section 5 text) |

The paper's fold-1 R² (0.75) comes from the 3.2.0 run while its averages and
the fold-1 MSE quoted in the text (43.74) come from the 3.3.0 run. With an MSE
of 43.74 the fold-1 R² would be 0.72, not 0.75, so **Table 3 mixes two runs**.
No single environment can reproduce every XGBoost number in the paper.

## 3. scikit-learn version (xgboost 3.2.0)

| scikit-learn | MSE | RMSE | MAE | R² | Top-3 importances | Matches notebook to 6 decimals |
|---|---|---|---|---|---|---|
| 1.6.1 | 10.469440 | 3.235651 | 2.573677 | 0.957726 | 0.486817 / 0.284977 / 0.170548 | yes |
| 1.7.2 | 10.469440 | 3.235651 | 2.573677 | 0.957726 | 0.486817 / 0.284977 / 0.170548 | yes |
| **1.8.0** | **10.469440** | **3.235651** | **2.573677** | **0.957726** | 0.486817 / 0.284977 / 0.170548 | **yes (pinned)** |
| 1.9.0 | 10.482664 | 3.237694 | 2.574770 | 0.957673 | 0.486816 / 0.284977 / 0.170554 | no |
| 1.9.1 | 10.482664 | 3.237694 | 2.574770 | 0.957673 | 0.486816 / 0.284977 / 0.170554 | no |

A separate fold-1 probe gave the same split: scikit-learn ≤ 1.8.0 reproduces
the notebook's fold-1 MSE `37.45378082` exactly, 1.9.0 gives `37.45447984`.
Note that 1.9.x still rounds to the paper's 3.24, so the 2-decimal paper alone
would never have revealed this change; the 8-decimal notebook output did.

## 4. Decision

* Pin **xgboost 3.2.0**: it reproduces every number of paper Table 2 (the
  baseline of record) and the paper's XGBoost importances.
* Pin **scikit-learn 1.8.0**: the newest version that reproduces the
  notebook's Random Forest and Linear Regression outputs exactly.
* The notebook's XGBoost outputs and the paper's XGBoost CV averages are
  recorded as *accepted deviations* in `metadata/expected_metrics.json`, each
  with this explanation.

## 5. Two more nondeterminism checks (pinned versions)

| Check | Result |
|---|---|
| XGBoost predictions with `n_jobs` = 1, 2, 4, 8 | bit-identical (same SHA-256 of the prediction array) |
| Random Forest `predict()` with `n_jobs=-1`, called 4 times | 4 *different* hashes (threads add tree outputs in a different order) |
| Random Forest `predict()` with `n_jobs=1`, called 3 times | identical |

So the pipeline fits the forest with all cores (fitting is deterministic with a
fixed seed) but always predicts with `n_jobs=1`.
