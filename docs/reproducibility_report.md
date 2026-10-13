# Reproducibility report

**DSS150P Fundamentals of Data Engineering — Integrated Individual Laboratory Activity #4:
Reproduction of a Predictive Analytics Project**

| Field | Student entry |
|---|---|
| Name | Abad, Rolando Alfonso |
| Student number | *(2022136475)* |
| Section | *(CM17)* |
| Previous Predictive Analytics group / project title | Risma, V. L. S.; Abad, R. A.; Toliba, M. G. — *Integrating IIoT Telemetry and Machine Health Indicators for Dynamic Fire Hazard Assessment* |
| Dataset name / source | *Industrial Fire and Accident Risk Predictor* (Kaggle, publisher "joganimitesh"), file `industrial_fire_risk_data.csv`, SHA-256 `5db607c5…14f309ba` |
| Predictive task | **Regression**: predict the continuous fire-risk score `Risk` (0–100), evaluated on a chronological (time-ordered) holdout |
| Repository | `DSS150P_REPRO_ABAD_ROLANDO` — GitHub URL: *(fill in)* |

---

## 1. Result at a glance

* **The reproduction passes.** The primary metric, Random Forest RMSE on the
  chronological test set, is **3.235651** against **3.24** in the paper
  (|difference| 0.0043, tolerance 0.005). All 12 test metrics of paper
  Table 2 are within tolerance.
* **Same rows as the baseline.** The training and test sets have the same
  row counts, the same time boundaries and the same SHA-256 fingerprint of
  their timestamps as the rows the original notebook used.
* **156 automatic checks** compare `outputs/metrics.json` with
  `metadata/expected_metrics.json`: **130 pass, 0 fail, 26 accepted
  deviations**. All 26 deviations are XGBoost values that came from a newer
  XGBoost release that Colab installed between two of the group's runs
  (section 5).
* **Deterministic.** Two full runs, a Docker run and a native run, and a CLI
  run and an Airflow run all produce byte-identical `metrics.json`,
  `predictions.csv`, `feature_importance.csv`, `split_manifest.json`,
  model card and model files.

The machine-readable comparison is `outputs/reproducibility_check.json`.
`outputs/reproducibility_summary.md` is a readable version of it, and the
`audit.reproducibility_results` / `audit.v_metric_comparison` tables hold the
same comparison in PostgreSQL. The pipeline generates all of them; no number
in this report was typed in by hand.

## 2. What had to be reproduced (baseline)

[`docs/baseline_capture.md`](baseline_capture.md) records the full baseline.
In short:

| Item | Baseline |
|---|---|
| Target | `Risk` (regression, 0–100) |
| Data | 100,000 readings (every 15 minutes, 2021-01-01 → 2023-11-08); 2,000 rows with a blank `Workers` are dropped → 98,000 rows |
| Features | 26, in the notebook order: 10 numeric sensor/staff fields, 4 temporal features, 3 binary encodings, 6 one-hot columns (first category dropped), 3 interaction terms. `Accident` is excluded because it is derived from `Risk` |
| Split | chronological: first 80 % train (78,400 rows), last 20 % test (19,600 rows) |
| Models | LinearRegression (defaults); RandomForestRegressor(n_estimators=100, random_state=42); XGBRegressor(n_estimators=100, random_state=42) |
| Evaluation | MSE, RMSE, MAE, R² on the test set (paper Table 2); 5-fold `TimeSeriesSplit` (Table 3); feature importances (Table 4 / Section 6) |
| Primary metric | Random Forest RMSE = **3.24** |

## 3. Prior project vs. engineered reproduction

| Aspect | Prior project (two Colab notebooks) | This repository |
|---|---|---|
| Source | uploaded by hand; the copy in the project zip had been re-saved by Excel (different bytes, `DD/MM/YYYY` dates) | the original Kaggle file in `data/source/`, SHA-256 checked before every run (`metadata/source_manifest.yml`) |
| Execution | run cells top to bottom by hand; state hidden in the notebook | one command (`python -m src.cli run-all`) or an Airflow DAG with 10 tasks; every stage is a function in `src/` |
| Environment | Colab, versions not recorded (XGBoost changed between runs) | Python 3.12, exact pins in `requirements.txt`, two Docker images, `env-check --strict` |
| Data layers | one DataFrame in memory | source → raw (12 quarterly batches) → staging → curated → model input → outputs, plus quarantine, exclusions and month partitions |
| Cleaning | `dropna` on `Workers` inside the notebook | contract-driven staging (types, formats, ranges, categories, business rule, quarantine) and the baseline rule applied in curated, with every excluded row listed |
| Storage | nothing kept after the session | Parquet layers, PostgreSQL schemas `staging`, `curated`, `serving`, `audit` |
| Split | `iloc[:80%]`, which only works because the file happens to be sorted | explicit `ORDER BY reading_ts`, split manifest with boundaries and key hashes, overlap and order checks |
| Metrics | printed with 2 decimals and copied into the paper | `metrics.json` with 10 decimals, compared automatically with the baseline file |
| Quality | none | 89 data-quality checks per run (raw 7, staging 44, curated 12, published curated 20, model input 6) and 54 automated tests |
| Observability | none | structured JSON logs, a run record per run, audit tables, error records |
| Outputs | tables in the paper | `metrics.json`, `predictions.csv`, `feature_importance.csv`, model files + model card, `serving.risk_predictions` |

## 4. Metric consistency

### 4.1 Test set — paper Table 2 vs. this pipeline

| Model | Metric | Paper | Reproduced | Abs. diff | Tolerance | Result |
|---|---|---|---|---|---|---|
| Linear Regression | MSE | 11.92 | 11.918664 | 0.001336 | 0.005 | pass |
| Linear Regression | RMSE | 3.45 | 3.452342 | 0.002342 | 0.005 | pass |
| Linear Regression | MAE | 2.75 | 2.749196 | 0.000804 | 0.005 | pass |
| Linear Regression | R² | 0.95 | 0.951875 | 0.001875 | 0.005 | pass |
| Random Forest | MSE | 10.47 | 10.469440 | 0.000560 | 0.005 | pass |
| **Random Forest** | **RMSE** | **3.24** | **3.235651** | **0.004349** | **0.005** | **pass (primary)** |
| Random Forest | MAE | 2.57 | 2.573677 | 0.003677 | 0.005 | pass |
| Random Forest | R² | 0.96 | 0.957726 | 0.002274 | 0.005 | pass |
| XGBoost | MSE | 10.57 | 10.566594 | 0.003406 | 0.005 | pass |
| XGBoost | RMSE | 3.25 | 3.250630 | 0.000630 | 0.005 | pass |
| XGBoost | MAE | 2.59 | 2.587040 | 0.002960 | 0.005 | pass |
| XGBoost | R² | 0.96 | 0.957334 | 0.002666 | 0.005 | pass |

### 4.2 Cross-validation — paper Table 3 vs. this pipeline

| Model | Avg R² | Avg MSE | Avg RMSE | Avg MAE | Fold R² (reproduced) |
|---|---|---|---|---|---|
| Linear Regression | 0.93 → 0.9340 | 11.98 → 11.9805 | 3.46 → 3.4612 | 2.76 → 2.7582 | 0.9242, 0.9302, 0.9337, 0.9393, 0.9425 |
| Random Forest | 0.91 → 0.9073 | 15.93 → 15.9285 | 3.82 → 3.8221 | 2.99 → 2.9918 | 0.7598, 0.9388, 0.9414, 0.9471, 0.9496 |
| XGBoost | 0.90 → 0.9037 | 17.34 → *16.5090* | 3.94 → *3.8798* | 3.09 → *3.0491* | 0.7461, 0.9359, 0.9410, 0.9464, 0.9494 |

The three values in *italics* are accepted deviations (section 5.2). All 15
fold R² values in Table 3 are reproduced, the XGBoost ones included.

### 4.3 All 156 checks

| Baseline source | Checks | Result | Largest difference |
|---|---|---|---|
| Paper Table 2 (test metrics, all models) | 12 | 12 pass | 0.0043 |
| Paper Table 3 (fold R², averages) | 27 | 24 pass, 3 accepted deviations | 0.0042 (passing checks) |
| Paper Table 4 / Section 6 (importances) | 8 | 8 pass | 0.00045 |
| Notebook, Linear Regression + Random Forest CV folds (8 decimals) | 40 | 40 pass | 0.000000005 |
| Notebook, Random Forest importances (6 decimals) | 15 | 15 pass | 0.00000048 |
| Notebook, Linear Regression coefficients (6 decimals) | 15 | 15 pass | 0.00000049 |
| Notebook, XGBoost outputs (non-blocking) | 39 | 16 pass, 23 accepted deviations | — |
| **Total** | **156** | **130 pass, 0 fail, 26 accepted deviations** | |

**Tolerance rule.** A reproduced value must agree with the baseline to the
precision the baseline was *reported* with: half a unit of the last reported
decimal, so ±0.005 for the paper's 2-decimal numbers and ±0.0005 for values
reported with 3 or more decimals. This is stricter than the ±0.01 the lab
recommends. 114 checks are *blocking*: if one of them fails, the
`reproducibility_check` stage stops the pipeline with exit code 6 and nothing
is published. The 42 XGBoost checks that belong to the other XGBoost release
are *non-blocking*. They are still computed and reported, each with its
reason.

**Interpretation (lab section 10.2).** The outcome is *matches within
tolerance*. The non-zero differences in Tables 2 and 3 are rounding: the paper
printed 2 decimals, so 3.235651 appears as 3.24. For Linear Regression and
Random Forest, where the group's notebook kept 6–8 decimals, the pipeline
matches to about 0.0000005 or better. That precision would be impossible if
the rows, the preprocessing or the split were different.

## 5. Deviations from the prior project and why

### 5.1 The source file

The notebook read a copy of the CSV that had been opened and saved in a
spreadsheet program. That copy has dates like `08/11/2023`, times like
`0:00:00` and whole numbers without `.0`, so it has a different SHA-256
(`6457d041…`). The notebook hard-codes `format="%d/%m/%Y %H:%M:%S"`, which
**fails on the original Kaggle file**. I compared both copies cell by cell
([evidence](evidence/source_copies_comparison.md)). All 100,000 records are
identical; only the text formatting differs. The pipeline uses the original
file and parses it with the formats declared in the data contract. This is
why the notebook cannot be "just rerun" on the real source, and why the
checksum check exists.

### 5.2 The XGBoost version (26 accepted deviations)

The paper's XGBoost numbers (MSE 10.57) and the notebook's saved output (MSE
10.52) do not agree. I replayed the notebook logic with seven XGBoost
releases ([evidence](evidence/dependency_sweep.md)). Releases ≤ 3.2.0
reproduce the paper exactly, and ≥ 3.3.0 reproduce the notebook exactly. Colab
upgraded XGBoost between the run used for the paper and the final notebook
run. The pipeline pins **xgboost 3.2.0**, because the paper is the baseline of
record. Two groups of values cannot be reproduced at the same time:

* 23 notebook XGBoost values (test metrics, CV folds, importances) come from
  ≥ 3.3.0;
* 3 XGBoost averages in paper Table 3 (17.34 / 3.94 / 3.09) also come from
  ≥ 3.3.0, while the fold R² values printed in the same table come from
  3.2.0. **Table 3 mixes two runs**: with the fold-1 MSE of 43.74 quoted in
  the text, fold 1 would have R² 0.72, not the 0.75 in the table.

These 26 checks are recorded as non-blocking *accepted deviations*, each with
this explanation in `expected_metrics.json`. A data or preprocessing problem
would also move the Linear Regression and Random Forest results, and those
match to 7–9 decimals.

### 5.3 Other differences (none of them changes a result)

| # | Prior project | This pipeline | Why |
|---|---|---|---|
| 1 | scikit-learn version unknown | pinned to 1.8.0 | 1.9.x changes Random Forest results slightly (MSE 10.4694 → 10.4827), which the 8-decimal notebook output reveals |
| 2 | feature names like `Service_Days` | lower case (`service_days`), same order | SQL-friendly names; the order is kept because tree models break ties by column position |
| 3 | `pd.get_dummies(drop_first=True)` on the whole dataset | encodings from the categories declared in the data contract | identical columns (unit-tested), but nothing is learned from the data, so no test information can leak into training |
| 4 | `cross_val_score` called three times per model (15 fits) | one fit per fold, all four metrics computed from it | same folds and same results (within 0.000000005); 3× less training |
| 5 | training read the cleaned CSV | training reads `curated.fire_risk_features` from PostgreSQL, `ORDER BY reading_ts, source_row_key` | the model provably uses the same rows that are served; `PIPELINE_MODEL_INPUT=parquet` reads the curated file instead and gives identical results |
| 6 | models never saved | scikit-learn models as joblib, XGBoost in its native JSON format | a pickled XGBoost model trained inside Airflow referenced the `dill` package and could not be loaded elsewhere (fixed in commit `fix: save XGBoost models in the native JSON format`) |
| 7 | — | generated text files always use LF line endings | otherwise Windows runs would produce different bytes for the same numbers |
| 8 | no validation | contract checks in every layer | for the real source nothing is quarantined, so the checks do not change the modelling data |

## 6. Deterministic controls

| Source of variation | Control in this repository | Evidence |
|---|---|---|
| Which file is used | SHA-256 + size check against the manifest before every run; raw batches carry the checksum | `verify-source`; failure drill 1 |
| Library versions | exact pins, Docker images, `env-check --strict`; versions recorded in `metrics.json`, the model card and the reproducibility report | dependency sweep |
| Configuration | every path, seed, hyperparameter, feature and the split fraction in `config/settings.yml` (versioned) | `env-check` prints the resolved configuration |
| Row order | curated layer sorted by `reading_ts`; SQL reads `ORDER BY reading_ts, source_row_key`; raw batches sorted by `source_row_key` | `ordered_by_time` quality check |
| Split | chronological rule plus `metadata/split_manifest.json` (counts, boundaries, key hashes, matrix hashes); the reproducibility stage fails if the split differs from the baseline | `split_equivalence` in `reproducibility_check.json` |
| Random seeds | `random_state=42` for both tree models, plus `random.seed` and `numpy.random.seed` | model card |
| Threads | Random Forest predictions use `n_jobs=1` (with several threads the trees are summed in a different order and the last bits change); XGBoost predictions are identical for 1, 2, 4 and 8 threads | dependency sweep, section 5 |
| Feature order | fixed list in `settings.yml`; checked against the model input | `feature_order` check, `test_feature_matrix_uses_the_configured_order` |
| Leakage | `risk` and `accident` can never be features; train/test overlap check; encodings fixed before the split | model-input quality checks, `test_predictive_integrity_detects_leakage_and_overlap` |
| Number formatting | metrics rounded to 10 decimals, predictions to 6, LF line endings | byte-identical reruns |
| Model files | joblib for scikit-learn, native JSON for XGBoost; SHA-256 of each file in the model card | model card |

**How I checked it.** Two full runs in a row gave byte-identical outputs. So
did a run in Docker and a native run, a CLI run and an Airflow run, and every
run in the Airflow evidence (`docs/evidence/airflow_runs.md` checks the hashes
before and after four DAG runs). The integration test
`test_pipeline_is_deterministic_and_the_reproducibility_gate_works` repeats
this on a small sample every time the tests run.

## 7. Clean-clone verification

To check acceptance test A1 I followed the README exactly as a reviewer
would, in an empty folder and with a separate Docker Compose project, so
nothing from my working copy could leak in:

| Step | What I did | Result |
|---|---|---|
| 1 | `git clone` of the repository | the source CSV and `.env` are not in the clone (git-ignored) |
| 2 | put the downloaded CSV in `data/source/`, `python -m src.cli verify-source` | SHA-256 matches the manifest |
| 3 | `cp .env.example .env`, changed only the two passwords, the two host ports and `AIRFLOW_UID` | no code or config file edited |
| 4 | `docker compose build --no-cache airflow pipeline`, `docker compose up -d` | both images rebuilt from the Dockerfiles in about 2.5 minutes (base images already downloaded); PostgreSQL and Airflow healthy |
| 5 | triggered `predictive_repro_pipeline` | DAG run `success` after about 4.5 minutes |
| 6 | `git status` | only `run_summary.json`, `reproducibility_check.json` and `reproducibility_summary.md` changed (run id and time) |
| 7 | compared SHA-256 with my working copy | `metrics.json`, `predictions.csv`, `feature_importance.csv`, `split_manifest.json`, `model_card.json` **and all three model files** byte-identical |
| 8 | reproducibility check | `PASS` (130 passed, 0 failed, 26 accepted deviations), split equivalent to the baseline |
| 9 | PostgreSQL | staging 100,000 rows, curated 98,000, 0 duplicate keys, 58,800 served predictions, run status `success` |
| 10 | tests and environment | 52 passed + 1 skipped in the pipeline image, 2 DAG tests passed in the Airflow image; `env-check --strict` reports every pin installed |

## 8. Known limitations

1. **Licence not confirmed.** I could not verify the Kaggle licence, so the
   CSV is not in the repository. A reviewer has to download it; the checksum
   proves it is the same file. If the Kaggle page disappeared, the group's
   copy would have to be shared directly.
2. **The incremental arrival is simulated.** The timestamps are real, but the
   file arrives all at once. Quarterly batches on `reading_ts` stand in for
   data arriving over time (section 9, question 4).
3. **One environment cannot reproduce every XGBoost number** of the old
   project (section 5.2).
4. **Bit-identical results are guaranteed only on the same platform.** I
   tested Linux x86-64 (Docker and native). On another CPU architecture, for
   example an Apple Silicon Mac running arm64 images, the last bits of
   floating-point results can differ. The tolerance covers such differences,
   but the byte-identical comparisons would not hold.
5. **Model files are not committed** (the Random Forest is about 150 MB). They
   are recreated by `train`, and their SHA-256 in the model card shows
   whether they are identical.
6. **Benchmark timings depend on the machine.** Only the ordering of the
   formats is used for conclusions.
7. **The Airflow setup is for local use.** It uses `airflow standalone` with
   every visitor as admin and no login, plus one PostgreSQL server for both
   the pipeline and Airflow's metadata. That is fine on a laptop but not
   anywhere shared.
8. **The data is synthetic** (a Kaggle simulation). The pipeline reproduces
   the model faithfully, but that does not make the model valid for real
   plants.

## 9. Answers to the analysis and reflection questions

**1. What prevented another person from reproducing the previous project immediately?**
Several things together. The notebook depended on a CSV that someone had to
upload by hand, and nothing recorded which file it was. The copy we used had
been re-saved by Excel, and the notebook's hard-coded date format crashes on
the original Kaggle file. No library versions were recorded, and Colab
upgraded XGBoost between two of our own runs, so even we could not get the
same XGBoost numbers twice. The notebooks kept state across cells (`df` was
overwritten and changed in place), so running cells out of order gave
different results. The split relied on the file happening to be sorted by
time. Finally, the results existed only as 2-decimal printouts copied into
the paper by hand, so there was nothing precise to compare against, and
Table 3 ended up mixing two runs.

**2. Which artifact is the true authoritative source?**
The original file in `data/source/`, identified by its SHA-256 in
`source_manifest.yml`, and nothing else. The **raw** layer is a faithful copy
of it, split into arrival batches and stamped with ingestion metadata (run
id, time, file name, checksum). It is evidence of what was ingested and when,
and can be rebuilt at any time. **Staging** is the same data typed and
validated against the contract, one row per reading: the technical truth that
any consumer can use. **Curated** is the business data product: the
baseline's cleaning rule, derived features, partitions, loaded into PostgreSQL
for analysts and the model. The **model-ready** train/test matrices belong to
one model configuration and are the most disposable. All four can be deleted
(`python -m src.cli clean --yes`) and rebuilt identically from the source.

**3. What identifier or rule prevents duplicates, and what assumption makes it valid?**
The business key is `reading_ts` (Date + Time): the profile shows exactly
one reading per 15-minute slot, and staging, curated and PostgreSQL
(`UNIQUE (reading_ts)`) all enforce it. The technical key is `source_row_key`,
the row's position in the immutable file. It is the primary key of the
tables and how ingestion knows which rows are already committed. A
`record_hash` (SHA-256 of the 20 raw values) and a `row_hash` detect changed
content. The rule only works under two assumptions. First, the source is
immutable, so a row number always points to the same record; the checksum
check guarantees this, and a changed file forces a full refresh. Second, the
plant produces one reading per time slot. If two factories ever reported for
the same slot, the key would have to become `(factory, reading_ts)`.
Conflicting rows with the same timestamp are quarantined rather than guessed.

**4. How was incremental ingestion simulated without pretending the checkpoint is a business timestamp?**
This dataset *does* have a reliable event timestamp, so I used the lab's
first option: time-ordered batches, one per calendar quarter of `reading_ts`
(12 batches), with the latest committed `reading_ts` as the watermark. What
is simulated is the *arrival*: the whole file exists from the start, and
`--max-batches` / `--fail-after-batches` only pretend that quarters arrive or
that a run breaks. The contract, the manifest and the README say this is a
lab simulation. The checkpoint records batch ids and the watermark of
*committed* data, not when data actually arrived. Freshness is never claimed
from it: for a static snapshot, the "freshness" check is the checksum.

**5. Which transformations belong in staging and which in curated?**
Staging holds technical, source-aligned work that any consumer would want and
that involves no business decision. That means combining Date + Time into a
timestamp, explicit type conversion, snake_case names, trimming,
standardising nulls, checking formats, categories and ranges, enforcing the
cadence and the Accident/Risk rule, handling duplicates, quarantining with
reason codes, and adding audit columns. Staging keeps every valid row, even
rows with a blank `Workers`. Curated holds the decisions of *this* data
product: the baseline's listwise deletion of blank `Workers` (each excluded
row is listed in `curated_exclusions.csv`), the derived temporal and
interaction features, the partition keys and a deterministic order. One-hot
and binary encodings are applied later, in the model input, because they
belong to one model's feature contract. That way the curated table stays
readable for an analyst (`Factory_B`, not `0/1`). The split means another
consumer could start from staging and make different business choices.

**6. Which storage format performed best, and why should the result not be generalised?**
For this workload Parquet was best almost everywhere: 5.2 MB against
16.1 MB (CSV) and 52.9 MB (JSON Lines), a full typed read in about 0.025 s
against 0.20 s and 1.15 s, and the fastest writes. PostgreSQL was the best
for selective analytical questions (`Factory_B` since 2023 in 0.048 s through
an index, against 0.70 s for the full table). Reading one month partition
touched 1 of 35 files. This should not be generalised. It was one machine
with 2 CPUs and a warm cache, a small and narrow table (98,000 rows), a
database on the same machine, and particular pandas/pyarrow versions and
codec. Workloads with frequent updates, many concurrent users, nested
records or much more data would rank the formats differently. Details are
in [`benchmark_report.md`](benchmark_report.md).

**7. What happens if Airflow retries the load task after the database committed but the task did not get the confirmation?**
The retry runs the whole load again against a database that already has the
data. With a plain `INSERT`/append that would double every row, or crash on
the unique constraint, depending on the table. In this design the retry
finds every row already present and identical, so it changes nothing. The
load runs inside one transaction, so the first attempt either committed
completely or not at all; a half-loaded table cannot exist.

**8. How does the design make that retry safe?**
`load_postgres` treats every load as a *snapshot merge* keyed on
`source_row_key`, with `UNIQUE (reading_ts)` as a second guard. It copies the
snapshot into a temporary table with `COPY`. It deletes rows that are no
longer in the snapshot (or whose business key changed). Then it runs
`INSERT ... ON CONFLICT (source_row_key) DO UPDATE ... WHERE (row values) IS
DISTINCT FROM (new values)`. Identical rows are therefore not even rewritten.
Run-specific audit columns (`pipeline_run_id`, timestamps) are left out of
the comparison, so a rerun does not count as a change. A second load reports
**0 inserted, 0 updated, 0 deleted**. The integration test
`test_postgres_load_is_rerun_safe` and Airflow scenario 2 both show this. The
audit sync and the publish step are upserts too.

**9. Which data-quality check is most likely to detect a change that would silently damage model accuracy?**
For this project, the **timestamp checks**: parsing with the exact contract
format, the 15-minute cadence and the unique business key. Our own history
shows why. An Excel re-save turned `2023-11-08` into `08/11/2023`. A lenient
parser can silently swap day and month for dates up to the 12th. That would
scramble the chronological order, move rows across the train/test boundary
and corrupt the `Month`/`Day_of_Week` features, all without any error. The
strict format quarantines such rows, the cadence and uniqueness checks catch
shifted or duplicated slots, and the split-equivalence check catches anything
that still gets through. A close second is the **null-rate check on
`Workers`**. Because the baseline drops incomplete rows, a jump in blanks
would quietly shrink and bias the training data, so the contract stops the
run above 5 %.

**10. How do you know the training and test rows are the same logical rows as in the baseline?**
I replayed the notebook's exact rule on the source: drop blank `Workers`,
keep file order, first 80 % for training. I recorded the counts, the
first/last timestamps of each side and a SHA-256 of each side's sorted
timestamps in `expected_metrics.json`. Every run writes the same fields to
`metadata/split_manifest.json`, and `reproducibility_check` compares all
eight. A mismatch fails the run, even if the metrics happened to be close.
Today the result is `equivalent: true`. On top of that, the Linear
Regression coefficients and Random Forest importances match the notebook to
about 0.0000005. With different rows that would be practically impossible.

**11. What sources of nondeterminism remain?**
(a) *Floating-point differences across platforms*: another CPU architecture
or BLAS/LAPACK build can change the last bits of the Linear Regression
solution and the tree split thresholds. This is covered by the tolerance and
not by byte equality. (b) *Unpinned installs*: someone who ignores
`requirements.txt` gets other scikit-learn/XGBoost releases, which the
dependency sweep shows do change results; `env-check --strict` and the
recorded versions make this visible. (c) *Threads*: Random Forest fitting
uses all cores but is deterministic because the per-tree seeds are drawn up
front. Prediction with several threads is not (the summation order changes),
so it is pinned to one thread. (d) *GPU XGBoost* would not be bit-identical
to the CPU (not used). (e) Non-model outputs naturally change: run ids, times
and benchmark timings.

**12. If the reproduced metric differs, how do you find whether the cause is data, preprocessing, split, dependencies, parameters or nondeterminism?**
I go from the cheapest and most certain check to the most expensive. I
change one thing at a time, which is how I found the XGBoost version
problem.
1. *Data*: `verify-source` (checksum), then row counts per layer in the run
   record and the table fingerprints.
2. *Split*: `split_equivalence` in `reproducibility_check.json`. Different
   key hashes mean the cause is upstream of the model.
3. *Preprocessing*: `train_matrix_sha256` / `test_matrix_sha256` against a
   known-good run. Same rows but different matrices means a different
   encoding, feature order or derived feature.
4. *Dependencies*: `env-check --strict` and the `libraries` block in
   `metrics.json`; then re-run with one library version changed at a time
   (`scripts/notebook_replay.py`).
5. *Parameters*: the model card against `settings.yml`, and
   `git diff config/settings.yml`.
6. *Nondeterminism*: run twice in the *same* environment. If the metric
   changes between identical runs, the cause is the execution itself, for
   example threads.

**13. What would you monitor if the pipeline ran every day?**
Run health: DAG and task status, duration, retries and failures, from Airflow
and `audit.stage_runs` / `audit.pipeline_errors`, with alerts from the
failure callback. Freshness: time since the last successful run and since new
source data arrived. Volume: rows per layer against the expected or previous
values. Quality: the quarantine rate, failed and warning checks, and the
`Workers` null rate. Drift: the distributions of key features (`Temp`,
`Pressure`, `Service_Days`) and of `Risk`, and the share of predictions above
75. Model quality: RMSE on newly labelled readings against the 3.24 baseline.
Integrity: duplicate counts and file/database reconciliation. Resources:
database size, load time and disk use.

**14. Which part of the architecture would become the first bottleneck with 100× more data?**
In-memory processing with pandas on one machine. Ingestion reads the whole
CSV as text, and every stage loads its complete input layer. Ten million rows
(several GB) would run out of memory long before PostgreSQL struggles. Close
behind: the full-snapshot pattern, where every run rebuilds staging and
curated and merges the *whole* table into PostgreSQL. Then the `executemany`
insert of predictions, Random Forest training and 5-fold cross-validation on
one CPU. My first changes would be to process and load only new partitions
(downstream incremental), stream with pyarrow or Polars (or Spark), and use
`COPY` for the predictions.

**15. What would change if the dataset became sensitive or regulated?**
I would keep the data out of Git (already the case) and add more. Encryption
in transit (TLS to PostgreSQL) and at rest (encrypted volumes and backups).
Secrets in a secret manager instead of `.env`. Least-privilege database roles
(a loader, a read-only analyst role, no superuser). Real authentication and
role-based access in Airflow instead of "everyone is admin". Data
minimisation and pseudonymisation in staging, for example hashing any worker
identifiers and keeping only needed columns. Retention limits and restricted
access for `raw/` and `quarantine/`, because the quarantine file keeps raw
values. Logs that never contain row values. A classification field in the
data contract. A check of the legal basis and licence, in the Philippines
under the Data Privacy Act of 2012. Access to the data would itself be
audited.

## 10. Technical acceptance tests (lab section 11)

| ID | Acceptance test | How it was verified | Evidence | Result |
|---|---|---|---|---|
| A1 | Repository clones and README setup is sufficient | fresh clone → place CSV → `.env` → build → run → outputs identical (section 7) | section 7 of this report | pass |
| A2 | Source identity verifiable by checksum/manifest | `python -m src.cli verify-source`; a re-saved copy is refused | `metadata/source_manifest.yml`, failure drill 1 | pass |
| A3 | No machine-specific absolute paths | relative paths in `settings.yml`, overrides only via environment; unit test for the clean-clone rule | `tests/test_config.py` | pass |
| A4 | No secret credentials committed | `.env` is git-ignored, `.env.example` has placeholders only; `git ls-files` shows no `.env` | `.gitignore`, `git log` | pass |
| A5 | Environment can be created from declared files | `docker compose build` / `pip install -r requirements.txt`; `env-check --strict` | `Dockerfile`, `Dockerfile.airflow`, `docker-compose.yml` | pass |
| A6 | Full ingestion gives the expected raw count/checksum | `ingest --mode full` → 100,000 rows in 12 batches; each batch carries the source checksum and a content hash; `validate_raw` compares raw to source | `docs/evidence/incremental_ingestion.md`, run records | pass |
| A7 | Incremental/checkpoint demo avoids duplicates | first run (2 batches), interrupted run (exit 75), resumed run, rerun with nothing to do, 0 duplicate keys | `docs/evidence/incremental_ingestion.md`, Airflow scenario 3 | pass |
| A8 | Staging and curated are deterministic | rebuilding from raw gives the same row hashes; a full refresh followed by a load changes 0 rows | `docs/evidence/airflow_runs.md` (scenarios 2 and 3), determinism test | pass |
| A9 | PostgreSQL load is rerun-safe | second load: 0 inserted / 0 updated / 0 deleted | `test_postgres_load_is_rerun_safe`, Airflow scenario 2 | pass |
| A10 | Storage comparison uses the same logical records | each read is checked for row/column counts, key and target sums and the first timestamp | `docs/benchmark_report.md` (*Same records: yes*) | pass |
| A11 | One partition can be read independently | `partition-demo --year 2022 --month 6`: 1 of 35 files, 2,836 rows = full file = PostgreSQL | `outputs/benchmark/partition_demo.json` | pass |
| A12 | Airflow DAG has valid dependencies and completes | DAG test (chain, settings) and four documented DAG runs | `tests/test_dag.py`, `docs/evidence/airflow_runs.md` | pass |
| A13 | A recoverable failure was demonstrated | interrupted ingestion recovered by retry; missing source fixed and cleared; six CLI drills + recovery | `docs/evidence/airflow_runs.md`, `docs/evidence/failure_drills.md` | pass |
| A14 | Quality checks detect invalid conditions | every check is tested against a deliberately broken dataset; drills 2–4 | `tests/test_quality_checks.py`, `docs/evidence/failure_drills.md` | pass |
| A15 | A second run does not increase duplicate business keys | `audit.v_duplicate_check` = 0 after repeated runs | `docs/evidence/airflow_runs.md` | pass |
| A16 | Metrics are regenerated from curated data | `train_model` reads `curated.fire_risk_features`; `evaluate_model` writes `metrics.json`; the matrix hash links them | run records, `metadata/split_manifest.json`, model card | pass |
| A17 | Reproduced metric passes the tolerance rule | RF RMSE 3.235651 vs 3.24 (≤ 0.005); 0 blocking failures | `outputs/reproducibility_check.json` | pass |
| A18 | Architecture, lineage, runbook and report are present and consistent | reviewed against the code during the final audit | `docs/` | pass |

## 11. Demonstration sequence (lab section 10.1)

| # | Step | What I run / show |
|---|---|---|
| 1 | Repository and commit history | the GitHub page; `git log --oneline` |
| 2 | No secrets or large artifacts committed | `git ls-files \| grep -i env` (only `.env.example`); `git check-ignore -v .env data/source/industrial_fire_risk_data.csv outputs/model/random_forest.joblib` |
| 3 | Source checksum | `docker compose run --rm pipeline python -m src.cli verify-source` |
| 4 | Build and start | `docker compose up -d --build`, then `docker compose ps` (both healthy) |
| 5 | Run the pipeline | Airflow UI → Trigger (or `docker compose run --rm pipeline`) |
| 6 | Raw, staging, curated and outputs | `python -m src.cli status`; the `data/` folders; `outputs/metrics.json`, `outputs/predictions.csv` |
| 7 | PostgreSQL tables and audit records | `psql`: `\dt curated.*`, `SELECT * FROM audit.v_run_overview;`, `SELECT * FROM serving.v_predictions LIMIT 5;` |
| 8 | Data-quality and code tests | `docker compose run --rm pipeline pytest`; `docker compose exec airflow python -m pytest tests/test_dag.py` |
| 9 | Airflow graph and run states | Graph and Grid views (`docs/evidence/airflow_graph.png`, `airflow_grid.png`) |
| 10 | Second run, no duplicates | trigger again; `load_postgres` reports 0/0/0; `SELECT * FROM audit.v_duplicate_check;` |
| 11 | Baseline vs new metrics side by side | `outputs/reproducibility_summary.md`; `SELECT * FROM audit.v_metric_comparison;` |
| 12 | Failure and recovery | trigger with `{"ingest_mode": "full", "fail_after_batches": 2}` and show the retry, or walk through `docs/evidence/airflow_runs.md` |
| 13 | A design decision I would change for production | below |

**What I would change for a real production system.** Every run rebuilds
staging and curated from the whole raw layer and merges the whole table into
PostgreSQL. That is simple and obviously correct for 100,000 rows, and it
made rerun safety easy to prove. In production I would make the downstream
stages incremental too: transform and load only the new month partitions,
with the same keys and the same merge, so each run's cost grows with the new
data instead of with the history. That is also the first scalability limit
named in question 14.

## 12. Final submission checklist (lab section 15)

| Item | Status |
|---|---|
| Repository URL submitted and accessible | to do at submission |
| README starts from a clean-machine perspective | ✔ `README.md` |
| Source data legally included or reproducibly referenced with checksum | ✔ referenced with SHA-256 (`data/source/README.md`) |
| No secrets or personal absolute paths committed | ✔ |
| Data contract and source profile complete | ✔ `metadata/` |
| Raw, staging, curated and output semantics documented | ✔ README, lineage diagram, this report |
| Full and incremental/checkpoint ingestion demonstrated | ✔ `docs/evidence/incremental_ingestion.md` |
| Docker / Docker Compose included | ✔ |
| PostgreSQL target uses rerun-safe loading | ✔ |
| Storage benchmark uses equivalent logical data | ✔ |
| Partition demonstration completed | ✔ |
| Airflow DAG runs successfully | ✔ |
| Quality tests and code tests run successfully | ✔ `docs/evidence/test_results.txt` |
| Failure/recovery evidence included | ✔ |
| Model retrains from curated data with deterministic configuration | ✔ |
| Expected vs actual metrics compared machine-readably | ✔ |
| Second run does not introduce logical duplicates | ✔ |
| Architecture and lineage diagrams match the implementation | ✔ |
| Runbook explains restart and recovery | ✔ `docs/runbook.md` |
| Reproducibility report explains deviations from the group project | ✔ section 5 |

## 13. Attribution and AI use

**Prior work.** The dataset choice, problem statement, feature definitions,
model choices and reported metrics come from our Predictive Analytics group
project (Risma, V. L. S.; Abad, R. A.; Toliba, M. G.), as allowed by lab
section 4.1. The dataset is by the Kaggle user "joganimitesh". Everything
about the data engineering is my individual work for DSS150P: ingestion,
layers, contract, storage, orchestration, tests, observability and
documentation.

**AI use.** I used an AI assistant (Anthropic's Claude) to help design,
write, debug and test the code and to draft the documentation. I reviewed,
ran and checked everything in this repository. Every number in this report
comes from files the pipeline generated, and I can explain the
implementation, the failure behaviour and the design choices. This follows
the DSS150P policy on generative AI (lab section 16).
