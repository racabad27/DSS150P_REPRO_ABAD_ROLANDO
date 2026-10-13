# data/source — the immutable source dataset

This folder holds the **original, unmodified** dataset. The pipeline only ever
*reads* from here; every later layer (`raw`, `staging`, `curated`, ...) is
rebuilt from this file.

| Item | Value |
|---|---|
| Expected file name | `industrial_fire_risk_data.csv` |
| SHA-256 | `5db607c512fad903bcfb6e242e2dcfc6ba8f9de091becf6169e6ae5414f309ba` |
| Size | 11,564,951 bytes |
| Rows / columns | 100,000 data rows + 1 header row, 20 columns |
| Origin | Kaggle — *Industrial Fire and Accident Risk Predictor* (joganimitesh) |
| URL | https://www.kaggle.com/datasets/joganimitesh/industrial-fire-and-accident-risk-predictor |

## Why the CSV is not committed

The redistribution licence of the Kaggle dataset was not confirmed when this
repository was built, so the file is excluded by `.gitignore` (lab section 4.4).
The checksum above is what makes the dataset identity verifiable instead.

## How to get it

1. Download `industrial_fire_risk_data.csv` from the Kaggle page above
   (or use the copy shared by the original Predictive Analytics group).
2. Put it in this folder **without opening and re-saving it in Excel**.
   Excel silently rewrites dates as `DD/MM/YYYY` and drops trailing `.0`,
   which changes the checksum (this actually happened to the copy inside the
   old project zip, see `docs/reproducibility_report.md`).
3. Verify the checksum:

   ```bash
   # macOS / Linux / WSL
   sha256sum data/source/industrial_fire_risk_data.csv
   ```

   ```powershell
   # Windows PowerShell
   Get-FileHash data\source\industrial_fire_risk_data.csv -Algorithm SHA256
   ```

   Both must print `5db607c5...14f309ba`. The pipeline repeats this check
   before every run and stops if the file differs.

If the licence on the Kaggle page allows redistribution you may commit the file
with `git add -f data/source/industrial_fire_risk_data.csv`; record the licence
in `metadata/source_manifest.yml` when you do.
