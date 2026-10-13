# Source profile — industrial_fire_risk_data.csv

This is the profile of the single authoritative source used by the pipeline.
The tables between the GENERATED markers are written by
`python -m src.cli profile` (machine-readable copy: `source_profile.json`);
everything outside the markers is my own interpretation and is kept when the
profile is regenerated.

| Item | Value |
|---|---|
| Dataset | Industrial Fire and Accident Risk Predictor (Kaggle, publisher "joganimitesh") |
| Source location | `data/source/industrial_fire_risk_data.csv` (see `data/source/README.md`) |
| Ownership | Data owner: the Kaggle publisher. Steward for this project: Rolando Alfonso Abad |
| Update frequency | Static snapshot, no updates expected (treated as a frozen release) |
| Availability | Public on Kaggle; a verified local copy is required |
| Licence | Not confirmed, so the CSV is not committed (see `source_manifest.yml`) |
| Personal data | None — machine telemetry and aggregate crew attributes only |

## Generated profile

<!-- BEGIN GENERATED PROFILE (python -m src.cli profile) -->
### File facts
| Property | Value |
|---|---|
| File | industrial_fire_risk_data.csv |
| Location | data/source/industrial_fire_risk_data.csv |
| Format | CSV |
| Size (bytes) | 11,564,951 |
| SHA-256 | `5db607c512fad903bcfb6e242e2dcfc6ba8f9de091becf6169e6ae5414f309ba` |
| Encoding | UTF-8 (ASCII only: yes, byte-order mark: no) |
| Line endings | CRLF |
| Delimiter | `,` |
| Quote characters | 0 |
| Rows x columns | 100,000 x 20 |
| Full-row duplicates | 0 |
| Duplicate business keys (Date+Time) | 0 |
| Candidate key | Date + Time |

### Timestamps
| Property | Value |
|---|---|
| Format | `%Y-%m-%d %H:%M:%S` |
| Unparseable | 0 |
| First reading | 2021-01-01 00:00:00 |
| Last reading | 2023-11-08 15:45:00 |
| Strictly increasing and unique | yes |
| Cadence (minutes) | 15 |
| Gaps other than 15 minutes | 0 |
| Days covered | 1,042 |
| Readings per full day | 96 |
| Days with fewer readings | 2023-11-08 (64) |
| Rows per year | 2021: 35,040, 2022: 35,040, 2023: 29,920 |

### Numeric columns
| Column | Logical type | Nulls | Null rate | Distinct | Min | Max | Mean | Std | IQR outliers | z>3 outliers |
|---|---|---|---|---|---|---|---|---|---|---|
| Workers | integer | 2,000 | 0.02 | 45 | 5 | 49 | 27.0412 | 12.97 | 0 | 0 |
| Temp | float | 0 | 0 | 3,284 | 10 | 45.82 | 25.3086 | 7.6824 | 0 | 0 |
| Pressure | float | 0 | 0 | 2,774 | 8.31 | 233.87 | 23.6563 | 14.7643 | 508 | 500 |
| Humidity | float | 0 | 0 | 5,001 | 30 | 80 | 55.0007 | 14.4128 | 0 | 0 |
| Vibration | float | 0 | 0 | 501 | 0 | 5 | 2.499 | 1.4429 | 0 | 0 |
| Speed | float | 0 | 0 | 85,084 | 1,000 | 3,999.99 | 2,503.5676 | 864.4817 | 0 | 0 |
| Age | integer | 0 | 0 | 19 | 1 | 19 | 9.9882 | 5.4743 | 0 | 0 |
| Service_Days | integer | 0 | 0 | 364 | 1 | 364 | 182.7415 | 105.0128 | 0 | 0 |
| Gas | float | 0 | 0 | 1,001 | 0 | 10 | 5.0168 | 2.8918 | 0 | 0 |
| Sparks | integer | 0 | 0 | 5 | 0 | 4 | 1.9938 | 1.4123 | 0 | 0 |
| Risk | float | 0 | 0 | 7,750 | 0 | 100 | 48.5242 | 15.5256 | 24 | 9 |
| Accident | integer | 0 | 0 | 2 | 0 | 1 | 0.0409 | 0.1981 |  |  |

### Categorical and text columns
| Column | Logical type | Nulls | Distinct | Values (count) |
|---|---|---|---|---|
| Factory | category | 0 | 4 | Factory_A (25,097), Factory_B (24,965), Factory_C (24,697), Factory_D (25,241) |
| Region | category | 0 | 3 | Industrial_Zone (33,095), Rural (33,328), Urban (33,577) |
| Shift | category | 0 | 2 | Day (49,862), Night (50,138) |
| Exp | category | 0 | 2 | Junior (49,981), Senior (50,019) |
| Training | category | 0 | 2 | No (50,267), Yes (49,733) |
| Alarm | category | 0 | 2 | Off (49,875), On (50,125) |
| Date | string | 0 | 1,042 | 2021-01-01 … 2023-11-08 |
| Time | string | 0 | 96 | 00:00:00 … 23:45:00 |

### Target: Risk
| Range observed | Mean | Std | Rows at 0 | Rows at 100 |
|---|---|---|---|---|
| 0.0 – 100.0 | 48.5242 | 15.5256 | 1 | 1 |

| Risk bin | Rows |
|---|---|
| 0-10 | 232 |
| 10-20 | 2,797 |
| 20-30 | 9,823 |
| 30-40 | 17,663 |
| 40-50 | 22,858 |
| 50-60 | 21,840 |
| 60-70 | 15,952 |
| 70-80 | 7,401 |
| 80-90 | 1,386 |
| 90-100 | 48 |

### Accident vs Risk (leakage check)
| Risk | Accident = 0 | Accident = 1 |
|---|---|---|
| < 75 | 95,902 | 0 |
| = 75.00 | 5 | 2 |
| > 75 | 0 | 4,091 |

Accident positive rate: 4.0930% (4,093 rows).

### Missing Workers
| Rows | Rate | By year | Accidents among them |
|---|---|---|---|
| 2,000 | 0.02 | 2021: 741, 2022: 655, 2023: 604 | 81 |
<!-- END GENERATED PROFILE -->

## Interpretation

**Grain and keys.** One row is one reading for one 15-minute slot. The
timestamps are unique, strictly increasing and never skip a slot (96 readings
per day; the last day stops at 15:45). `Date + Time` is therefore a natural
business key, which the pipeline turns into `reading_ts`. No single column is
unique and there are no duplicate rows. Each slot carries exactly one factory,
so this is one interleaved stream rather than four parallel ones.

**Identifiers.** There is no ID column. The pipeline adds `source_row_key`
(the 1-based row number in this immutable file) for lineage and a
`record_hash` (SHA-256 of the 20 raw values) for duplicate detection.

**Target.** `Risk` is a continuous score clipped to 0–100 (exactly one row at
each bound), roughly bell-shaped around 48.5. Rows above 90 are rare (48),
which is why Risk shows 24 IQR outliers; they are real and kept, as in the
original project.

**Leakage field: `Accident`.** The table above shows that `Accident` is a
deterministic function of the target: it is 1 for every row with Risk above
75 and 0 for every row below 75 (the 7 rows at exactly 75.00 are mixed
because Risk was rounded to 2 decimals after the label was set). Using
`Accident` as a feature would leak the answer, so it is excluded from the
features and used only as a consistency rule in the data contract
(`accident_matches_risk`). It is also very imbalanced (4.09 % positive).

**Missing values.** Only `Workers` has blanks: 2,000 rows (2.0 %), spread
evenly over the three years, and 81 of them are accident rows. The original
project dropped these rows; the pipeline keeps them in staging (they are valid
under the contract) and excludes them in the curated, model-ready layer.

**Outliers.** `Pressure` has a cluster of very high values (508 IQR / 500
z-score outliers, up to 233.87). The original project kept them because they
plausibly describe hazardous conditions; the contract limit for Pressure is
set to 300 so they pass, while impossible values are still caught.

**Categories.** All six categorical columns are clean: no stray spaces, no
case variants, nearly balanced counts. `Shift` is not consistent with the
hour of day (for example a 00:00 reading can be a "Day" shift), which suggests
the data is synthetic; I therefore did **not** add a shift-vs-hour rule.

**Format risks (schema risks).**

1. The date and time formats are easy to destroy: opening and saving the file
   in a spreadsheet changes `2021-01-01` to `01/01/2021` and `00:00:00` to
   `0:00:00`. This already happened to the copy inside the old project zip.
   Raw ingestion therefore keeps every value as text, and only staging parses
   it with the contract format.
2. `Workers` is stored as `7.0` because of the blanks; a strict integer
   parser must accept a trailing `.0` but reject `7.5`.
3. Line endings are CRLF (the file was produced on Windows); pandas handles
   both, and the checksum pins the exact bytes.
4. The timestamps have no time zone. They are stored as
   `TIMESTAMP WITHOUT TIME ZONE` (plant local time); all audit timestamps
   added by the pipeline are UTC.
