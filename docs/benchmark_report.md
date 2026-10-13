# Storage benchmark report

## Why I ran this

The lab asks for the same logical records in CSV, JSON Lines, Parquet and
PostgreSQL so that the storage choices in this project are backed by
measurements instead of habit. Everything here is produced by

```bash
python -m src.cli benchmark          # sizes, write/read times, selective reads
python -m src.cli partition-demo     # one-partition read + row-count check
```

which write `outputs/benchmark/benchmark_results.json`,
`outputs/benchmark/partition_demo.json` and the generated section below.

## Method

* **Dataset:** the curated business view — 98,000 rows × 29 columns (all
  business and derived-feature columns). The pipeline's own audit columns
  (row hashes, run ids, timestamps of processing) are left out because a
  consumer of the data never reads them.
* **Same logical records:** after every read the frame is checked for the same
  row count, column count, sum of `source_row_key`, sum of `risk` and first
  timestamp as the original (the *Same records* column).
* **Fair reads:** every reader returns a *typed* DataFrame, so CSV and JSON
  must also parse `reading_ts` into timestamps, like Parquet and PostgreSQL do.
* **Repeats:** each write is repeated 3 times and each read 5 times; the
  median is reported. The first read is shown separately and excluded from the
  median. It is *not* a true cold-cache read, because the file was written a
  moment earlier and is still in the operating system's page cache.
* **Selective read:** `factory = 'Factory_B' AND reading_ts >= '2023-01-01'`
  (7,373 rows, about 7.5 %), the kind of question an analyst would ask.
  CSV and JSON have no index, so they read everything and filter in pandas;
  Parquet uses predicate push-down; PostgreSQL uses an index on
  `(factory, reading_ts)`.
* **PostgreSQL:** the write time includes `CREATE TABLE`, `COPY`,
  `CREATE INDEX` and `ANALYZE`. The full-table query returns all rows to Python.

## Measurements

<!-- BEGIN GENERATED BENCHMARK (python -m src.cli benchmark) -->
Run `run_20261013T120355Z_45fde3` at 2026-10-13T12:04:22.994Z on Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.41 (Python 3.12.15, 16 CPU, pandas 3.0.5, pyarrow 25.0.1, inside Docker: yes).

Dataset: 98,000 rows x 29 columns. Writes: median of 3. Reads: median of 5 (first read excluded and shown separately). Selective filter: `factory = 'Factory_B' AND reading_ts >= '2023-01-01 00:00:00'`.

| Format | Size (MB) | Write (s) | First read (s) | Read median (s) | Read min–max (s) | Selective read median (s) | Selective rows | Same records |
|---|---|---|---|---|---|---|---|---|
| CSV | 16.13 | 1.2751 | 0.2283 | 0.2009 | 0.1917–0.2171 | 0.1982 | 7,373 | yes |
| JSONL | 52.93 | 0.6404 | 1.216 | 1.0652 | 1.0312–1.1025 | 1.0284 | 7,373 | yes |
| Parquet (snappy) | 5.24 | 0.1796 | 0.0317 | 0.0273 | 0.0248–0.03 | 0.0278 | 7,373 | yes |
| PostgreSQL 16 table | 24.68 | 0.5326 | 0.6247 | 0.5936 | 0.5933–0.6734 | 0.0399 | 7,373 | yes |

PostgreSQL 16.15: table without indexes 20.64 MB (total with index 24.68 MB). Full-table query median 0.5936 s, selective query (index on factory, reading_ts) median 0.0399 s, aggregate query (140 groups) median 0.018 s.

Plan of the selective query:

```text
Bitmap Heap Scan on fire_risk_curated  (cost=223.37..2853.05 rows=7312 width=162)
  Recheck Cond: ((factory = 'Factory_B'::text) AND (reading_ts >= '2023-01-01 00:00:00'::timestamp without time zone))
  ->  Bitmap Index Scan on fire_risk_curated_factory_reading_ts_idx  (cost=0.00..221.54 rows=7312 width=0)
        Index Cond: ((factory = 'Factory_B'::text) AND (reading_ts >= '2023-01-01 00:00:00'::timestamp without time zone))
```

**Partition demo** — `reading_year=2022/reading_month=6`: read 1 of 35 files (partitioned/fire_risk_curated/reading_year=2022/reading_month=6/part-0.parquet), **2,836 rows**; expected from the full curated file: 2,836; in PostgreSQL: 2,836. Counts agree: yes. Median of 5 reads: partition only 0.0053 s vs reading the whole curated file and filtering 0.0761 s.
<!-- END GENERATED BENCHMARK -->

> The numbers above change from machine to machine (and a little from run to
> run). The conclusions below are based on the *ordering* of the formats,
> which was the same in my local run and in the Docker run.

## What the numbers say

1. **Size.** Parquet is about 3× smaller than CSV and 10× smaller than JSON
   Lines. Parquet stores each column with its type and compresses it
   (categories such as `factory` repeat constantly), while JSON Lines repeats
   every column name in every single row. The PostgreSQL table is bigger than
   the CSV because each row carries a header and the index adds about 4 MB.
2. **Full reads.** Reading the whole dataset as typed data is roughly 8× faster
   from Parquet than from CSV and about 40× faster than from JSON Lines. The
   PostgreSQL full-table query is the slowest after JSON: every row travels
   through a client connection and is turned into Python objects one by one.
3. **Selective reads.** This is where the formats really separate. Parquet
   (push-down) and PostgreSQL (index) answer the Factory_B-since-2023 question
   in tens of milliseconds. CSV and JSON cannot skip anything, so the selective
   read costs as much as a full read.
4. **Writes.** Parquet is also the fastest to write. The PostgreSQL write is
   slower, but it includes building the index and the planner statistics.
5. **Partitions.** Reading one month touched 1 of 35 files and was more than
   10× faster than reading the whole curated file and filtering, and the row
   count (2,836) matched the full file and PostgreSQL exactly.

## Which representation for which job — in *this* project

| Job | Choice | Reason |
|---|---|---|
| Source exchange (receiving or sharing the dataset) | **CSV** | It is what Kaggle publishes and any tool can open it. The weakness is that it is untyped text: an Excel re-save silently rewrote the dates (see the reproducibility report). That is why the CSV is only used at the boundary and is pinned by its SHA-256. |
| Pipeline intermediate storage (raw, staging, curated layers) | **Parquet** | Smallest and fastest, the schema travels with the data (no date re-parsing), and a file can be replaced atomically, which makes reruns safe. |
| Model input | **PostgreSQL by default, Parquet as an option** | Training reads the curated table from PostgreSQL so the model provably sees the same rows that are served (validate_curated compares both copies). It costs about 0.7 s per run, which is fine for 98,000 rows. Parquet is the faster choice (`PIPELINE_MODEL_INPUT=parquet`) and is what I would switch to for much larger data. |
| Analytical querying | **PostgreSQL** (and month partitions for batch scans) | Indexes for selective questions, SQL views such as `curated.v_daily_factory_risk`, constraints, and many users at once. Month partitions in Parquet are the cheap option for "give me one month" batch jobs. |
| — | JSON Lines: not used | Largest and slowest for flat tabular data. It makes more sense for nested, event-style data or API exchange. |

**Partition key.** I partitioned by `reading_year` / `reading_month` because
time is how this data is used: it arrives in time order, the model is split by
time, and reports are usually per month. Factory would only give 4 large
partitions and most questions also have a time range. The downside is 35
small files (about 2,800 rows each); with real production volumes I would
keep months but expect much bigger files, and for data this small a coarser
key (year or quarter) would avoid the "many small files" problem.

## Why this result should not be generalised

* It is one machine (2 CPUs here), a warm page cache and only 98,000 narrow
  rows. With 100× more data the PostgreSQL full-table query and the CSV parse
  would grow much faster than a Parquet scan, and the index/partition
  advantages would become even larger.
* PostgreSQL ran on the same machine. Over a real network its full reads would
  be slower still, while selective queries would still benefit from the index.
* The access pattern matters more than the format: many small inserts or
  updates favour PostgreSQL, while rewriting Parquet files for every change
  would be wasteful; nested or semi-structured data would favour JSON.
* Compression codec, row-group size, pandas/pyarrow versions and the disk
  all change the absolute numbers.
