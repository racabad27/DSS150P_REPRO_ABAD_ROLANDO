-- =============================================================================
-- PostgreSQL target schema (applied by `python -m src.cli load-postgres`).
-- Every statement is idempotent (IF NOT EXISTS), so applying it again on an
-- existing database changes nothing.
--
--   staging.*  typed copy of the staging layer          (one row per reading)
--   curated.*  model-ready dataset with derived features (complete cases)
--   serving.*  model predictions delivered to consumers
--   audit.*    run history, stage timings, quality results, ingestion batches,
--              model metrics and reproducibility results
--
-- CHECK constraints repeat the most important data-contract rules as a second
-- line of defence: if a bad row ever slipped past the Python checks, the
-- database would reject it and roll the whole load back.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS audit;

-- -----------------------------------------------------------------------------
-- staging.fire_risk_readings
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.fire_risk_readings (
    source_row_key    BIGINT            PRIMARY KEY,     -- row number in the immutable source file
    reading_ts        TIMESTAMP         NOT NULL,        -- business key (Date + Time, plant local time)
    factory           TEXT              NOT NULL CHECK (factory IN ('Factory_A','Factory_B','Factory_C','Factory_D')),
    region            TEXT              NOT NULL CHECK (region IN ('Industrial_Zone','Rural','Urban')),
    shift             TEXT              NOT NULL CHECK (shift IN ('Day','Night')),
    workers           INTEGER                    CHECK (workers BETWEEN 1 AND 100),   -- may be NULL
    exp               TEXT              NOT NULL CHECK (exp IN ('Junior','Senior')),
    training          TEXT              NOT NULL CHECK (training IN ('No','Yes')),
    temp              DOUBLE PRECISION  NOT NULL CHECK (temp BETWEEN -20 AND 80),
    pressure          DOUBLE PRECISION  NOT NULL CHECK (pressure BETWEEN 0 AND 300),
    humidity          DOUBLE PRECISION  NOT NULL CHECK (humidity BETWEEN 0 AND 100),
    vibration         DOUBLE PRECISION  NOT NULL CHECK (vibration BETWEEN 0 AND 20),
    speed             DOUBLE PRECISION  NOT NULL CHECK (speed BETWEEN 0 AND 10000),
    age               INTEGER           NOT NULL CHECK (age BETWEEN 0 AND 60),
    service_days      INTEGER           NOT NULL CHECK (service_days BETWEEN 0 AND 3650),
    gas               DOUBLE PRECISION  NOT NULL CHECK (gas BETWEEN 0 AND 100),
    sparks            INTEGER           NOT NULL CHECK (sparks BETWEEN 0 AND 50),
    alarm             TEXT              NOT NULL CHECK (alarm IN ('Off','On')),
    risk              DOUBLE PRECISION  NOT NULL CHECK (risk BETWEEN 0 AND 100),
    accident          SMALLINT          NOT NULL CHECK (accident IN (0, 1)),
    record_hash       CHAR(64)          NOT NULL,        -- SHA-256 of the raw values (lineage)
    row_hash          CHAR(64)          NOT NULL,        -- SHA-256 of this row's values (change detection)
    batch_id          TEXT              NOT NULL,
    ingested_at_utc   TIMESTAMPTZ       NOT NULL,
    pipeline_run_id   TEXT              NOT NULL,        -- run that last inserted/changed the row
    processed_at_utc  TIMESTAMPTZ       NOT NULL,
    CONSTRAINT uq_staging_reading_ts UNIQUE (reading_ts)
);

-- -----------------------------------------------------------------------------
-- curated.fire_risk_features
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.fire_risk_features (
    source_row_key               BIGINT            PRIMARY KEY,
    reading_ts                   TIMESTAMP         NOT NULL,
    reading_year                 SMALLINT          NOT NULL,
    reading_month                SMALLINT          NOT NULL CHECK (reading_month BETWEEN 1 AND 12),
    factory                      TEXT              NOT NULL CHECK (factory IN ('Factory_A','Factory_B','Factory_C','Factory_D')),
    region                       TEXT              NOT NULL CHECK (region IN ('Industrial_Zone','Rural','Urban')),
    shift                        TEXT              NOT NULL CHECK (shift IN ('Day','Night')),
    workers                      INTEGER           NOT NULL CHECK (workers BETWEEN 1 AND 100),
    exp                          TEXT              NOT NULL CHECK (exp IN ('Junior','Senior')),
    training                     TEXT              NOT NULL CHECK (training IN ('No','Yes')),
    temp                         DOUBLE PRECISION  NOT NULL,
    pressure                     DOUBLE PRECISION  NOT NULL,
    humidity                     DOUBLE PRECISION  NOT NULL,
    vibration                    DOUBLE PRECISION  NOT NULL,
    speed                        DOUBLE PRECISION  NOT NULL,
    age                          INTEGER           NOT NULL,
    service_days                 INTEGER           NOT NULL,
    gas                          DOUBLE PRECISION  NOT NULL,
    sparks                       INTEGER           NOT NULL,
    alarm                        TEXT              NOT NULL CHECK (alarm IN ('Off','On')),
    risk                         DOUBLE PRECISION  NOT NULL CHECK (risk BETWEEN 0 AND 100),
    accident                     SMALLINT          NOT NULL CHECK (accident IN (0, 1)),
    hour_of_day                  SMALLINT          NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week                  SMALLINT          NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),   -- Monday = 0
    is_weekend                   SMALLINT          NOT NULL CHECK (is_weekend IN (0, 1)),
    month                        SMALLINT          NOT NULL CHECK (month BETWEEN 1 AND 12),
    workers_sparks_interaction   DOUBLE PRECISION  NOT NULL,
    temp_pressure_interaction    DOUBLE PRECISION  NOT NULL,
    vibration_speed_interaction  DOUBLE PRECISION  NOT NULL,
    record_hash                  CHAR(64)          NOT NULL,
    row_hash                     CHAR(64)          NOT NULL,
    batch_id                     TEXT              NOT NULL,
    pipeline_run_id              TEXT              NOT NULL,
    processed_at_utc             TIMESTAMPTZ       NOT NULL,
    CONSTRAINT uq_curated_reading_ts UNIQUE (reading_ts)
);

-- selective analytical queries filter on factory + time, or on a month
CREATE INDEX IF NOT EXISTS ix_curated_factory_ts ON curated.fire_risk_features (factory, reading_ts);
CREATE INDEX IF NOT EXISTS ix_curated_year_month ON curated.fire_risk_features (reading_year, reading_month);

-- -----------------------------------------------------------------------------
-- audit tables (observability)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    pipeline_run_id        TEXT          PRIMARY KEY,
    status                 TEXT          NOT NULL,      -- running | success | partial | failed
    trigger                TEXT,                        -- cli | docker-compose | airflow
    started_at_utc         TIMESTAMPTZ,
    finished_at_utc        TIMESTAMPTZ,
    source_rows            INTEGER,
    raw_rows               INTEGER,
    staging_rows           INTEGER,
    quarantined_rows       INTEGER,
    curated_rows           INTEGER,
    curated_excluded_rows  INTEGER,
    train_rows             INTEGER,
    test_rows              INTEGER,
    prediction_rows        INTEGER,
    synced_at_utc          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.stage_runs (
    pipeline_run_id   TEXT              NOT NULL,
    stage             TEXT              NOT NULL,
    status            TEXT              NOT NULL,
    attempts          INTEGER           NOT NULL DEFAULT 1,
    started_at_utc    TIMESTAMPTZ,
    finished_at_utc   TIMESTAMPTZ,
    duration_s        DOUBLE PRECISION,
    counts            JSONB,
    error             TEXT,
    PRIMARY KEY (pipeline_run_id, stage)
);

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    pipeline_run_id   TEXT          NOT NULL,
    check_id          TEXT          NOT NULL,
    layer             TEXT          NOT NULL,
    category          TEXT          NOT NULL,
    description       TEXT,
    status            TEXT          NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    blocking          BOOLEAN       NOT NULL,
    observed          JSONB,
    expected          JSONB,
    failing_rows      INTEGER,
    checked_at_utc    TIMESTAMPTZ,
    PRIMARY KEY (pipeline_run_id, check_id)
);

CREATE TABLE IF NOT EXISTS audit.ingestion_batches (
    batch_id          TEXT          PRIMARY KEY,
    rows              INTEGER       NOT NULL,
    min_reading_ts    TIMESTAMP,
    max_reading_ts    TIMESTAMP,
    content_sha256    CHAR(64)      NOT NULL,
    file              TEXT          NOT NULL,
    committed_at_utc  TIMESTAMPTZ   NOT NULL,
    pipeline_run_id   TEXT          NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.pipeline_errors (
    error_id          BIGSERIAL     PRIMARY KEY,
    pipeline_run_id   TEXT,
    stage             TEXT,
    try_number        INTEGER,
    error_type        TEXT,
    error_message     TEXT,
    occurred_at_utc   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- serving + model audit (written by the publish_outputs stage)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS serving.risk_predictions (
    model_name        TEXT              NOT NULL,       -- linear_regression | random_forest | xgboost
    source_row_key    BIGINT            NOT NULL,
    reading_ts        TIMESTAMP         NOT NULL,
    actual_risk       DOUBLE PRECISION  NOT NULL,
    predicted_risk    DOUBLE PRECISION  NOT NULL,
    abs_error         DOUBLE PRECISION  NOT NULL,
    pipeline_run_id   TEXT              NOT NULL,
    published_at_utc  TIMESTAMPTZ       NOT NULL,
    PRIMARY KEY (model_name, source_row_key)          -- rerun-safe: one prediction per model and reading
);

CREATE TABLE IF NOT EXISTS audit.model_metrics (
    pipeline_run_id   TEXT              NOT NULL,
    model_name        TEXT              NOT NULL,
    eval_set          TEXT              NOT NULL,       -- test | cv_average
    metric_name       TEXT              NOT NULL,       -- mse | rmse | mae | r2
    metric_value      DOUBLE PRECISION  NOT NULL,
    PRIMARY KEY (pipeline_run_id, model_name, eval_set, metric_name)
);

CREATE TABLE IF NOT EXISTS audit.reproducibility_results (
    pipeline_run_id   TEXT              NOT NULL,
    check_id          TEXT              NOT NULL,
    source            TEXT,
    model_name        TEXT,
    kind              TEXT,
    metric_key        TEXT,
    baseline          DOUBLE PRECISION,
    reproduced        DOUBLE PRECISION,
    abs_diff          DOUBLE PRECISION,
    tolerance         DOUBLE PRECISION,
    status            TEXT              NOT NULL CHECK (status IN ('pass', 'fail', 'accepted_deviation')),
    blocking          BOOLEAN           NOT NULL,
    PRIMARY KEY (pipeline_run_id, check_id)
);
