-- =============================================================================
-- SQL used by src/storage.py. Each block starts with "-- name: <query_name>".
-- Placeholders in {braces} are table/column identifiers filled in safely by
-- Python (psycopg.sql); values are never pasted into the SQL text.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Rerun-safe "merge snapshot" load, executed in ONE transaction:
--   1. COPY the new snapshot into a temporary table
--   2. delete target rows that are not in the snapshot (or whose business key changed)
--   3. insert new rows / update changed rows (ON CONFLICT on the primary key)
-- Running it twice gives the same table: the second run inserts 0, updates 0
-- and deletes 0 rows. If Airflow retries after a commit it never saw, the
-- retry finds the same state and again changes nothing.
-- -----------------------------------------------------------------------------

-- name: create_load_table
CREATE TEMP TABLE {load_table} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP;

-- name: delete_stale_rows
DELETE FROM {table} AS t
WHERE NOT EXISTS (
    SELECT 1
    FROM {load_table} AS s
    WHERE s.source_row_key = t.source_row_key
      AND s.reading_ts = t.reading_ts
);

-- name: upsert_rows
INSERT INTO {table} AS t ({columns})
SELECT {columns} FROM {load_table}
ON CONFLICT (source_row_key) DO UPDATE
SET {update_set}
-- only rows whose data really changed are rewritten (audit columns such as
-- pipeline_run_id are ignored in the comparison)
WHERE ({target_values}) IS DISTINCT FROM ({new_values})
RETURNING (xmax = 0) AS inserted;                     -- true = inserted, false = updated

-- -----------------------------------------------------------------------------
-- Consistency checks used by validate_curated
-- -----------------------------------------------------------------------------

-- name: table_fingerprint
-- row count, distinct business keys and one digest over all row hashes
SELECT count(*)                    AS row_count,
       count(DISTINCT reading_ts)  AS distinct_reading_ts,
       encode(sha256(convert_to(coalesce(string_agg(row_hash, '' ORDER BY source_row_key), ''), 'UTF8')), 'hex')
                                   AS row_hash_digest
FROM {table};

-- name: curated_missing_in_staging
-- referential rule: every curated reading must exist in staging
SELECT count(*) AS missing
FROM curated.fire_risk_features AS c
WHERE NOT EXISTS (SELECT 1 FROM staging.fire_risk_readings AS s
                  WHERE s.source_row_key = c.source_row_key);

-- name: curated_feature_mismatches
-- the derived features recomputed independently in SQL must equal the stored ones
SELECT count(*) AS mismatches
FROM curated.fire_risk_features
WHERE hour_of_day  <> EXTRACT(HOUR  FROM reading_ts)
   OR month        <> EXTRACT(MONTH FROM reading_ts)
   OR day_of_week  <> EXTRACT(ISODOW FROM reading_ts) - 1                       -- Monday = 0
   OR is_weekend   <> CASE WHEN EXTRACT(ISODOW FROM reading_ts) >= 6 THEN 1 ELSE 0 END
   OR temp_pressure_interaction   <> temp * pressure
   OR vibration_speed_interaction <> vibration * speed
   OR workers_sparks_interaction  <> workers::double precision * sparks;

-- -----------------------------------------------------------------------------
-- Views for analysts and for checking the runs (recreated on every load)
-- -----------------------------------------------------------------------------

-- name: create_views
CREATE OR REPLACE VIEW curated.v_daily_factory_risk AS
SELECT reading_ts::date                    AS reading_date,
       factory,
       count(*)                            AS readings,
       round(avg(risk)::numeric, 2)        AS avg_risk,
       max(risk)                           AS max_risk,
       sum(accident)                       AS accidents,
       round(avg(temp)::numeric, 2)        AS avg_temp,
       round(avg(pressure)::numeric, 2)    AS avg_pressure
FROM curated.fire_risk_features
GROUP BY reading_ts::date, factory;

CREATE OR REPLACE VIEW audit.v_run_overview AS
SELECT r.pipeline_run_id,
       r.status,
       r.trigger,
       r.started_at_utc,
       r.finished_at_utc,
       round(extract(epoch FROM (r.finished_at_utc - r.started_at_utc))::numeric, 1) AS duration_s,
       r.raw_rows, r.staging_rows, r.quarantined_rows, r.curated_rows,
       r.train_rows, r.test_rows, r.prediction_rows,
       (SELECT count(*) FROM audit.data_quality_results q
         WHERE q.pipeline_run_id = r.pipeline_run_id AND q.status = 'fail') AS failed_checks
FROM audit.pipeline_runs AS r;

CREATE OR REPLACE VIEW audit.v_duplicate_check AS
SELECT 'staging.fire_risk_readings' AS table_name,
       count(*) AS row_count,
       count(*) - count(DISTINCT reading_ts) AS duplicate_business_keys
FROM staging.fire_risk_readings
UNION ALL
SELECT 'curated.fire_risk_features', count(*), count(*) - count(DISTINCT reading_ts)
FROM curated.fire_risk_features;

CREATE OR REPLACE VIEW serving.v_predictions AS
SELECT p.model_name,
       p.reading_ts,
       c.factory,
       c.region,
       p.actual_risk,
       round(p.predicted_risk::numeric, 2) AS predicted_risk,
       round(p.abs_error::numeric, 2)      AS abs_error,
       p.pipeline_run_id
FROM serving.risk_predictions AS p
JOIN curated.fire_risk_features AS c USING (source_row_key);

CREATE OR REPLACE VIEW audit.v_metric_comparison AS
-- baseline (paper Table 2) next to every published run
SELECT m.pipeline_run_id, m.model_name, m.metric_name,
       r.baseline, m.metric_value AS reproduced,
       round(abs(m.metric_value - r.baseline)::numeric, 6) AS abs_diff,
       r.status
FROM audit.model_metrics AS m
JOIN audit.reproducibility_results AS r
  ON r.pipeline_run_id = m.pipeline_run_id
 AND r.check_id = 'paper.table2.' || m.model_name || '.' || m.metric_name
WHERE m.eval_set = 'test';
