"""PostgreSQL storage: schema, rerun-safe loads, audit records and fingerprints.

Rerun safety
------------
Staging and curated are loaded with a "merge snapshot" inside one
transaction (see sql/transformations.sql): COPY into a temp table, delete
rows that disappeared, then INSERT ... ON CONFLICT (source_row_key) DO UPDATE
only where the row's values actually changed. If the load is repeated - by a rerun or by an
Airflow retry after a commit the task never heard about - the second attempt
finds identical rows and inserts/updates/deletes nothing. The primary key and
the UNIQUE (reading_ts) constraint make a duplicate physically impossible.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg import sql

from src.config import Settings
from src.errors import DatabaseError
from src.ingest import Checkpoint
from src.observability import StageLogger

_NAME_LINE = re.compile(r"^-- name:\s*(\w+)\s*$", re.MULTILINE)
STAGING_TABLE = "staging.fire_risk_readings"
CURATED_TABLE = "curated.fire_risk_features"
# audit columns that change on every run; they do not count as a data change
RUN_SPECIFIC_COLUMNS = {"pipeline_run_id", "processed_at_utc", "ingested_at_utc"}


# ---------------------------------------------------------------------------
# connection + SQL files
# ---------------------------------------------------------------------------
def connect(settings: Settings, log: StageLogger | None = None, attempts: int = 5,
            wait_s: float = 2.0) -> psycopg.Connection:
    """Open a connection, retrying a few times while the database starts up."""
    params = {k: v for k, v in settings.database().items() if v not in (None, "")}
    target = f"{params.get('user')}@{params.get('host')}:{params.get('port')}/{params.get('dbname')}"
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return psycopg.connect(**params, application_name="dss150p-pipeline")
        except psycopg.OperationalError as exc:
            last_error = exc
            if attempt < attempts:
                if log:
                    log.event("db_retry", f"PostgreSQL not reachable ({attempt}/{attempts}), "
                              f"retrying in {wait_s * attempt:.0f}s", level=logging.WARNING)
                time.sleep(wait_s * attempt)
    raise DatabaseError(f"Cannot connect to PostgreSQL {target} after {attempts} attempts: {last_error}")


def load_queries(path: Path) -> dict[str, str]:
    """Split a .sql file into named blocks (``-- name: xyz`` headers)."""
    parts = _NAME_LINE.split(path.read_text(encoding="utf-8"))
    return {name: body.strip() for name, body in zip(parts[1::2], parts[2::2])}


def queries(settings: Settings) -> dict[str, str]:
    return load_queries(settings.sql_dir / "transformations.sql")


def apply_schema(conn: psycopg.Connection, settings: Settings) -> None:
    """Create schemas/tables (idempotent) and (re)create the views."""
    conn.execute((settings.sql_dir / "schema.sql").read_text(encoding="utf-8"))
    conn.execute(queries(settings)["create_views"])
    conn.commit()


# ---------------------------------------------------------------------------
# loads
# ---------------------------------------------------------------------------
def frame_to_csv(frame: pd.DataFrame) -> str:
    """CSV text for COPY. Floats keep full precision; missing values become empty."""
    return frame.to_csv(index=False, header=False, na_rep="")


def merge_snapshot(conn: psycopg.Connection, frame: pd.DataFrame, table: str,
                   named: dict[str, str]) -> dict[str, Any]:
    """Make ``table`` equal to ``frame`` (keyed on source_row_key) in one transaction."""
    schema_name, table_name = table.split(".")
    target = sql.Identifier(schema_name, table_name)
    load_table = sql.Identifier(f"load_{table_name}")
    columns = list(frame.columns)
    column_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    update_set = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in columns if c != "source_row_key")
    compared = [c for c in columns if c not in RUN_SPECIFIC_COLUMNS]
    target_values = sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c in compared)
    new_values = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(c)) for c in compared)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(sql.SQL(named["create_load_table"]).format(load_table=load_table, table=target))
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(load_table, column_list)
            with cur.copy(copy_sql) as copy:
                copy.write(frame_to_csv(frame))
            cur.execute(sql.SQL(named["delete_stale_rows"]).format(table=target, load_table=load_table))
            deleted = cur.rowcount
            cur.execute(sql.SQL(named["upsert_rows"]).format(
                table=target, load_table=load_table, columns=column_list, update_set=update_set,
                target_values=target_values, new_values=new_values))
            flags = [row[0] for row in cur.fetchall()]
    inserted = int(sum(flags))
    updated = len(flags) - inserted
    return {"table": table, "snapshot_rows": int(len(frame)), "inserted": inserted, "updated": updated,
            "deleted": int(deleted), "unchanged": int(len(frame)) - inserted - updated}


# ---------------------------------------------------------------------------
# audit records
# ---------------------------------------------------------------------------
def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str)


def sync_audit(conn: psycopg.Connection, settings: Settings) -> dict[str, int]:
    """Copy run records, quality reports and the ingestion ledger into audit.*.

    Every run record found in outputs/runs/ is (re)synced, including failed runs,
    so the database shows the full history. All statements are upserts.
    """
    runs = stages = checks = 0
    with conn.transaction(), conn.cursor() as cur:
        for path in sorted(settings.runs_dir.glob("*.json")):
            record = json.loads(path.read_text(encoding="utf-8"))
            counts = record.get("counts", {})
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs AS r (pipeline_run_id, status, trigger, started_at_utc,
                    finished_at_utc, source_rows, raw_rows, staging_rows, quarantined_rows, curated_rows,
                    curated_excluded_rows, train_rows, test_rows, prediction_rows, synced_at_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (pipeline_run_id) DO UPDATE SET status = EXCLUDED.status,
                    trigger = EXCLUDED.trigger, started_at_utc = EXCLUDED.started_at_utc,
                    finished_at_utc = EXCLUDED.finished_at_utc, source_rows = EXCLUDED.source_rows,
                    raw_rows = EXCLUDED.raw_rows, staging_rows = EXCLUDED.staging_rows,
                    quarantined_rows = EXCLUDED.quarantined_rows, curated_rows = EXCLUDED.curated_rows,
                    curated_excluded_rows = EXCLUDED.curated_excluded_rows, train_rows = EXCLUDED.train_rows,
                    test_rows = EXCLUDED.test_rows, prediction_rows = EXCLUDED.prediction_rows,
                    synced_at_utc = now()
                """,
                (record["pipeline_run_id"], record.get("status"), record.get("trigger"),
                 record.get("started_at_utc"), record.get("finished_at_utc"),
                 counts.get("source_rows"), counts.get("raw_rows"), counts.get("staging_rows"),
                 counts.get("quarantined_rows"), counts.get("curated_rows"),
                 counts.get("curated_excluded_rows"), counts.get("train_rows"),
                 counts.get("test_rows"), counts.get("prediction_rows")))
            runs += 1
            for stage, entry in record.get("stages", {}).items():
                cur.execute(
                    """
                    INSERT INTO audit.stage_runs (pipeline_run_id, stage, status, attempts, started_at_utc,
                        finished_at_utc, duration_s, counts, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_run_id, stage) DO UPDATE SET status = EXCLUDED.status,
                        attempts = EXCLUDED.attempts, started_at_utc = EXCLUDED.started_at_utc,
                        finished_at_utc = EXCLUDED.finished_at_utc, duration_s = EXCLUDED.duration_s,
                        counts = EXCLUDED.counts, error = EXCLUDED.error
                    """,
                    (record["pipeline_run_id"], stage, entry.get("status"), entry.get("attempts", 1),
                     entry.get("started_at_utc"), entry.get("finished_at_utc"), entry.get("duration_s"),
                     _json(entry.get("counts")), entry.get("error")))
                stages += 1

        for path in sorted(settings.quality_dir.glob("*.json")):
            if path.name.startswith("latest_"):
                continue
            report = json.loads(path.read_text(encoding="utf-8"))
            rows = [(report["pipeline_run_id"], r["check_id"], r["layer"], r["category"], r["description"],
                     r["status"], r["blocking"], _json(r["observed"]), _json(r["expected"]),
                     r["failing_rows"], report["created_at_utc"]) for r in report["results"]]
            cur.executemany(
                """
                INSERT INTO audit.data_quality_results (pipeline_run_id, check_id, layer, category,
                    description, status, blocking, observed, expected, failing_rows, checked_at_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, check_id) DO UPDATE SET status = EXCLUDED.status,
                    observed = EXCLUDED.observed, expected = EXCLUDED.expected,
                    failing_rows = EXCLUDED.failing_rows, checked_at_utc = EXCLUDED.checked_at_utc
                """, rows)
            checks += len(rows)

        state = Checkpoint(settings.checkpoint_file).load()
        batches = [(batch_id, b["rows"], b["min_reading_ts"], b["max_reading_ts"], b["content_sha256"],
                    b["file"], b["committed_at_utc"], b["pipeline_run_id"])
                   for batch_id, b in state["committed_batches"].items()]
        cur.execute("DELETE FROM audit.ingestion_batches WHERE NOT (batch_id = ANY(%s))",
                    ([b[0] for b in batches],))
        cur.executemany(
            """
            INSERT INTO audit.ingestion_batches (batch_id, rows, min_reading_ts, max_reading_ts,
                content_sha256, file, committed_at_utc, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET rows = EXCLUDED.rows,
                min_reading_ts = EXCLUDED.min_reading_ts, max_reading_ts = EXCLUDED.max_reading_ts,
                content_sha256 = EXCLUDED.content_sha256, file = EXCLUDED.file,
                committed_at_utc = EXCLUDED.committed_at_utc, pipeline_run_id = EXCLUDED.pipeline_run_id
            """, batches)
    return {"runs_synced": runs, "stage_rows": stages, "quality_rows": checks, "batches": len(batches)}


def record_error(settings: Settings, run_id: str | None, stage: str | None,
                 try_number: int | None, error: BaseException) -> bool:
    """Best-effort error record for failure callbacks. Never raises."""
    try:
        with connect(settings, attempts=1) as conn:
            apply_schema(conn, settings)
            conn.execute(
                "INSERT INTO audit.pipeline_errors (pipeline_run_id, stage, try_number, error_type, error_message) "
                "VALUES (%s, %s, %s, %s, %s)",
                (run_id, stage, try_number, type(error).__name__, str(error)[:4000]))
        return True
    except Exception:  # noqa: BLE001 - reporting must not hide the original failure
        return False


# ---------------------------------------------------------------------------
# fingerprints (file vs database reconciliation)
# ---------------------------------------------------------------------------
def frame_fingerprint(frame: pd.DataFrame) -> dict[str, Any]:
    """Same numbers as the SQL `table_fingerprint` query, computed in Python."""
    ordered = frame.sort_values("source_row_key")
    digest = hashlib.sha256("".join(ordered["row_hash"].astype(str)).encode("utf-8")).hexdigest()
    return {"row_count": int(len(frame)), "distinct_reading_ts": int(frame["reading_ts"].nunique()),
            "row_hash_digest": digest}


def table_fingerprint(conn: psycopg.Connection, settings: Settings, table: str) -> dict[str, Any]:
    schema_name, table_name = table.split(".")
    query = sql.SQL(queries(settings)["table_fingerprint"]).format(table=sql.Identifier(schema_name, table_name))
    with conn.cursor() as cur:
        cur.execute(query)
        row_count, distinct, digest = cur.fetchone()
    return {"row_count": int(row_count), "distinct_reading_ts": int(distinct), "row_hash_digest": digest}


def scalar(conn: psycopg.Connection, settings: Settings, query_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(queries(settings)[query_name])
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# the load stage
# ---------------------------------------------------------------------------
def load_postgres(settings: Settings, run_id: str, log: StageLogger) -> dict[str, Any]:
    """Load staging and curated into PostgreSQL and sync the audit tables."""
    from src.transform import CURATED_COLUMNS, curated_path, staging_path  # avoid import cycle

    staging = pd.read_parquet(staging_path(settings))
    curated = pd.read_parquet(curated_path(settings))[CURATED_COLUMNS]
    named = queries(settings)
    with connect(settings, log) as conn:
        apply_schema(conn, settings)
        results = [merge_snapshot(conn, staging, STAGING_TABLE, named),
                   merge_snapshot(conn, curated, CURATED_TABLE, named)]
        for res in results:
            log.event("table_loaded", f"{res['table']}: {res['inserted']:,} inserted, {res['updated']:,} updated, "
                      f"{res['deleted']:,} deleted, {res['unchanged']:,} unchanged", **res)
        audit = sync_audit(conn, settings)
    return {"tables": results, "audit": audit}
