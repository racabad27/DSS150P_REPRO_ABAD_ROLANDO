"""Command line entry point for the pipeline.

Usage examples (run from the repository root):

    python -m src.cli --help
    python -m src.cli env-check
    python -m src.cli verify-source
    python -m src.cli profile
    python -m src.cli ingest --mode incremental --max-batches 3
    python -m src.cli status
    python -m src.cli transform
    python -m src.cli build-curated
    python -m src.cli load-postgres
    python -m src.cli run-all            (add --skip-db when no PostgreSQL is running)
"""

from __future__ import annotations

import argparse
import platform
import re
import shutil
import sys
from importlib import metadata
from pathlib import Path

from src.config import Settings, load_settings
from src.contract import verify_source
from src.errors import PipelineError
from src.ingest import Checkpoint, clear_raw_layer, raw_status
from src.observability import RunRecord, resolve_run_id, setup_logging
from src.pipeline import PIPELINE_ORDER, run_all, run_stage
from src.profiling import profile_source, write_profile
from src.storage import connect


# ---------------------------------------------------------------------------
# env-check
# ---------------------------------------------------------------------------
def pinned_requirements(path: Path) -> dict[str, str]:
    """Return {package: version} for every ``name==version`` line."""
    pins: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        match = re.match(r"^([A-Za-z0-9_.-]+)(\[[^\]]*\])?==([^\s;]+)", line)
        if match:
            pins[match.group(1)] = match.group(3)
    return pins


def cmd_env_check(args: argparse.Namespace, settings: Settings) -> int:
    """Show interpreter, installed vs pinned package versions and resolved config."""
    print(f"Python      : {platform.python_version()} ({platform.system()} {platform.machine()})")
    print(f"Project root: {settings.root}")
    mismatches = 0
    print("\nPackage versions (installed vs requirements.txt):")
    for name, wanted in pinned_requirements(settings.root / "requirements.txt").items():
        try:
            installed = metadata.version(name)
        except metadata.PackageNotFoundError:
            installed = "NOT INSTALLED"
        ok = installed == wanted
        mismatches += not ok
        print(f"  {'OK ' if ok else '!! '} {name:<14} installed={installed:<14} pinned={wanted}")
    print("\nResolved configuration:")
    for key, value in settings.describe().items():
        print(f"  {key:<24}: {value}")
    for warning in settings.warnings:
        print(f"  {warning}")
    source_present = settings.source_file.is_file()
    print(f"\nSource file present: {'yes' if source_present else 'NO - see data/source/README.md'}")
    if args.check_db:
        try:
            with connect(settings, attempts=1) as conn:
                version = conn.execute("SHOW server_version").fetchone()[0]
            print(f"PostgreSQL reachable: yes (server {version})")
        except PipelineError as exc:
            print(f"PostgreSQL reachable: NO - {str(exc).splitlines()[0]}")
    if mismatches:
        print(f"\n{mismatches} package(s) differ from requirements.txt. Results may not "
              "match the baseline; install with: pip install -r requirements.txt")
        return 1 if args.strict else 0
    print("\nEnvironment matches requirements.txt.")
    return 0


# ---------------------------------------------------------------------------
# source identity and profile
# ---------------------------------------------------------------------------
def cmd_verify_source(args: argparse.Namespace, settings: Settings) -> int:
    log = setup_logging(settings, resolve_run_id(args.run_id), "verify_source")
    result = verify_source(settings, log)
    print(f"File     : {settings.source_file}")
    print(f"Size     : {result['size_bytes']:,} bytes (expected {result['expected_size_bytes']:,})")
    print(f"SHA-256  : {result['sha256']}")
    print(f"Expected : {result['expected_sha256']}")
    print("Result   : " + ("MATCHES the source manifest" if result["matches_manifest"]
                           else "DOES NOT MATCH (verification disabled for this run)"))
    return 0


def cmd_profile(args: argparse.Namespace, settings: Settings) -> int:
    log = setup_logging(settings, resolve_run_id(args.run_id), "profile")
    verify_source(settings, log)
    profile = profile_source(settings)
    json_path, md_path = write_profile(settings, profile)
    log.event("profile_written", "Source profile written",
              rows=profile["shape"]["rows"], columns=profile["shape"]["columns"])
    print(f"Rows x columns : {profile['shape']['rows']:,} x {profile['shape']['columns']}")
    print(f"Time range     : {profile['timestamps']['first']} -> {profile['timestamps']['last']}")
    print(f"Duplicate keys : {profile['duplicates']['business_key_duplicates']}")
    print(f"Workers blank  : {profile['workers_missing']['rows']:,}")
    print(f"Wrote          : {json_path.relative_to(settings.root)}, {md_path.relative_to(settings.root)}")
    return 0


# ---------------------------------------------------------------------------
# ingestion
# ---------------------------------------------------------------------------
def cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    run_id = resolve_run_id(args.run_id)
    try:
        summary = run_stage("ingest_source", settings, run_id, mode=args.mode,
                            max_batches=args.max_batches, fail_after_batches=args.fail_after_batches)
    except BaseException:
        RunRecord(settings, run_id).set_status("failed")
        raise
    RunRecord(settings, run_id).set_status("success" if summary["coverage"] >= 1 else "partial")
    print(f"\nRun id               : {run_id}")
    print(f"Batches written now  : {summary['batches_written_this_run']} "
          f"({summary['rows_written_this_run']:,} rows)")
    print(f"Batches committed    : {summary['batches_committed_total']} of {summary['batches_in_source']}")
    print(f"Raw rows / source    : {summary['raw_rows_total']:,} / {summary['source_rows']:,} "
          f"(coverage {summary['coverage']:.1%})")
    print(f"Watermark            : {summary['watermark_reading_ts']}")
    return 0


def cmd_reset(args: argparse.Namespace, settings: Settings) -> int:
    """Delete generated raw data and the checkpoint (never touches data/source)."""
    if not args.yes:
        print("This deletes data/raw/batch_id=*, data/raw/_runs and the ingestion checkpoint.\n"
              "Run again with --yes to confirm.")
        return 1
    removed = clear_raw_layer(settings)
    runs_dir = settings.raw_dir / "_runs"
    if runs_dir.is_dir():
        shutil.rmtree(runs_dir)
    settings.checkpoint_file.unlink(missing_ok=True)
    print(f"Removed {removed} raw batch folder(s) and the checkpoint. data/source/ was not touched.")
    return 0


def cmd_status(args: argparse.Namespace, settings: Settings) -> int:
    state = Checkpoint(settings.checkpoint_file).load()
    status = raw_status(settings)
    print(f"Checkpoint file      : {settings.checkpoint_file}")
    print(f"Batch frequency      : {state['batch_frequency']}")
    print(f"Watermark            : {status['watermark_reading_ts']}")
    print(f"Last committed batch : {status['last_committed_batch_id']}")
    print(f"Committed batches    : {status['committed_batches']}")
    print(f"Raw rows             : {status['raw_rows']:,}")
    print(f"Distinct row keys    : {status['distinct_source_row_keys']:,}")
    print(f"Duplicate row keys   : {status['duplicate_source_row_keys']}")
    print(f"Duplicate row hashes : {status['duplicate_record_hashes']}")
    for batch_id, rows in status["rows_per_batch"].items():
        print(f"  {batch_id:<20} {rows:>7,} rows")
    return 0


# ---------------------------------------------------------------------------
# single stages and the whole pipeline
# ---------------------------------------------------------------------------
def _run_single(stage_name: str, **params):
    def command(args: argparse.Namespace, settings: Settings) -> int:
        run_id = resolve_run_id(args.run_id)
        try:
            summary = run_stage(stage_name, settings, run_id, **params)
        except BaseException:
            RunRecord(settings, run_id).set_status("failed")
            raise
        stage_status = RunRecord(settings, run_id).load()["stages"][stage_name]["status"]
        RunRecord(settings, run_id).set_status("partial" if stage_status == "partial" else "success")
        print(f"\n{stage_name} finished for run {run_id}")
        for key, value in summary.items():
            if not isinstance(value, (dict, list)):
                print(f"  {key:<26}: {value}")
        return 0
    return command


def cmd_validate(args: argparse.Namespace, settings: Settings) -> int:
    stage_name = {"raw": "validate_raw", "curated": "validate_curated"}[args.layer]
    params = {"check_database": False} if args.layer == "curated" and args.skip_db else {}
    return _run_single(stage_name, **params)(args, settings)


def cmd_run_all(args: argparse.Namespace, settings: Settings) -> int:
    run_id = resolve_run_id(args.run_id)
    record = run_all(settings, run_id, mode=args.mode, max_batches=args.max_batches,
                     fail_after_batches=args.fail_after_batches, skip_db=args.skip_db)
    print(f"\nRun {run_id} finished with status: {record['status'].upper()}")
    for key, value in record["counts"].items():
        print(f"  {key:<24}: {value:,}" if isinstance(value, int) else f"  {key:<24}: {value}")
    print(f"Run record: {settings.runs_dir / (run_id + '.json')}")
    return 0


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.cli",
        description="DSS150P reproducible fire-risk pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    p = sub.add_parser("env-check", help="check Python/package versions and the resolved config")
    p.add_argument("--strict", action="store_true", help="exit 1 if any package differs from the pins")
    p.add_argument("--check-db", action="store_true", help="also try to connect to PostgreSQL")
    p.set_defaults(func=cmd_env_check)

    p = sub.add_parser("verify-source", help="check the source CSV against source_manifest.yml")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_verify_source)

    p = sub.add_parser("profile", help="profile the source and write metadata/source_profile.*")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_profile)

    p = sub.add_parser("ingest", help="copy the source into the raw layer (full or incremental)")
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                   help="incremental = only batches not committed yet (default); full = full refresh")
    p.add_argument("--max-batches", type=int, help="ingest at most N new batches in this run")
    p.add_argument("--fail-after-batches", type=int,
                   help="failure drill: stop on purpose after N committed batches")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("status", help="show the ingestion checkpoint and raw-layer counts")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("reset-raw", help="delete the raw layer and the checkpoint (keeps data/source)")
    p.add_argument("--yes", action="store_true", help="confirm the deletion")
    p.set_defaults(func=cmd_reset)

    p = sub.add_parser("validate", help="run the data-quality checks of one layer")
    p.add_argument("--layer", choices=["raw", "curated"], required=True)
    p.add_argument("--skip-db", action="store_true", help="curated only: do not compare with PostgreSQL")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_validate)

    for name, stage_name, text in [
        ("transform", "transform_staging", "build the staging layer from raw (+ quarantine)"),
        ("build-curated", "build_curated", "build the curated layer and its partitions from staging"),
        ("load-postgres", "load_postgres", "load staging + curated into PostgreSQL (rerun-safe upsert)"),
    ]:
        p = sub.add_parser(name, help=text)
        p.add_argument("--run-id", help="pipeline run id (default: generated)")
        p.set_defaults(func=_run_single(stage_name))

    p = sub.add_parser("run-all", help="run every stage in order: " + " -> ".join(PIPELINE_ORDER))
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    p.add_argument("--max-batches", type=int, help="ingest at most N new batches in this run")
    p.add_argument("--fail-after-batches", type=int, help="failure drill for the ingestion stage")
    p.add_argument("--skip-db", action="store_true",
                   help="run without PostgreSQL (skips load_postgres and the database checks)")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_run_all)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings()
        return int(args.func(args, settings) or 0)
    except PipelineError as exc:
        print(f"\nERROR [{type(exc).__name__}] {exc}", file=sys.stderr)
        return exc.exit_code
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
