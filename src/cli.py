"""Command line entry point for the pipeline.

Usage examples (run from the repository root):

    python -m src.cli --help
    python -m src.cli env-check
    python -m src.cli verify-source
    python -m src.cli profile
"""

from __future__ import annotations

import argparse
import platform
import re
import sys
from importlib import metadata
from pathlib import Path

from src.config import Settings, load_settings
from src.contract import verify_source
from src.errors import PipelineError
from src.observability import resolve_run_id, setup_logging
from src.profiling import profile_source, write_profile


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
    p.set_defaults(func=cmd_env_check)

    p = sub.add_parser("verify-source", help="check the source CSV against source_manifest.yml")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_verify_source)

    p = sub.add_parser("profile", help="profile the source and write metadata/source_profile.*")
    p.add_argument("--run-id", help="pipeline run id (default: generated)")
    p.set_defaults(func=cmd_profile)
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
