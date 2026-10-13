"""Failure-injection drills with evidence (lab: Data Quality, Testing, Observability).

Every drill runs the REAL CLI against an isolated sandbox (a temporary copy of
the source and separate data/outputs/state/logs folders), so the real layers
and outputs are never touched. For each drill the script records the command,
the exit code and the key log lines, then checks that nothing bad was
published. At the end the real outputs/metrics.json is compared with its hash
from before the drills: bad data never reached the model outputs.

    python scripts/failure_drill.py                 # writes docs/evidence/failure_drills.md
    docker compose run --rm pipeline python scripts/failure_drill.py

Drills
  1. spreadsheet re-save of the source (checksum mismatch)         -> exit 3
  2. required column missing (checksum check disabled for the drill) -> exit 4 at validate_raw
  3. 2% invalid rows (above the 1% quarantine threshold)           -> exit 4, nothing published
  4. a few invalid rows (below the threshold)                       -> quarantined, run continues
  5. invalid configuration value                                    -> exit 2
  6. database unavailable                                           -> exit 5 after retries
  7. recovery: the same commands with the problem fixed             -> exit 0
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "source" / "industrial_fire_risk_data.csv"
OUTPUT = ROOT / "docs" / "evidence" / "failure_drills.md"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "missing"


class Sandbox:
    """Separate data/outputs/state/logs folders + a source file for one drill."""

    def __init__(self, base: Path, name: str, source: Path):
        self.dir = base / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.env = {
            "PIPELINE_SOURCE_FILE": str(source),
            "PIPELINE_DATA_DIR": str(self.dir / "data"),
            "PIPELINE_OUTPUTS_DIR": str(self.dir / "outputs"),
            "PIPELINE_STATE_DIR": str(self.dir / "state"),
            "PIPELINE_LOGS_DIR": str(self.dir / "logs"),
        }

    def run(self, *args: str, **extra_env: str) -> subprocess.CompletedProcess:
        env = {**os.environ, **self.env, **extra_env}
        return subprocess.run([sys.executable, "-m", "src.cli", *args], cwd=ROOT, env=env,
                              capture_output=True, text=True)

    def exists(self, relative: str) -> bool:
        return (self.dir / relative).exists()


def key_lines(text: str, limit: int = 12) -> str:
    keep = [line for line in text.splitlines()
            if any(k in line for k in ("ERROR", "FAIL", "WARNING", "quarantine", "Source", "retry",
                                       "finished", "committed (", "not reachable", "Invalid", "expected"))]
    return "\n".join(keep[-limit:]) or text.strip()[-1500:]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run failure-injection drills and write evidence")
    parser.add_argument("--keep", action="store_true", help="keep the sandbox folders for inspection")
    parser.add_argument("--skip-recovery", action="store_true",
                        help="do not run the final full pipeline in the real workspace")
    args = parser.parse_args()
    if not SOURCE.is_file():
        print("The source CSV is missing - see data/source/README.md")
        return 1

    metrics_before = sha(ROOT / "outputs" / "metrics.json")
    base = ROOT / "outputs" / "drills"
    shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True)
    original = pd.read_csv(SOURCE, dtype=str, keep_default_na=False)
    results: list[dict] = []

    def record(title: str, why: str, sandbox: Sandbox | None, steps: list[tuple[list[str], dict, int]],
               checks: dict[str, Callable[[], bool]]) -> None:
        """Run the steps, THEN evaluate the checks (they are functions, called afterwards)."""
        outputs = []
        ok = True
        for cli_args, env, expected in steps:
            done = sandbox.run(*cli_args, **env) if sandbox else subprocess.run(
                [sys.executable, "-m", "src.cli", *cli_args], cwd=ROOT, capture_output=True, text=True,
                env={**os.environ, **env})
            ok &= done.returncode == expected
            outputs.append((cli_args, env, expected, done.returncode, key_lines(done.stdout + done.stderr)))
        evaluated = {name: bool(check()) for name, check in checks.items()}
        checks_ok = all(evaluated.values())
        results.append({"title": title, "why": why, "outputs": outputs, "checks": evaluated,
                        "passed": ok and checks_ok})
        print(f"[{'ok' if ok and checks_ok else 'UNEXPECTED'}] {title}")

    # 1. spreadsheet re-save -> checksum mismatch -------------------------------------------
    resaved = base / "resaved.csv"
    excel_like = original.copy()
    stamp = pd.to_datetime(excel_like["Date"] + " " + excel_like["Time"])
    excel_like["Date"] = stamp.dt.strftime("%d/%m/%Y")
    excel_like["Time"] = stamp.dt.strftime("%H:%M:%S").str.lstrip("0").str.replace(r"^:", "0:", regex=True)
    excel_like.to_csv(resaved, index=False, lineterminator="\r\n")
    box = Sandbox(base, "1_checksum", resaved)
    record("1. Source re-saved by a spreadsheet (checksum mismatch)",
           "Same records, different bytes: dates became DD/MM/YYYY. The manifest check must refuse it.",
           box, [(["ingest"], {}, 3)], {"no raw data was written": lambda: not box.exists("data/raw/batch_id=2021Q1")})

    # 2. missing required column --------------------------------------------------------------
    no_pressure = base / "missing_column.csv"
    original.drop(columns=["Pressure"]).to_csv(no_pressure, index=False, lineterminator="\r\n")
    box = Sandbox(base, "2_missing_column", no_pressure)
    drill = {"PIPELINE_VERIFY_CHECKSUM": "false"}   # let the broken file reach the contract checks
    record("2. Required column missing (Pressure)",
           "Raw keeps what arrived, but validate_raw must stop the run before staging.",
           box, [(["ingest"], drill, 0), (["validate", "--layer", "raw"], drill, 4)],
           {"staging was not published": lambda: not box.exists("data/staging/fire_risk_staging.parquet"),
            "curated was not published": lambda: not box.exists("data/curated/fire_risk_curated.parquet")})

    # 3. too many invalid rows ------------------------------------------------------------------
    many_bad = base / "many_invalid_rows.csv"
    broken = original.copy()
    broken.loc[broken.index[:2000], "Factory"] = "Factory_Z"
    broken.to_csv(many_bad, index=False, lineterminator="\r\n")
    box = Sandbox(base, "3_many_invalid_rows", many_bad)
    record("3. 2% invalid rows (above the 1% quarantine threshold)",
           "Rows are quarantined with a reason, and the threshold check stops the run.",
           box, [(["ingest"], drill, 0), (["validate", "--layer", "raw"], drill, 0), (["transform"], drill, 4)],
           {"staging was not published": lambda: not box.exists("data/staging/fire_risk_staging.parquet"),
            "quarantine file lists 2,000 rows": lambda: box.exists("data/quarantine/rejected_rows.csv") and
            len(pd.read_csv(box.dir / "data/quarantine/rejected_rows.csv")) == 2000})

    # 4. a few invalid rows -------------------------------------------------------------------
    few_bad = base / "few_invalid_rows.csv"
    broken = original.copy()
    broken.loc[10, "Risk"] = "150.0"
    broken.loc[11, "Date"] = "2021-13-45"
    broken.loc[12, "Workers"] = "7.5"
    broken.loc[13, "Accident"] = "1"
    broken.loc[14, "Temp"] = ""
    broken.to_csv(few_bad, index=False, lineterminator="\r\n")
    box = Sandbox(base, "4_few_invalid_rows", few_bad)
    record("4. Five invalid rows (below the threshold)",
           "The five rows go to quarantine with reason codes and the clean rows continue.",
           box, [(["ingest"], drill, 0), (["validate", "--layer", "raw"], drill, 0), (["transform"], drill, 0),
                 (["build-curated"], drill, 0)],
           {"5 rows quarantined": lambda: box.exists("data/quarantine/rejected_rows.csv") and
            len(pd.read_csv(box.dir / "data/quarantine/rejected_rows.csv")) == 5,
            "curated was published": lambda: box.exists("data/curated/fire_risk_curated.parquet")})

    # 5. invalid configuration ------------------------------------------------------------------
    box = Sandbox(base, "5_bad_config", SOURCE)
    record("5. Invalid configuration value",
           "A wrong setting is caught when the configuration loads, before any data is read.",
           box, [(["ingest"], {"PIPELINE_BATCH_FREQUENCY": "weekly"}, 2)],
           {"no raw data was written": lambda: not box.exists("data/raw")})

    # 6. database unavailable ------------------------------------------------------------------
    box = Sandbox(base, "6_database_down", SOURCE)
    down = {"POSTGRES_HOST": "127.0.0.1", "POSTGRES_PORT": "1"}
    record("6. PostgreSQL unavailable",
           "The load retries with growing waits, then fails with a clear message (exit 5).",
           box, [(["ingest"], {}, 0), (["transform"], {}, 0), (["build-curated"], {}, 0),
                 (["load-postgres"], down, 5)], {})

    metrics_after_drills = sha(ROOT / "outputs" / "metrics.json")

    # 7. recovery ---------------------------------------------------------------------------------
    if not args.skip_recovery:
        record("7. Recovery: problems fixed, full pipeline rerun in the real workspace",
               "With the original source, a valid configuration and the database reachable, the "
               "pipeline succeeds and reproduces the same metrics file byte for byte.",
               None, [(["run-all"], {}, 0)],
               {"metrics.json unchanged by the drills": lambda: metrics_after_drills == metrics_before,
                "metrics.json identical after the rerun": lambda: sha(ROOT / "outputs" / "metrics.json") == metrics_before})

    lines = ["# Evidence: failure injection and recovery", "",
             f"Generated by `python scripts/failure_drill.py` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. "
             "Drills 1-6 run in throw-away sandboxes under `outputs/drills/` (git-ignored); "
             "the real data layers are not touched. All log times are UTC.", "",
             f"`outputs/metrics.json` SHA-256 before the drills: `{metrics_before[:16]}...`, after drills 1-6: "
             f"`{metrics_after_drills[:16]}...` "
             f"({'unchanged' if metrics_before == metrics_after_drills else 'CHANGED'}).", ""]
    for r in results:
        lines += [f"## {r['title']} — {'as expected' if r['passed'] else 'UNEXPECTED'}", "", r["why"], ""]
        for cli_args, env, expected, code, text in r["outputs"]:
            env_text = " ".join(f"{k}={v}" for k, v in env.items())
            lines += [f"`{(env_text + ' ') if env_text else ''}python -m src.cli {' '.join(cli_args)}` "
                      f"→ exit **{code}** (expected {expected})", "", "```text", text, "```", ""]
        for name, ok in r["checks"].items():
            lines.append(f"- {'✔' if ok else '✘'} {name}")
        lines.append("")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(lines), encoding="utf-8")
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0 if all(r["passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
