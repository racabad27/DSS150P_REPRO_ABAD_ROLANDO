"""Load and validate the pipeline configuration.

Why this module exists
----------------------
The original notebooks hard-coded everything: the CSV name, the 80/20 split,
the seeds and the model settings were all typed directly into cells. Here every
value that can change between machines (paths, database credentials) or between
experiments (seed, split, model parameters) is read from ``config/settings.yml``
and, for secrets and machine-specific values, from environment variables or the
local ``.env`` file. Python code never contains a personal absolute path.
"""

from __future__ import annotations

import os
import re
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from src.errors import ConfigError

# The repository root is the parent of the ``src`` package, unless the caller
# says otherwise (the Airflow container mounts the project at /opt/project).
PROJECT_ROOT = Path(
    os.environ.get("PIPELINE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
).resolve()
DEFAULT_SETTINGS_FILE = PROJECT_ROOT / "config" / "settings.yml"

# ${NAME} or ${NAME:-default}
_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")

KNOWN_MODELS = {"linear_regression", "random_forest", "xgboost"}
KNOWN_METRICS = {"mse", "rmse", "mae", "r2"}
REQUIRED_SECTIONS = (
    "project", "paths", "source", "ingestion", "staging", "curated",
    "model", "reproducibility", "database", "benchmark", "logging",
)


# ---------------------------------------------------------------------------
# .env handling and ${VAR:-default} interpolation
# ---------------------------------------------------------------------------
def load_env_file(path: Path) -> dict[str, str]:
    """Read KEY=VALUE pairs from a .env file into os.environ.

    Variables that are already set in the real environment win, so Docker
    Compose or a CI system can always override the file. Returns what was read.
    """
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            raise ConfigError(f"{path.name} line {line_no}: expected KEY=VALUE, got {raw_line!r}")
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]  # remove matching quotes
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()  # allow trailing comments
        values[key] = value
        os.environ.setdefault(key, value)
    return values


def _interpolate(obj: Any) -> Any:
    """Replace ${VAR:-default} placeholders recursively inside the YAML data."""
    if isinstance(obj, dict):
        return {key: _interpolate(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(item) for item in obj]
    if isinstance(obj, str):
        def replace(match: re.Match) -> str:
            name, default = match.group("name"), match.group("default")
            value = os.environ.get(name)
            if value is None or value == "":
                return default if default is not None else ""
            return value

        return _ENV_PATTERN.sub(replace, obj)
    return obj


def _deep_merge(base: dict, extra: dict) -> dict:
    """Return a copy of ``base`` with ``extra`` merged into it (used by tests)."""
    merged = deepcopy(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def to_bool(value: Any) -> bool:
    """Interpret config/env values such as 'true', 'false', '1', 'no'."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    raise ConfigError(f"Expected a true/false value, got {value!r}")


# ---------------------------------------------------------------------------
# Settings object
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    """Resolved configuration with helpers for paths and nested values."""

    raw: dict
    root: Path = PROJECT_ROOT
    warnings: tuple[str, ...] = field(default_factory=tuple)

    # -- generic access ------------------------------------------------------
    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Read a nested value, e.g. ``settings.get("model.split.train_fraction")``."""
        node: Any = self.raw
        for part in dotted_key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def resolve(self, value: str | Path) -> Path:
        """Turn a configured path into an absolute path (relative to the repo root)."""
        path = Path(value).expanduser()
        return path if path.is_absolute() else (self.root / path)

    def path(self, key: str) -> Path:
        """Absolute path for an entry of the ``paths`` section."""
        value = self.get(f"paths.{key}")
        if value in (None, ""):
            raise ConfigError(f"paths.{key} is not configured")
        return self.resolve(value)

    # -- frequently used locations ---------------------------------------------
    @property
    def source_file(self) -> Path:
        return self.path("source_file")

    @property
    def data_dir(self) -> Path:
        return self.path("data_dir")

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def curated_dir(self) -> Path:
        return self.data_dir / "curated"

    @property
    def partitioned_dir(self) -> Path:
        return self.data_dir / "partitioned"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def model_input_dir(self) -> Path:
        return self.data_dir / "model_input"

    @property
    def outputs_dir(self) -> Path:
        return self.path("outputs_dir")

    @property
    def model_dir(self) -> Path:
        return self.outputs_dir / "model"

    @property
    def runs_dir(self) -> Path:
        return self.outputs_dir / "runs"

    @property
    def quality_dir(self) -> Path:
        return self.outputs_dir / "quality"

    @property
    def benchmark_dir(self) -> Path:
        return self.outputs_dir / "benchmark"

    @property
    def state_dir(self) -> Path:
        return self.path("state_dir")

    @property
    def logs_dir(self) -> Path:
        return self.path("logs_dir")

    @property
    def metadata_dir(self) -> Path:
        return self.path("metadata_dir")

    @property
    def sql_dir(self) -> Path:
        return self.path("sql_dir")

    @property
    def docs_dir(self) -> Path:
        return self.path("docs_dir")

    @property
    def checkpoint_file(self) -> Path:
        return self.state_dir / self.get("ingestion.checkpoint_file")

    @property
    def verify_checksum(self) -> bool:
        return to_bool(self.get("source.verify_checksum", True))

    def file_setting(self, dotted_key: str) -> Path:
        """Absolute path for settings such as ``source.contract_file``."""
        value = self.get(dotted_key)
        if not value:
            raise ConfigError(f"{dotted_key} is not configured")
        return self.resolve(value)

    # -- database ----------------------------------------------------------------
    def database(self) -> dict[str, Any]:
        """Connection settings for psycopg (password comes from the environment)."""
        db = self.get("database", {})
        return {
            "host": db.get("host") or "localhost",
            "port": int(db.get("port") or 5432),
            "dbname": db.get("dbname"),
            "user": db.get("user"),
            "password": db.get("password") or None,
            "connect_timeout": int(db.get("connect_timeout_s", 10)),
        }

    def describe(self) -> dict[str, Any]:
        """Human-readable summary used by ``python -m src.cli env-check``."""
        db = self.database()
        return {
            "project_root": str(self.root),
            "source_file": str(self.source_file),
            "data_dir": str(self.data_dir),
            "outputs_dir": str(self.outputs_dir),
            "state_dir": str(self.state_dir),
            "logs_dir": str(self.logs_dir),
            "batch_frequency": self.get("ingestion.batch_frequency"),
            "random_seed": self.get("model.random_seed"),
            "train_fraction": self.get("model.split.train_fraction"),
            "model_input_source": self.get("model.input_source"),
            "database": f"{db['user']}@{db['host']}:{db['port']}/{db['dbname']}",
            "database_password_set": bool(db["password"]),
        }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _validate(raw: dict) -> list[str]:
    """Return a list of problems (empty list = valid). Warnings are prefixed."""
    problems: list[str] = []

    for section in REQUIRED_SECTIONS:
        if not isinstance(raw.get(section), dict):
            problems.append(f"missing section '{section}'")
    if problems:
        return problems

    freq = raw["ingestion"].get("batch_frequency")
    if freq not in {"quarter", "month"}:
        problems.append(f"ingestion.batch_frequency must be 'quarter' or 'month', got {freq!r}")

    rate = raw["staging"].get("max_quarantine_rate")
    if not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
        problems.append(f"staging.max_quarantine_rate must be between 0 and 1, got {rate!r}")

    model = raw["model"]
    target = model.get("target")
    features = model.get("features") or []
    if not isinstance(target, str) or not target:
        problems.append("model.target must be a column name")
    if not features or not all(isinstance(f, str) for f in features):
        problems.append("model.features must be a non-empty list of column names")
    elif len(set(features)) != len(features):
        problems.append("model.features contains duplicates")
    for leak in set(model.get("leakage_columns") or []) | {target}:
        if leak in features:
            problems.append(f"model.features must not contain '{leak}' (target leakage)")

    split = model.get("split") or {}
    if split.get("method") != "chronological":
        problems.append("model.split.method must be 'chronological' (baseline used a time-based split)")
    fraction = split.get("train_fraction")
    if not isinstance(fraction, (int, float)) or not 0 < fraction < 1:
        problems.append(f"model.split.train_fraction must be between 0 and 1, got {fraction!r}")

    seed = model.get("random_seed")
    if not isinstance(seed, int) or seed < 0:
        problems.append(f"model.random_seed must be a non-negative integer, got {seed!r}")

    models = model.get("models") or {}
    unknown = set(models) - KNOWN_MODELS
    if not models or unknown:
        problems.append(f"model.models must use {sorted(KNOWN_MODELS)}, got {sorted(models)}")
    for name, params in models.items():
        params = params or {}
        n_est = params.get("n_estimators")
        if n_est is not None and (not isinstance(n_est, int) or n_est < 1):
            problems.append(f"model.models.{name}.n_estimators must be a positive integer")
    if model.get("primary_model") not in models:
        problems.append("model.primary_model must be one of the configured models")
    if model.get("primary_metric") not in KNOWN_METRICS:
        problems.append(f"model.primary_metric must be one of {sorted(KNOWN_METRICS)}")
    n_splits = (model.get("cross_validation") or {}).get("n_splits")
    if not isinstance(n_splits, int) or n_splits < 2:
        problems.append("model.cross_validation.n_splits must be an integer >= 2")
    if model.get("input_source") not in {"postgres", "parquet"}:
        problems.append("model.input_source must be 'postgres' or 'parquet'")
    encoding = model.get("encoding") or {}
    binary = encoding.get("binary") or {}
    if not isinstance(binary, dict) or not all(isinstance(m, dict) and all(isinstance(k, str) for k in m)
                                               for m in binary.values()):
        problems.append("model.encoding.binary must map column -> {\"category\": number} with quoted categories")
    if not isinstance(encoding.get("one_hot_drop_first"), list):
        problems.append("model.encoding.one_hot_drop_first must be a list of columns")

    try:
        int(raw["database"].get("port"))
    except (TypeError, ValueError):
        problems.append(f"database.port must be a number, got {raw['database'].get('port')!r}")

    repeats = raw["benchmark"].get("read_repeats")
    if not isinstance(repeats, int) or repeats < 1:
        problems.append("benchmark.read_repeats must be a positive integer")
    elif repeats < 5:
        problems.append("WARNING: benchmark.read_repeats below 5 does not meet the lab requirement")

    level = str(raw["logging"].get("level", "")).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        problems.append(f"logging.level must be DEBUG/INFO/WARNING/ERROR, got {level!r}")

    try:
        to_bool(raw["source"].get("verify_checksum", True))
    except ConfigError as exc:
        problems.append(f"source.verify_checksum: {exc}")
    return problems


def load_settings(
    config_file: str | Path | None = None,
    overrides: dict | None = None,
    root: Path | None = None,
    use_env_file: bool = True,
) -> Settings:
    """Read settings.yml, apply environment values and optional overrides, validate.

    ``overrides`` is a nested dict merged on top of the YAML (tests use it to
    point the pipeline at a temporary folder).
    """
    root = (root or PROJECT_ROOT).resolve()
    if use_env_file:
        load_env_file(root / ".env")
    path = Path(config_file) if config_file else root / "config" / "settings.yml"
    if not path.is_file():
        raise ConfigError(f"Configuration file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name} is not valid YAML: {exc}") from exc

    data = _interpolate(data)
    if overrides:
        data = _deep_merge(data, overrides)

    problems = _validate(data)
    errors = [p for p in problems if not p.startswith("WARNING")]
    if errors:
        raise ConfigError("Invalid configuration:\n  - " + "\n  - ".join(errors))
    warnings = tuple(p for p in problems if p.startswith("WARNING"))
    return Settings(raw=data, root=root, warnings=warnings)
