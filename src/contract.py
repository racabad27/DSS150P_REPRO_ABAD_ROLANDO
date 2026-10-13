"""Source identity (manifest) and the data contract.

* ``verify_source`` proves the file in data/source/ is the exact snapshot
  recorded in metadata/source_manifest.yml (size + SHA-256). For a static
  dataset this is the "freshness" check: we verify the version instead of
  pretending the file is continuously updated.
* ``load_contract`` turns metadata/data_contract.yml into small Python
  objects that the validation and transformation code use, so the rules are
  written once (in YAML) and enforced in code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from src.config import Settings
from src.errors import ConfigError, SourceVerificationError
from src.profiling import sha256_file


@dataclass(frozen=True)
class ColumnSpec:
    """One column of the source as described in the data contract."""

    name: str
    type: str
    nullable: bool
    staging_name: str
    role: str
    description: str = ""
    allowed_values: tuple | None = None
    min: float | None = None
    max: float | None = None
    format: str | None = None
    max_null_rate: float | None = None


@dataclass(frozen=True)
class Contract:
    """Parsed data contract with helpers used by validate.py and transform.py."""

    raw: dict
    columns: tuple[ColumnSpec, ...]
    rules: dict[str, dict] = field(default_factory=dict)

    @property
    def version(self) -> str:
        return str(self.raw.get("contract_version"))

    @property
    def source_columns(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def expected_rows(self) -> int:
        return int(self.raw["source"]["expected_rows"])

    @property
    def sha256(self) -> str:
        return str(self.raw["source"]["sha256"])

    @property
    def timestamp(self) -> dict:
        return self.raw["timestamp"]

    def column(self, name: str) -> ColumnSpec:
        for spec in self.columns:
            if spec.name == name:
                return spec
        raise KeyError(name)

    def value_columns(self) -> list[ColumnSpec]:
        """All columns except Date/Time (which become reading_ts)."""
        return [c for c in self.columns if c.type not in {"date", "time"}]

    def rule(self, rule_id: str) -> dict:
        if rule_id not in self.rules:
            raise ConfigError(f"data contract has no rule '{rule_id}'")
        return self.rules[rule_id]

    def on_violation(self, rule_id: str) -> str:
        return str(self.rule(rule_id)["on_violation"])


def load_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"Required metadata file is missing: {path}")
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_manifest(settings: Settings) -> dict:
    return load_yaml(settings.file_setting("source.manifest_file"))


def load_contract(settings: Settings) -> Contract:
    raw = load_yaml(settings.file_setting("source.contract_file"))
    columns = []
    for item in raw.get("columns", []):
        allowed = item.get("allowed_values")
        columns.append(ColumnSpec(
            name=item["name"],
            type=item["type"],
            nullable=bool(item.get("nullable", False)),
            staging_name=item.get("staging_name", item["name"].lower()),
            role=item.get("role", "attribute"),
            description=str(item.get("description", "")).strip(),
            allowed_values=tuple(allowed) if allowed is not None else None,
            min=item.get("min"),
            max=item.get("max"),
            format=item.get("format"),
            max_null_rate=item.get("max_null_rate"),
        ))
    if not columns:
        raise ConfigError("data_contract.yml defines no columns")
    rules = {rule["id"]: rule for rule in raw.get("rules", [])}
    return Contract(raw=raw, columns=tuple(columns), rules=rules)


def verify_source(settings: Settings, log=None) -> dict[str, Any]:
    """Check that the source file exists and matches the manifest.

    Raises SourceVerificationError (exit code 3) on any mismatch, unless
    checksum verification was switched off for a failure drill.
    """
    path = settings.source_file
    manifest = load_manifest(settings)
    contract = load_contract(settings)
    expected_sha = manifest["file"]["sha256"]
    expected_size = int(manifest["file"]["size_bytes"])

    if contract.sha256 != expected_sha:
        raise ConfigError("source_manifest.yml and data_contract.yml disagree on the source SHA-256")
    if not path.is_file():
        raise SourceVerificationError(
            f"Source file not found: {path}\n"
            "Download it and place it as described in data/source/README.md."
        )

    actual_size = path.stat().st_size
    actual_sha = sha256_file(path)
    matches = actual_sha == expected_sha and actual_size == expected_size
    result = {
        "source_file": path.name,
        "size_bytes": actual_size,
        "expected_size_bytes": expected_size,
        "sha256": actual_sha,
        "expected_sha256": expected_sha,
        "matches_manifest": matches,
        "verification_enforced": settings.verify_checksum,
    }
    if matches:
        if log:
            log.event("source_verified", "Source matches the manifest (size and SHA-256)",
                      sha256=actual_sha[:16], size_bytes=actual_size)
        return result

    message = (
        f"Source file does not match metadata/source_manifest.yml\n"
        f"  expected size {expected_size:,} bytes, sha256 {expected_sha}\n"
        f"  actual   size {actual_size:,} bytes, sha256 {actual_sha}\n"
        "Use the original Kaggle file (do not open and re-save it in Excel)."
    )
    if settings.verify_checksum:
        raise SourceVerificationError(message)
    if log:
        log.event("source_unverified", "Checksum verification is OFF for this run (failure drill)",
                  level=logging.WARNING, sha256=actual_sha[:16])
    return result
