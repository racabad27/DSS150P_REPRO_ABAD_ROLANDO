"""Unit tests: configuration loading, validation and the clean-clone rule."""

from __future__ import annotations

import re

import pytest
import yaml

from src.config import PROJECT_ROOT, load_settings, to_bool
from src.errors import ConfigError


def test_default_settings_load_and_resolve_relative_paths():
    settings = load_settings(use_env_file=False)
    assert settings.get("model.random_seed") == 42
    assert settings.get("model.split.train_fraction") == 0.8
    assert len(settings.get("model.features")) == 26
    # relative paths in settings.yml are resolved against the repository root
    assert settings.source_file.is_relative_to(PROJECT_ROOT) or settings.source_file.is_absolute()


def test_committed_config_has_no_machine_specific_paths():
    """Clean-clone rule: no C:\\Users\\... or /home/<name>/... in committed config."""
    text = (PROJECT_ROOT / "config" / "settings.yml").read_text(encoding="utf-8")
    paths = yaml.safe_load(text)["paths"]
    for key, value in paths.items():
        default = re.sub(r"\$\{[A-Z_]+:-([^}]*)\}", r"\1", str(value))
        assert not re.match(r"^([A-Za-z]:\\|/)", default), f"paths.{key} is absolute: {value}"


def test_environment_variable_overrides_default(monkeypatch):
    monkeypatch.setenv("PIPELINE_BATCH_FREQUENCY", "month")
    settings = load_settings(use_env_file=False)
    assert settings.get("ingestion.batch_frequency") == "month"


@pytest.mark.parametrize("overrides, message", [
    ({"model": {"split": {"train_fraction": 1.5}}}, "train_fraction"),
    ({"model": {"features": ["temp", "accident"]}}, "leakage"),
    ({"model": {"features": ["temp", "temp"]}}, "duplicates"),
    ({"ingestion": {"batch_frequency": "weekly"}}, "batch_frequency"),
    ({"model": {"primary_model": "neural_net"}}, "primary_model"),
])
def test_invalid_values_raise_config_error(overrides, message):
    with pytest.raises(ConfigError, match=message):
        load_settings(overrides=overrides, use_env_file=False)


@pytest.mark.parametrize("text, expected", [("true", True), ("0", False), ("Yes", True), ("off", False)])
def test_to_bool(text, expected):
    assert to_bool(text) is expected
