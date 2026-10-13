"""Exception types used across the pipeline.

Each error carries the exit code the CLI returns, so a shell script, Docker or
Airflow can tell *why* a step failed without reading the log text:

    0  success                         4  data-quality / contract failure
    1  unexpected error                5  database problem
    2  invalid configuration           6  reproducibility check failed
    3  source file missing/changed     75 simulated interruption (failure drill)
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for expected, explainable pipeline failures."""

    exit_code = 1


class ConfigError(PipelineError, ValueError):
    """The configuration file or an environment value is missing or invalid."""

    exit_code = 2


class SourceVerificationError(PipelineError):
    """The source CSV is missing or is not the snapshot recorded in the manifest."""

    exit_code = 3


class DataQualityError(PipelineError):
    """A blocking data-quality check or data-contract rule failed."""

    exit_code = 4


class DatabaseError(PipelineError):
    """PostgreSQL is unreachable or a load/query failed."""

    exit_code = 5


class ReproducibilityError(PipelineError):
    """Reproduced metrics are outside the declared tolerance of the baseline."""

    exit_code = 6


class SimulatedInterruption(PipelineError):
    """Raised on purpose by failure drills to imitate a crash mid-run."""

    exit_code = 75
