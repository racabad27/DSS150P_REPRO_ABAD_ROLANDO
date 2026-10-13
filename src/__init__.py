"""DSS150P reproducible pipeline for the IIoT industrial fire-risk project.

Each module is one stage (or one shared concern) of the pipeline. The command
line entry point is ``python -m src.cli --help``; the Airflow DAG in ``dags/``
calls the same functions, so there is only one copy of the business logic.
"""

__version__ = "1.0.0"
