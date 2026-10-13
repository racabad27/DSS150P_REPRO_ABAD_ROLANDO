# -----------------------------------------------------------------------------
# Pipeline image: Python 3.12 + the exact package versions in requirements.txt.
# Docker Compose mounts the repository at /app, so data/, outputs/, state/ and
# logs/ written inside the container appear in your project folder.
#
#   docker compose build pipeline
#   docker compose run --rm pipeline python -m src.cli run-all
# -----------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp

# XGBoost needs the OpenMP runtime (libgomp1), which the slim image does not include.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first so this layer is cached when only the code changes.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the project (see .dockerignore: no data, secrets or generated outputs).
COPY . .

# The container can run as any user id (compose uses AIRFLOW_UID), so make the
# working folders writable for the root group like the official Airflow image.
RUN mkdir -p data outputs state logs \
 && chgrp -R 0 /app && chmod -R g+rwX /app

CMD ["python", "-m", "src.cli", "run-all"]
