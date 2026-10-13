#!/bin/sh
# Creates the role and database that Airflow uses for its own metadata.
# Safe to run on every start: nothing is created twice, and the password is
# kept in sync with .env. Runs in the one-off "airflow-db-init" service.
set -eu

psql -v ON_ERROR_STOP=1 \
     -v db="$AIRFLOW_DB_NAME" -v usr="$AIRFLOW_DB_USER" -v pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'usr', :'pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'usr')
\gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'usr', :'pw')
\gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'db', :'usr')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db')
\gexec
SQL

echo "Airflow metadata database '$AIRFLOW_DB_NAME' is ready"
