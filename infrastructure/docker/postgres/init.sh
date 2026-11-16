#!/bin/sh
# Runs once, on an empty data directory. One role per consumer, least privilege:
#   dagster   owns the Dagster instance database
#   costlake  owns the ledger and the serving schema
#   grafana   read-only on the serving schema (dashboards)
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v dagster_pw="$DAGSTER_PG_PASSWORD" \
  -v costlake_pw="$COSTLAKE_PG_PASSWORD" \
  -v grafana_pw="$GRAFANA_PG_PASSWORD" <<'SQL'
CREATE ROLE dagster LOGIN PASSWORD :'dagster_pw';
CREATE ROLE costlake LOGIN PASSWORD :'costlake_pw';
CREATE ROLE grafana LOGIN PASSWORD :'grafana_pw';
CREATE DATABASE dagster OWNER dagster;
CREATE DATABASE costlake OWNER costlake;
REVOKE ALL ON DATABASE dagster FROM PUBLIC;
REVOKE ALL ON DATABASE costlake FROM PUBLIC;
GRANT CONNECT ON DATABASE costlake TO grafana;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname costlake <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO costlake;
CREATE SCHEMA serving AUTHORIZATION costlake;
GRANT USAGE ON SCHEMA serving TO grafana;
ALTER DEFAULT PRIVILEGES FOR ROLE costlake IN SCHEMA serving GRANT SELECT ON TABLES TO grafana;
SQL
