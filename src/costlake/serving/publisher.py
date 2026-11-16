"""Publish gold tables from the DuckDB warehouse to PostgreSQL (``serving`` schema).

DuckDB is an excellent single-writer analytical engine but not a server: Grafana,
the metrics exporter and ad-hoc SQL clients read from PostgreSQL instead. Each
table is loaded into a staging table with ``COPY`` and swapped in within one
transaction, so readers always see either the previous or the new version.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb
import psycopg
from psycopg import sql

from costlake.logs import get_logger

log = get_logger(__name__)

SERVING_SCHEMA = "serving"

PUBLISHED_TABLES: tuple[str, ...] = (
    "fct_daily_cost",
    "agg_daily_service_cost",
    "mart_owner_monthly_cost",
    "mart_tag_coverage_daily",
    "mart_commitment_savings_monthly",
    "mart_month_to_date",
    "cost_anomalies",
)

_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class PublishedTable:
    name: str
    rows: int


def postgres_type(duckdb_type: str) -> str:
    t = duckdb_type.upper()
    if t.startswith("DECIMAL"):
        return t.replace("DECIMAL", "NUMERIC")
    mapping = {
        "VARCHAR": "TEXT",
        "DATE": "DATE",
        "DOUBLE": "DOUBLE PRECISION",
        "FLOAT": "REAL",
        "BIGINT": "BIGINT",
        "INTEGER": "INTEGER",
        "SMALLINT": "SMALLINT",
        "HUGEINT": "NUMERIC(38,0)",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    }
    return mapping.get(t, "TEXT")


def to_psycopg_url(sqlalchemy_url: str) -> str:
    """``postgresql+psycopg://...`` (SQLAlchemy) -> ``postgresql://...`` (libpq)."""
    return re.sub(r"^postgresql\+\w+://", "postgresql://", sqlalchemy_url)


def publish_tables(
    warehouse: duckdb.DuckDBPyConnection,
    serving_url: str,
    tables: tuple[str, ...] = PUBLISHED_TABLES,
    source_schema: str = "gold",
) -> list[PublishedTable]:
    published: list[PublishedTable] = []
    with psycopg.connect(to_psycopg_url(serving_url)) as pg:
        for table in tables:
            if not _IDENT.match(table):
                raise ValueError(f"invalid table name {table!r}")
            columns = warehouse.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_schema = ? and table_name = ? order by ordinal_position",
                [source_schema, table],
            ).fetchall()
            if not columns:
                raise LookupError(f"{source_schema}.{table} does not exist in the warehouse")
            rows = warehouse.execute(f'select * from "{source_schema}"."{table}"').fetchall()
            _swap_table(pg, table, columns, rows)
            published.append(PublishedTable(table, len(rows)))
            log.info("serving.table_published", table=table, rows=len(rows))
    return published


def _swap_table(
    pg: psycopg.Connection[tuple[object, ...]],
    table: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[object, ...]],
) -> None:
    schema = sql.Identifier(SERVING_SCHEMA)
    target = sql.Identifier(table)
    staging = sql.Identifier(f"{table}__staging")
    column_defs = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(postgres_type(dtype)))
        for name, dtype in columns
    )
    names = sql.SQL(", ").join(sql.Identifier(name) for name, _ in columns)
    with pg.transaction(), pg.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema))
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(schema, staging))
        cur.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(schema, staging, column_defs))
        with cur.copy(sql.SQL("COPY {}.{} ({}) FROM STDIN").format(schema, staging, names)) as copy:
            for row in rows:
                copy.write_row(row)
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(schema, target))
        cur.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(schema, staging, target))
        # After the swap: index names are schema-wide, the old table's index is gone now.
        if any(name == "charge_date" for name, _ in columns):
            cur.execute(
                sql.SQL("CREATE INDEX {} ON {}.{} (charge_date)").format(
                    sql.Identifier(f"ix_{table}_charge_date"[:63]), schema, target
                )
            )
