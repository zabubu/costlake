"""Operational metadata of the lake ("ledger").

The ledger is the system of record for *what* was ingested and *which* delivery
currently backs each silver partition. It makes ingestion idempotent and keeps an
audit trail of quality results. SQLAlchemy Core keeps it portable between
PostgreSQL (containers) and SQLite (tests, no-Docker mode).
"""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

metadata = MetaData()


class DeliveryStatus(StrEnum):
    BRONZE = "bronze"  # raw files copied to the lake, waiting for normalisation
    PUBLISHED = "published"  # owns at least one silver partition
    SUPERSEDED = "superseded"  # every partition it fed was replaced by a newer delivery
    QUARANTINED = "quarantined"  # rejected by the quality gate or unreadable


deliveries = Table(
    "deliveries",
    metadata,
    Column("delivery_id", String(32), primary_key=True),
    Column("source_id", String(64), nullable=False),
    Column("prefix", Text, nullable=False),
    Column("status", String(16), nullable=False),
    Column("file_count", Integer, nullable=False),
    Column("total_bytes", Integer, nullable=False),
    Column("exported_at", DateTime(timezone=True), nullable=False),
    Column("discovered_at", DateTime(timezone=True), nullable=False),
    Column("processed_at", DateTime(timezone=True)),
    Column("row_count", Integer),
    Column("billed_cost_total", Float),
    Column("billing_periods", Text),
    Column("error", Text),
    Index("ix_deliveries_source_status", "source_id", "status"),
)

delivery_files = Table(
    "delivery_files",
    metadata,
    Column("delivery_id", ForeignKey("deliveries.delivery_id"), primary_key=True),
    Column("landing_key", Text, primary_key=True),
    Column("etag", String(64), nullable=False),
    Column("size", Integer, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("bronze_key", Text, nullable=False),
)

silver_partitions = Table(
    "silver_partitions",
    metadata,
    Column("source_id", String(64), primary_key=True),
    Column("billing_period", String(7), primary_key=True),
    Column("delivery_id", ForeignKey("deliveries.delivery_id"), nullable=False),
    Column("object_key", Text, nullable=False),
    Column("row_count", Integer, nullable=False),
    Column("billed_cost_total", Float, nullable=False),
    Column("written_at", DateTime(timezone=True), nullable=False),
)

dq_results = Table(
    "dq_results",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("delivery_id", ForeignKey("deliveries.delivery_id"), nullable=False),
    Column("check_name", String(64), nullable=False),
    Column("severity", String(16), nullable=False),
    Column("passed", Boolean, nullable=False),
    Column("observed", Float),
    Column("message", Text, nullable=False),
    Column("checked_at", DateTime(timezone=True), nullable=False),
    Index("ix_dq_results_delivery", "delivery_id"),
)

step_runs = Table(
    "step_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("step", String(32), nullable=False),
    Column("status", String(16), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("rows", Integer),
    Column("error", Text),
    Index("ix_step_runs_step_started", "step", "started_at"),
)
