"""Command-line interface: ``costlake --help``."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Annotated

import typer

from costlake.config import get_settings, load_pipeline_config
from costlake.generator.synthetic import (
    DEFAULT_PROVIDERS,
    LEGACY_AWS,
    default_anomalies,
    generate_focus,
    write_month_to_date_deliveries,
)
from costlake.ledger import Ledger
from costlake.logs import configure_logging
from costlake.storage import build_object_store

# Locals are never printed in tracebacks: they may hold credentials.
app = typer.Typer(
    no_args_is_help=True,
    help="FinOps lakehouse for FOCUS billing exports.",
    pretty_exceptions_show_locals=False,
)
pipeline_app = typer.Typer(no_args_is_help=True, help="Run the data pipeline without Dagster.")
deliveries_app = typer.Typer(no_args_is_help=True, help="Inspect and operate deliveries.")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(deliveries_app, name="deliveries")

# (source prefix, provider, file format, render as CUR 2.0)
_GENERATED_SOURCES = (
    ("aws/focus", DEFAULT_PROVIDERS[0], "parquet", False),
    ("azure/focus", DEFAULT_PROVIDERS[1], "csv.gz", False),
    ("gcp/focus", DEFAULT_PROVIDERS[2], "csv.gz", False),
    ("aws/cur2", LEGACY_AWS, "csv.gz", True),
)


@app.callback()
def _setup() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)


def _ledger() -> Ledger:
    ledger = Ledger(get_settings().database_url)
    ledger.init_schema()
    return ledger


@app.command("init-db")
def init_db() -> None:
    """Create the ledger tables (idempotent)."""
    _ledger()
    typer.echo("ledger schema ready")


@app.command()
def generate(
    end: Annotated[
        str | None, typer.Option(help="Last day of data, exclusive (YYYY-MM-DD). Default: today.")
    ] = None,
    days: Annotated[int, typer.Option(min=1, max=1100, help="Days of history.")] = 120,
    seed: Annotated[int, typer.Option(help="Random seed.")] = 42,
    scale: Annotated[int, typer.Option(min=1, max=500, help="Resource multiplier.")] = 1,
    anomalies: Annotated[bool, typer.Option(help="Inject the three demo incidents.")] = True,
) -> None:
    """Write synthetic month-to-date exports for every demo source into the landing bucket."""
    settings = get_settings()
    store = build_object_store(settings)
    end_day = date.fromisoformat(end) if end else datetime.now(UTC).date()
    start_day = end_day - timedelta(days=days)
    incidents = default_anomalies(end_day) if anomalies else []
    export_time = datetime.now(UTC)
    for prefix, provider, fmt, as_cur2 in _GENERATED_SOURCES:
        frame = generate_focus(provider, start_day, end_day, seed, incidents, scale=scale)
        keys = write_month_to_date_deliveries(
            store, settings.landing_bucket, prefix, frame, fmt, export_time, as_cur2=as_cur2
        )
        typer.echo(f"{prefix}: {frame.height} charges in {len(keys)} files")
    for incident in incidents:
        typer.echo(
            f"injected anomaly: {incident.day} {incident.provider} / {incident.service} "
            f"x{incident.multiplier}"
        )


@pipeline_app.command("run")
def pipeline_run() -> None:
    """Ingest, normalise, transform, detect anomalies and publish, in order."""
    from costlake.pipeline import Runtime, run_all

    summary = run_all(Runtime.from_settings(get_settings()))
    quarantined = [o for o in summary.outcomes if o.status == "quarantined"]
    typer.echo(
        f"run {summary.run_id}: {len(summary.landed)} new deliveries, "
        f"{len(summary.outcomes) - len(quarantined)} normalised, {len(quarantined)} quarantined, "
        f"{summary.dbt_nodes} dbt nodes, {summary.anomalies} anomalies, "
        f"{len(summary.published)} tables published"
    )
    for outcome in quarantined:
        typer.echo(f"  quarantined {outcome.delivery_id}: {outcome.error}", err=True)


@deliveries_app.command("list")
def deliveries_list(limit: int = 20) -> None:
    """Most recently discovered deliveries."""
    for d in _ledger().list_deliveries(limit):
        line = f"{d.delivery_id}  {d.status:<11} {d.source_id:<16} {d.prefix}"
        typer.echo(line + (f"\n    error: {d.error}" if d.error else ""))


@deliveries_app.command("requeue")
def deliveries_requeue(delivery_id: str) -> None:
    """Send a quarantined delivery back to normalisation."""
    try:
        _ledger().requeue(delivery_id)
    except LookupError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"{delivery_id} requeued")


@app.command()
def status() -> None:
    """Summary of the lake state."""
    ledger = _ledger()
    rows, total = ledger.silver_totals()
    counts = ledger.delivery_status_counts()
    typer.echo(f"deliveries: {counts or 'none'}")
    typer.echo(
        f"silver: {len(ledger.silver_object_keys())} partitions, {rows} rows, billed {total:,.2f}"
    )


@app.command()
def exporter(port: Annotated[int, typer.Option(min=1, max=65535)] = 9108) -> None:
    """Serve Prometheus metrics about the lake (PostgreSQL ledger only)."""
    from costlake.observability.exporter import serve

    settings = get_settings()
    serve(settings.database_url, port)


@app.command("validate-config")
def validate_config() -> None:
    """Load and validate the settings and the pipeline configuration."""
    settings = get_settings()
    config = load_pipeline_config(settings)
    typer.echo(f"env={settings.env} sources={[s.id for s in config.sources]}")
