"""Connection details shared by dbt and Python code that read the lake with DuckDB."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlparse

import duckdb

from costlake.config import Settings, StorageBackend
from costlake.normalization.silver import SILVER_PREFIX


def silver_glob(settings: Settings) -> str:
    return f"{settings.lake_uri}/{SILVER_PREFIX}**/*.parquet"


def dbt_environment(settings: Settings) -> dict[str, str]:
    """Environment variables consumed by ``dbt/profiles.yml``."""
    env = {
        "COSTLAKE_WAREHOUSE_PATH": str(settings.warehouse_path),
        "COSTLAKE_SILVER_GLOB": silver_glob(settings),
        "COSTLAKE_DBT_TARGET": "local",
    }
    if settings.storage_backend is StorageBackend.S3:
        # Static keys for S3-compatible stores; AWS credential chain (IAM role) otherwise.
        env["COSTLAKE_DBT_TARGET"] = "s3" if settings.s3_endpoint_url else "aws"
        env["COSTLAKE_S3_REGION"] = settings.s3_region
        if settings.s3_access_key_id and settings.s3_secret_access_key:
            env["COSTLAKE_S3_ACCESS_KEY_ID"] = settings.s3_access_key_id.get_secret_value()
            env["COSTLAKE_S3_SECRET_ACCESS_KEY"] = settings.s3_secret_access_key.get_secret_value()
        if settings.s3_endpoint_url:
            parsed = urlparse(settings.s3_endpoint_url)
            env["COSTLAKE_DUCKDB_S3_ENDPOINT"] = parsed.netloc
            env["COSTLAKE_DUCKDB_S3_USE_SSL"] = "true" if parsed.scheme == "https" else "false"
            env["COSTLAKE_DUCKDB_S3_URL_STYLE"] = "path"
    return env


@contextmanager
def connect_warehouse(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse read-write.

    Always the same mode on purpose: DuckDB refuses a second connection to a file
    with a different configuration in the same process, and dbt (run in-process by
    the CLI) keeps its own read-write connection open.
    """
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.warehouse_path))
    try:
        conn.execute("SET TimeZone = 'UTC'")
        yield conn
    finally:
        conn.close()
