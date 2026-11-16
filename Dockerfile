# syntax=docker/dockerfile:1.7
# One image for every costlake process: Dagster code server, webserver, daemon,
# CLI and metrics exporter. Multi-stage: build tools never reach the runtime image.

ARG PYTHON_IMAGE=python:3.12.16-slim-trixie

FROM ${PYTHON_IMAGE} AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /build
# Dependencies first (hash-pinned lock) so that code changes reuse this layer.
COPY requirements/runtime.lock requirements/runtime.lock
RUN pip install --require-hashes --no-deps -r requirements/runtime.lock
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-deps .

FROM ${PYTHON_IMAGE} AS runtime
ARG UID=10001
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    COSTLAKE_CONFIG_DIR=/app/config \
    COSTLAKE_DBT_PROJECT_DIR=/app/dbt \
    COSTLAKE_DBT_MANIFEST_DIR=/app/dbt/target \
    COSTLAKE_LOG_FORMAT=json \
    DAGSTER_HOME=/opt/dagster/home \
    HOME=/home/costlake
# Debian security updates published after the base image was built.
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid ${UID} costlake \
 && useradd --uid ${UID} --gid ${UID} --create-home --home-dir /home/costlake --shell /usr/sbin/nologin costlake \
 && mkdir -p /app /data /opt/dagster/home \
 && chown -R costlake:costlake /app /data /opt/dagster/home
COPY --from=build /opt/venv /opt/venv
COPY --chown=costlake:costlake config /app/config
COPY --chown=costlake:costlake dbt /app/dbt
COPY --chown=costlake:costlake infrastructure/docker/dagster/dagster.yaml infrastructure/docker/dagster/workspace.yaml /opt/dagster/home/
WORKDIR /app
USER ${UID}:${UID}
# Pre-install the DuckDB httpfs extension and pre-compile the dbt manifest at build
# time: containers start without network access to extension or package servers.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL aws')" \
 && COSTLAKE_SILVER_GLOB=/nonexistent dbt parse --project-dir /app/dbt --profiles-dir /app/dbt --target-path /app/dbt/target --quiet \
 && rm -rf /app/dbt/logs
EXPOSE 3000 4000 9108
HEALTHCHECK NONE
CMD ["costlake", "--help"]
