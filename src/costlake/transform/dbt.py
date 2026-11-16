"""Run the dbt project (gold layer) in-process with dbt's programmatic API."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

from costlake.config import Settings
from costlake.logs import get_logger
from costlake.transform.warehouse import dbt_environment

log = get_logger(__name__)


class DbtRunError(RuntimeError):
    pass


@dataclass(frozen=True)
class DbtNodeResult:
    unique_id: str
    status: str
    message: str | None


@contextmanager
def _patched_environ(values: dict[str, str]) -> Iterator[None]:
    previous = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run_dbt(settings: Settings, args: Sequence[str] = ("build",)) -> list[DbtNodeResult]:
    from dbt.cli.main import dbtRunner  # heavy import, deferred

    project_dir = str(settings.dbt_project_dir)
    cli_args = [
        *args,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        project_dir,
        "--target-path",
        str(settings.warehouse_path.parent / "dbt-target"),
        "--log-path",
        str(settings.warehouse_path.parent / "dbt-logs"),
    ]
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    with _patched_environ(dbt_environment(settings)):
        outcome = dbtRunner().invoke(cli_args)

    nodes: list[DbtNodeResult] = []
    results = getattr(outcome.result, "results", None) or []
    for item in results:
        nodes.append(
            DbtNodeResult(
                unique_id=item.node.unique_id, status=str(item.status), message=item.message
            )
        )
    failed = [n for n in nodes if n.status in {"error", "fail", "runtime error"}]
    if not outcome.success or failed:
        detail = "; ".join(f"{n.unique_id}: {n.message}" for n in failed[:10])
        if outcome.exception is not None:
            detail = f"{detail} {outcome.exception}".strip()
        raise DbtRunError(f"dbt {' '.join(args)} failed: {detail or 'see dbt logs'}")
    log.info("dbt.completed", command=" ".join(args), nodes=len(nodes))
    return nodes
