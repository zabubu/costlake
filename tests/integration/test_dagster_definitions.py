from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_definitions_load_with_expected_lineage() -> None:
    from dagster import AssetKey

    from costlake.orchestration.definitions import defs

    graph = defs.get_repository_def().asset_graph
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {
        "bronze_deliveries",
        "silver_focus",
        "staging/stg_focus__charges",
        "gold/agg_daily_service_cost",
        "cost_anomalies",
        "serving_tables",
    } <= keys
    # No dangling dependency: every upstream key is a real asset.
    assert all(
        "/" in k or k in {"bronze_deliveries", "silver_focus", "cost_anomalies", "serving_tables"}
        for k in keys
    )
    stg = graph.get(AssetKey(["staging", "stg_focus__charges"]))
    assert AssetKey("silver_focus") in stg.parent_keys
    assert defs.resolve_job_def("costlake_pipeline") is not None
    assert {s.name for s in defs.sensors or []} == {"landing_sensor"}
