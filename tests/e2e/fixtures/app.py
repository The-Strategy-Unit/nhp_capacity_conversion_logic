"""Load the real app with deterministic data so e2e tests avoid external services."""

import importlib.util
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import pandas as pd


class _CapacityConversionAppModule(Protocol):
    ACTIVITY_TYPES: tuple[str, ...]
    _load_capacity_results: Callable[[dict], dict[str, pd.DataFrame | pd.Series]]
    load_functional_aggregations_from_ats: Callable[[str, str, str], list[dict]]
    load_metadata_from_ats: Callable[[str, str, str, str], dict]
    app: object


def _load_app_module() -> _CapacityConversionAppModule:
    app_path = Path(__file__).parents[3] / "app.py"
    spec = importlib.util.spec_from_file_location("capacity_conversion_app", app_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {app_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(_CapacityConversionAppModule, module)


capacity_conversion_app = _load_app_module()


FUNCTIONAL_AGGREGATION = {
    "PartitionKey": "dev",
    "RowKey": "test-guid",
    "dataset": "RXX",
    "scenario_name": "Example scenario",
    "scenario_runtime": "20260817_143723",
}


def _load_functional_aggregations_from_ats(
    storage_endpoint: str,
    table_name: str,
    capacity_model_version: str,
) -> list[dict]:
    assert capacity_model_version == FUNCTIONAL_AGGREGATION["PartitionKey"]
    return [FUNCTIONAL_AGGREGATION.copy()]


def _load_metadata_from_ats(
    guid: str,
    storage_endpoint: str,
    table_name: str,
    capacity_model_version: str,
) -> dict:
    assert guid == FUNCTIONAL_AGGREGATION["RowKey"]
    assert capacity_model_version == FUNCTIONAL_AGGREGATION["PartitionKey"]
    return FUNCTIONAL_AGGREGATION.copy()


def _load_capacity_results(model_run: dict) -> dict[str, pd.DataFrame | pd.Series]:
    assert model_run["RowKey"] == FUNCTIONAL_AGGREGATION["RowKey"]
    functional_areas = pd.DataFrame(
        {"activity": [100.0, 110.0, 120.0]},
        index=pd.MultiIndex.from_product(
            [["example_activity"], range(3)],
            names=["grouping", "model_run"],
        ),
    )
    baseline = pd.DataFrame(
        {
            "total": [100.0],
            "spells": [50.0],
            "beddays": [200.0],
            "total_theatre_time": [300.0],
        },
        index=pd.Index(["example_activity"], name="grouping"),
    )
    capacity = pd.DataFrame(
        {"capacity": [10.0, 12.0, 14.0]},
        index=pd.MultiIndex.from_product(
            [["example"], range(3)],
            names=["grouping", "model_run"],
        ),
    )
    results: dict[str, pd.DataFrame | pd.Series] = {
        "metadata": pd.Series(
            {
                "guid": "test-guid",
                "dataset": "RXX",
                "capacity_model_version": "dev",
                "ip_sites": "ALL",
                "op_sites": "ALL",
                "aae_sites": "ALL",
                "capacity_conversion_runtime": "20260911_123456",
            }
        ),
        "assumptions": pd.DataFrame(
            {"Value": [1.0]},
            index=pd.Index(["example_assumption"], name="Assumption ID"),
        ),
    }
    for activity_type in capacity_conversion_app.ACTIVITY_TYPES:
        results[f"{activity_type}_fun_area_groupings"] = functional_areas
        results[f"{activity_type}_baseline"] = baseline
        results[f"{activity_type}_capacity"] = capacity
    return results


capacity_conversion_app.load_functional_aggregations_from_ats = (
    _load_functional_aggregations_from_ats
)
capacity_conversion_app.load_metadata_from_ats = _load_metadata_from_ats
capacity_conversion_app._load_capacity_results = _load_capacity_results
app = capacity_conversion_app.app
