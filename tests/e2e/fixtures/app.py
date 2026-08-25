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
    capacity = pd.DataFrame(
        {"capacity": [10.0, 12.0, 14.0]},
        index=pd.MultiIndex.from_product(
            [["example"], range(3)],
            names=["grouping", "model_run"],
        ),
    )
    return {
        "metadata": pd.Series({"guid": "test-guid"}),
        **{
            f"{activity_type}_capacity": capacity
            for activity_type in capacity_conversion_app.ACTIVITY_TYPES
        },
    }


capacity_conversion_app.load_functional_aggregations_from_ats = (
    _load_functional_aggregations_from_ats
)
capacity_conversion_app.load_metadata_from_ats = _load_metadata_from_ats
capacity_conversion_app._load_capacity_results = _load_capacity_results
app = capacity_conversion_app.app
