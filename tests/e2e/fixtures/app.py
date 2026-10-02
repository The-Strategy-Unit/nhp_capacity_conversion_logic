"""Load the real app with deterministic data so e2e tests avoid external services."""

import importlib.util
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import pandas as pd


class _CapacityConversionAppModule(Protocol):
    ACTIVITY_TYPES: tuple[str, ...]
    _load_capacity_results: Callable[[dict], dict[str, pd.DataFrame | pd.Series]]
    load_datasets_from_ats: Callable[[str, str], list[str]]
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


DATASET = "RXX"

FUNCTIONAL_AGGREGATION = {
    "guid": "test-guid",
    "dataset": DATASET,
    "scenario": "Example scenario",
    "create_datetime": "2026-08-17T14:37:23.4420972Z",
    "app_version": "v6.0",
}


def _load_datasets_from_ats(storage_endpoint: str, table_name: str) -> list[str]:
    return [DATASET]


def _load_functional_aggregations_from_ats(
    dataset: str,
    storage_endpoint: str,
    table_name: str,
) -> list[dict]:
    assert dataset == DATASET
    return [FUNCTIONAL_AGGREGATION.copy()]


def _load_metadata_from_ats(
    dataset: str,
    guid: str,
    storage_endpoint: str,
    table_name: str,
) -> dict:
    assert dataset == DATASET
    assert guid == FUNCTIONAL_AGGREGATION["guid"]
    return {
        **FUNCTIONAL_AGGREGATION,
        "start_year": "2023",
        "end_year": "2041",
        "model_run_id": "test-run",
        "aggregated_results_path": "aggregated/results",
    }


def _load_capacity_results(model_run: dict) -> dict[str, pd.DataFrame | pd.Series]:
    assert model_run["guid"] == FUNCTIONAL_AGGREGATION["guid"]
    functional_areas = pd.DataFrame(
        {
            "model_run": list(range(11)),
            "group": ["group"] * 11,
            "value": list(range(11)),
        }
    ).set_index(["model_run", "group"])
    baseline = pd.DataFrame(
        {
            "group": ["group"] * 11,
            "value": list(range(11)),
        }
    ).set_index("group")
    capacity = pd.DataFrame(
        {"capacity": [10.0, 12.0, 14.0]},
        index=pd.MultiIndex.from_product(
            [["example"], range(3)],
            names=["output", "model_run"],
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


capacity_conversion_app.load_datasets_from_ats = _load_datasets_from_ats
capacity_conversion_app.load_functional_aggregations_from_ats = (
    _load_functional_aggregations_from_ats
)
capacity_conversion_app.load_metadata_from_ats = _load_metadata_from_ats
capacity_conversion_app._load_capacity_results = _load_capacity_results
app = capacity_conversion_app.app
