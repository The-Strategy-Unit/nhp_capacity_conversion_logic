"""Load the real app with deterministic data so e2e tests avoid external services."""

import importlib.util
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import pandas as pd


class _CapacityConversionAppModule(Protocol):
    ACTIVITY_TYPES: tuple[str, ...]
    _load_capacity_results: Callable[[], dict[str, pd.DataFrame | pd.Series]]
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


def _load_capacity_results() -> dict[str, pd.DataFrame | pd.Series]:
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


capacity_conversion_app._load_capacity_results = _load_capacity_results
app = capacity_conversion_app.app
