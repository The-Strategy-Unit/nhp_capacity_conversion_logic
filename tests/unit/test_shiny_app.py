import importlib.util
import os
from io import BytesIO
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from openpyxl import load_workbook


def _load_app_module() -> ModuleType:
    app_path = Path(__file__).parents[2] / "app.py"
    spec = importlib.util.spec_from_file_location("capacity_conversion_app", app_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {app_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


app = _load_app_module()


def test_load_capacity_results(mocker):
    mocker.patch.dict(
        os.environ,
        {
            "AZ_FUNC_AGG_BLOB_PATH": ("functional-aggregations/v1/guid-123/op.parquet"),
            "AZ_STORAGE_EP": "https://storage.example.com",
            "AZ_STORAGE_RESULTS": "results",
        },
    )
    mock_datetime = mocker.patch.object(app, "datetime")
    mock_datetime.now.return_value.strftime.return_value = "20260101_120000"

    connection = mocker.Mock()
    connect = mocker.patch.object(
        app,
        "connect_to_container",
        return_value=connection,
    )
    aggregations = pd.DataFrame({"total": [1]})
    load_parquet = mocker.patch.object(
        app,
        "load_parquet_file",
        return_value=aggregations,
    )
    assumptions = pd.DataFrame({"Value": [1]})
    mocker.patch.object(app, "load_assumptions", return_value=assumptions)
    process = mocker.patch.object(app, "process_activity_type")

    activity_type, data_to_save = app._load_capacity_results()

    assert activity_type == "op"
    connect.assert_called_once_with("https://storage.example.com", "results")
    load_parquet.assert_called_once_with(
        connection,
        "functional-aggregations/v1/guid-123/op.parquet",
    )
    process.assert_called_once_with(
        "op",
        aggregations,
        app.calculate_op_capacity,
        assumptions,
        data_to_save,
    )
    assert data_to_save["metadata"].to_dict() == {
        "guid": "guid-123",
        "capacity_model_version": "v1",
        "capacity_conversion_runtime": "20260101_120000",
    }


def test_load_capacity_results_rejects_invalid_blob_path(mocker):
    mocker.patch.dict(
        os.environ,
        {"AZ_FUNC_AGG_BLOB_PATH": "op.parquet"},
    )

    with pytest.raises(
        ValueError,
        match="AZ_FUNC_AGG_BLOB_PATH must have the form",
    ):
        app._load_capacity_results()


def test_load_capacity_results_rejects_unknown_activity_type(mocker):
    mocker.patch.dict(
        os.environ,
        {
            "AZ_FUNC_AGG_BLOB_PATH": (
                "functional-aggregations/v1/guid-123/unknown.parquet"
            )
        },
    )

    with pytest.raises(ValueError, match="Unsupported aggregation type 'unknown'"):
        app._load_capacity_results()


def test_create_workbook():
    capacity = pd.DataFrame(
        {
            "output": ["room", "room"],
            "model_run": [1, 2],
            "value": [1.0, 3.0],
        }
    ).set_index(["output", "model_run"])
    data_to_save = {
        "metadata": pd.Series({"guid": "guid-123"}),
        "op_capacity": capacity,
    }

    workbook = load_workbook(BytesIO(app._create_workbook(data_to_save)))

    assert workbook.sheetnames == ["metadata", "op_capacity"]
    rows = list(workbook["op_capacity"].values)
    assert rows[0] == ("output", "p10", "mean", "p90")
    assert rows[1] == ("room", 1.2, 2, 2.8)
