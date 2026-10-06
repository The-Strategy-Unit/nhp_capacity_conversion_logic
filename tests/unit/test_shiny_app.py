import importlib.util
import os
from pathlib import Path
from types import ModuleType
from unittest.mock import call, patch

import pandas as pd
import pytest


def _load_app_module() -> ModuleType:
    app_path = Path(__file__).parents[2] / "app.py"
    spec = importlib.util.spec_from_file_location("capacity_conversion_app", app_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {app_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


with patch.dict(os.environ, {"CAPACITY_MODEL_VERSION": "dev"}):
    app = _load_app_module()

APP_ENVIRONMENT = {
    "AZ_STORAGE_EP": "https://storage.example.com",
    "AZ_STORAGE_RESULTS": "results",
    "AZ_TABLE_ENDPOINT": "https://table.example.com",
    "CAPACITY_MODEL_VERSION": "dev",
    "TABLE_NAME": "metadata",
}


def test_app_registers_favicon():
    assert app.FAVICON_DEPENDENCY.head is not None
    html = app.FAVICON_DEPENDENCY.head.get_html_string()

    assert '<link rel="icon" type="image/x-icon" href="favicon.ico"/>' in html
    assert app.app._static_assets["/"] == Path(app.STATIC_ASSETS_DIR)


def _model_run(**overrides) -> dict:
    run = {
        "guid": "guid-123",
        "dataset": "RXX",
        "scenario": "scenario-a",
        "create_datetime": "2026-08-17T14:37:23.4420972Z",
        "app_version": "v6.0",
    }
    run.update(overrides)
    return run


def test_capacity_model_version_is_loaded_from_environment(mocker):
    mocker.patch.dict(
        os.environ,
        {"CAPACITY_MODEL_VERSION": "prod"},
        clear=True,
    )

    configured_app = _load_app_module()

    assert configured_app.CAPACITY_MODEL_VERSION == "prod"


def test_app_requires_capacity_model_version(mocker):
    mocker.patch.dict(os.environ, {}, clear=True)

    with pytest.raises(
        RuntimeError,
        match="Missing required environment variable: CAPACITY_MODEL_VERSION",
    ):
        _load_app_module()


def test_catalogue_frame_shapes_and_parses_model_runs():
    result = app._catalogue_frame([_model_run(app_version="v6.1", extra="ignored")])

    assert list(result.columns) == list(app.CATALOGUE_COLUMNS)
    assert result.loc[0, "guid"] == "guid-123"
    assert result.loc[0, "create_datetime"] == pd.Timestamp(
        "2026-08-17T14:37:23.4420972Z"
    )


def test_catalogue_frame_handles_an_empty_catalogue():
    result = app._catalogue_frame([])

    assert result.empty
    assert list(result.columns) == list(app.CATALOGUE_COLUMNS)


def test_catalogue_frame_requires_all_columns():
    run = _model_run()
    del run["scenario"]
    del run["guid"]

    with pytest.raises(
        ValueError,
        match="Catalogue is missing required columns: guid, scenario",
    ):
        app._catalogue_frame([run])


def test_functional_aggregation_choices_are_newest_first():
    functional_aggregations = app._catalogue_frame(
        [
            _model_run(),
            _model_run(guid="guid-newer", create_datetime="2026-08-18T09:05:00Z"),
        ]
    )

    result = app._functional_aggregation_choices(functional_aggregations)

    assert result == {
        "guid-newer": "18 Aug 2026, 09:05 UTC",
        "guid-123": "17 Aug 2026, 14:37 UTC",
    }


@pytest.mark.parametrize(
    ("groups", "is_local", "expected"),
    [
        (["nhp_devs"], False, True),
        (["nhp_power_users", "other"], False, True),
        (None, True, True),
        (["nhp_provider_RXX"], False, False),
        (None, False, False),
        ([], False, False),
    ],
)
def test_is_privileged(groups, is_local, expected):
    assert app._is_privileged(groups, is_local=is_local) is expected


def test_provider_datasets():
    groups = ["nhp_provider_RXX", "nhp_provider_RYY", "nhp_provider_", "unrelated"]

    assert app._provider_datasets(groups) == {"RXX", "RYY"}
    assert app._provider_datasets(None) == set()


@pytest.mark.parametrize(
    ("dataset", "groups", "is_local", "expected"),
    [
        ("RXX", ["nhp_provider_RXX", "unrelated_group"], False, True),
        ("RYY", ["nhp_provider_RXX"], False, False),
        ("RXX", ["nhp_devs"], False, True),
        ("RXX", ["nhp_power_users"], False, True),
        ("RXX", None, True, True),
        ("RXX", None, False, False),  # fails closed without Connect groups
    ],
)
def test_may_access_dataset(dataset, groups, is_local, expected):
    assert app._may_access_dataset(dataset, groups, is_local=is_local) is expected


def test_available_datasets_for_privileged_users_scans_the_table(mocker):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    load_datasets = mocker.patch.object(
        app, "load_datasets_from_ats", return_value=["RXX", "RYY"]
    )

    result = app._available_datasets(["nhp_devs"], is_local=False)

    assert result == ["RXX", "RYY"]
    load_datasets.assert_called_once_with("https://table.example.com", "metadata")


def test_available_datasets_for_local_development_scans_the_table(mocker):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    load_datasets = mocker.patch.object(
        app, "load_datasets_from_ats", return_value=["RXX"]
    )

    assert app._available_datasets(None, is_local=True) == ["RXX"]
    load_datasets.assert_called_once()


def test_available_datasets_for_providers_uses_groups_without_querying(mocker):
    load_datasets = mocker.patch.object(app, "load_datasets_from_ats")

    result = app._available_datasets(
        ["nhp_provider_RYY", "nhp_provider_RXX", "unrelated_group"],
        is_local=False,
    )

    assert result == ["RXX", "RYY"]
    load_datasets.assert_not_called()


def _metadata(**overrides) -> dict:
    metadata = {
        "guid": "guid-123",
        "dataset": "RXX",
        "scenario": "scenario-a",
        "create_datetime": "2026-08-17T14:37:23.442097+00:00",
        "app_version": "v6.0",
        "aggregated_results_path": "aggregated/results",
    }
    metadata.update(overrides)
    return metadata


def _selection(**overrides) -> dict:
    return {
        "dataset": "RXX",
        "scenario": "scenario-a",
        "guid": "guid-123",
        "groups": ["nhp_provider_RXX"],
        "is_local": False,
    } | overrides


def test_load_authorised_metadata_returns_the_selected_run(mocker):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    metadata = _metadata()
    load_metadata = mocker.patch.object(
        app, "load_metadata_from_ats", return_value=metadata
    )

    result = app._load_authorised_metadata(**_selection())

    assert result == metadata
    load_metadata.assert_called_once_with(
        "RXX", "guid-123", "https://table.example.com", "metadata"
    )


@pytest.mark.parametrize(
    "fetched",
    [
        {"dataset": "RYY"},
        {"scenario": "different-scenario"},
        {"guid": "different-guid"},
    ],
)
def test_load_authorised_metadata_rejects_stale_selections(mocker, fetched):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    mocker.patch.object(
        app, "load_metadata_from_ats", return_value=_metadata(**fetched)
    )

    with pytest.raises(PermissionError, match="not available"):
        app._load_authorised_metadata(**_selection())


def test_load_authorised_metadata_checks_entitlement_before_querying(mocker):
    load_metadata = mocker.patch.object(app, "load_metadata_from_ats")

    with pytest.raises(PermissionError, match="not available"):
        app._load_authorised_metadata(**_selection(groups=["nhp_provider_RYY"]))

    load_metadata.assert_not_called()


def test_load_authorised_metadata_propagates_unusable_runs(mocker):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    mocker.patch.object(
        app,
        "load_metadata_from_ats",
        side_effect=ValueError("unsupported app_version 'dev'"),
    )

    with pytest.raises(ValueError, match="unsupported app_version"):
        app._load_authorised_metadata(**_selection())


def test_is_local_development(mocker):
    mocker.patch.dict(os.environ, {}, clear=True)
    assert app._is_local_development()

    mocker.patch.dict(os.environ, {"POSIT_PRODUCT": "CONNECT"})
    assert not app._is_local_development()


def test_load_capacity_results(mocker):
    mocker.patch.dict(os.environ, APP_ENVIRONMENT, clear=True)
    metadata = _metadata()
    aggregations = pd.DataFrame({"total": [1]})
    load_aggregation = mocker.patch.object(
        app,
        "load_aggregations",
        return_value=aggregations,
    )
    filter_aggregation = mocker.patch.object(
        app,
        "filter_aggregations",
        return_value=aggregations,
    )
    assumptions = pd.DataFrame({"Value": [1]})
    mocker.patch.object(app, "load_assumptions", return_value=assumptions)
    process = mocker.patch.object(app, "process_activity_type")

    data_to_save = app._load_capacity_results(metadata)

    # the aggregations file is downloaded once, then filtered per activity type
    load_aggregation.assert_called_once_with(
        "https://storage.example.com",
        "results",
        "aggregated/results/functional_areas.parquet",
    )
    assert filter_aggregation.call_args_list == [
        call(load_aggregation.return_value, "ALL", activity_type)
        for activity_type in app.ACTIVITY_TYPES
    ]
    process.assert_has_calls(
        [
            call(
                "op",
                aggregations,
                app.calculate_op_capacity,
                assumptions,
                data_to_save,
                preprocess=None,
            ),
            call(
                "aae",
                aggregations,
                app.calculate_aae_capacity,
                assumptions,
                data_to_save,
                preprocess=None,
            ),
            call(
                "ip_daycase",
                aggregations,
                app.calculate_daycase_capacity,
                assumptions,
                data_to_save,
                preprocess=None,
            ),
            call(
                "ip_maternity",
                aggregations,
                app.calculate_maternity_capacity,
                assumptions,
                data_to_save,
                preprocess=app.preprocess_ip_maternity_data,
            ),
            call(
                "ip_wards",
                aggregations,
                app.calculate_ip_wards_capacity,
                assumptions,
                data_to_save,
                preprocess=app.preprocess_ip_wards_data,
            ),
            call(
                "ip_procedures_and_theatres",
                aggregations,
                app.calculate_ip_theatres_capacity,
                assumptions,
                data_to_save,
                preprocess=app.preprocess_ip_theatres_data,
            ),
        ]
    )
    assert process.call_count == 6
    assert data_to_save["assumptions"] is assumptions
    run_metadata = data_to_save["metadata"]
    assert run_metadata.loc["guid"] == "guid-123"
    assert run_metadata.loc["create_datetime"] == "2026-08-17T14:37:23.442097+00:00"
    assert run_metadata.loc["capacity_model_version"] == "dev"
    runtime = run_metadata.loc["capacity_conversion_runtime"]
    assert len(runtime) == 15
    assert runtime[8] == "_"
    assert runtime.replace("_", "").isdigit()
    assert run_metadata.loc[["ip_sites", "op_sites", "aae_sites"]].to_dict() == {
        "ip_sites": "ALL",
        "op_sites": "ALL",
        "aae_sites": "ALL",
    }
    assert "PartitionKey" not in run_metadata.index
    assert "RowKey" not in run_metadata.index
    assert metadata == _metadata()  # the input metadata is not modified


def test_load_capacity_results_requires_storage_configuration(mocker):
    mocker.patch.dict(
        os.environ,
        {
            key: value
            for key, value in APP_ENVIRONMENT.items()
            if key != "AZ_STORAGE_RESULTS"
        },
        clear=True,
    )

    with pytest.raises(
        RuntimeError,
        match="Missing required environment variable: AZ_STORAGE_RESULTS",
    ):
        app._load_capacity_results(_metadata())


def test_create_workbook_uses_shared_writer(mocker):
    data_to_save = {
        "metadata": pd.Series({"guid": "guid-123"}),
    }
    workbook_bytes = b"shared workbook"

    def write_workbook(data, *, destination):
        assert data is data_to_save
        destination.write(workbook_bytes)

    mock_write_workbook = mocker.patch.object(
        app,
        "process_and_save_results_to_excel",
        side_effect=write_workbook,
    )

    result = app._create_workbook(data_to_save)

    assert result == workbook_bytes
    mock_write_workbook.assert_called_once()
