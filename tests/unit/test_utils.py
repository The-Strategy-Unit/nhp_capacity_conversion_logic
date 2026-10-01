import datetime
import logging
from unittest.mock import call

import pandas as pd
import pytest
from azure.core.exceptions import ResourceNotFoundError
from pandas.testing import assert_frame_equal

from nhp.capacity_conversion.config import (
    CATALOGUE_FIELDS,
    REQUIRED_METADATA_FIELDS,
)
from nhp.capacity_conversion.utils import (
    add_run_details,
    calculate_prediction_intervals_and_mean,
    configure_logging,
    connect_to_container,
    connect_to_table,
    create_aggregations_path,
    entity_to_metadata,
    filter_aggregations,
    get_baseline_activity,
    is_supported_app_version,
    load_aggregations,
    load_assumptions,
    load_datasets_from_ats,
    load_functional_aggregations_from_ats,
    load_metadata_from_ats,
    load_parquet_file,
    metadata_problem,
    parse_app_version,
    process_activity_type,
    run_single_activity_type,
    validate_required_env_vars,
    validate_sites,
)


@pytest.fixture
def filter_agg_df():
    return pd.DataFrame(
        {
            "model_run": [0] * 6,
            "sitetret": ["A", "B"] * 3,
            "functional_area": ["X", "X", "Y", "Y", "X", "X"],
            "measure": ["measure_1"] * 4 + ["measure_2"] * 2,
            "value": [1] * 6,
        }
    ).set_index("model_run")


def test_configure_logging(mocker):
    mock_basic_config = mocker.patch(
        "nhp.capacity_conversion.utils.logging.basicConfig"
    )
    mock_azure_logger = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.logging.getLogger",
        return_value=mock_azure_logger,
    )

    configure_logging(logging.DEBUG)

    mock_basic_config.assert_called_once_with(
        level=logging.DEBUG,
        format="%(message)s",
        force=True,
    )
    mock_azure_logger.setLevel.assert_called_once_with(logging.WARNING)


def test_connect_to_container(mocker):
    credential = mocker.Mock()
    container = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.DefaultAzureCredential",
        return_value=credential,
    )
    mock_container_client = mocker.patch(
        "nhp.capacity_conversion.utils.ContainerClient",
        return_value=container,
    )

    result = connect_to_container("https://example.blob.core.windows.net", "results")

    assert result is container
    mock_container_client.assert_called_once_with(
        account_url="https://example.blob.core.windows.net",
        container_name="results",
        credential=credential,
    )


@pytest.mark.parametrize(
    ("account_url", "container_name", "message"),
    [
        (None, "results", "An account URL is required"),
        ("https://example.blob.core.windows.net", None, "A container name is required"),
    ],
)
def test_connect_to_container_requires_configuration(
    account_url,
    container_name,
    message,
):
    with pytest.raises(ValueError, match=message):
        connect_to_container(account_url, container_name)


def test_load_parquet_file(mocker):
    container_client = mocker.Mock()
    parquet_bytes = b"parquet data"
    container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = parquet_bytes
    expected = pd.DataFrame({"value": [1]})
    mock_read_parquet = mocker.patch(
        "nhp.capacity_conversion.utils.pd.read_parquet",
        return_value=expected,
    )

    result = load_parquet_file(container_client, "path/results.parquet")

    assert result is expected
    container_client.get_blob_client.assert_called_once_with("path/results.parquet")
    parquet_stream = mock_read_parquet.call_args.args[0]
    assert parquet_stream.getvalue() == parquet_bytes
    assert mock_read_parquet.call_args.kwargs == {"engine": "pyarrow"}


def test_get_baseline_activity():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "total": [3, 10, 100] * 3,
        }
    ).set_index(["model_run", "grouping"])

    result = get_baseline_activity(aggregations)

    assert pd.isna(result.loc["a", "total"])  # 3 is suppressed (1–7)
    assert result.loc["b", "total"] == 10  # 10 rounds to 10
    assert result.loc["c", "total"] == 100  # 100 rounds to 100


def test_calculate_prediction_intervals_and_mean():
    # arrange
    test_activity = pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    expected = {"mean": 5.0, "p10": 1.0, "p90": 9.0}

    # act
    actual = calculate_prediction_intervals_and_mean(test_activity)

    # assert
    assert actual == expected


def test_load_assumptions(tmp_path):
    # arrange
    csv_file = tmp_path / "assumptions.csv"
    csv_file.write_text("Assumption ID,Value\nA1,10\nA2,20\n")
    expected = pd.DataFrame(
        {"Value": [10, 20]},
        index=pd.Index(["A1", "A2"], name="Assumption ID"),
    )
    # act
    result = load_assumptions(csv_file)

    # assert
    assert_frame_equal(expected, result)


def make_entity(**overrides):
    """A valid Azure Table Storage entity for a supported model run."""
    entity = {
        "PartitionKey": "RXX",
        "RowKey": "GUID123",
        "app_version": "v6.0",
        "dataset": "RXX",
        "start_year": 2023,
        "end_year": 2041,
        "scenario": "Example scenario",
        "create_datetime": datetime.datetime(
            2026, 9, 25, 13, 31, 16, 442097, tzinfo=datetime.UTC
        ),
        "model_run_id": "run-1",
        "aggregated_results_path": "aggregated/results",
        "do_not_include": "ignored",
    }
    entity.update(overrides)
    return entity


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("v6.0", (6, 0)),
        ("V5.2", (5, 2)),
        (" v10.1.3 ", (10, 1, 3)),
        ("6", (6,)),
        ("dev", None),
        ("", None),
        ("v6.0-rc1", None),
        (None, None),
    ],
)
def test_parse_app_version(value, expected):
    assert parse_app_version(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("v6.0", True),
        ("v6", True),  # padded to 6.0
        ("v6.0.1", True),
        ("v6.1", True),
        ("v10.0", True),  # numeric, not lexical, comparison
        ("v5.2", False),
        ("v5.9.9", False),
        ("dev", False),
        ("", False),
        (None, False),
    ],
)
def test_is_supported_app_version(value, expected):
    assert is_supported_app_version(value) is expected


def test_connect_to_table(mocker):
    credential = mocker.Mock()
    table_client = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.DefaultAzureCredential",
        return_value=credential,
    )
    table_client_class = mocker.patch(
        "nhp.capacity_conversion.utils.TableClient",
        return_value=table_client,
    )

    result = connect_to_table("https://example.table.core.windows.net", "catalogue")

    assert result is table_client
    table_client_class.assert_called_once_with(
        endpoint="https://example.table.core.windows.net",
        table_name="catalogue",
        credential=credential,
    )


def test_entity_to_metadata():
    result = entity_to_metadata(make_entity())

    assert result == {
        "app_version": "v6.0",
        "dataset": "RXX",
        "start_year": "2023",
        "end_year": "2041",
        "scenario": "Example scenario",
        "create_datetime": "2026-09-25T13:31:16.442097+00:00",
        "model_run_id": "run-1",
        "aggregated_results_path": "aggregated/results",
        "guid": "GUID123",
    }


def test_entity_to_metadata_ignores_absent_fields():
    entity = {"PartitionKey": "RXX", "RowKey": "GUID123", "dataset": "RXX"}

    assert entity_to_metadata(entity) == {"dataset": "RXX", "guid": "GUID123"}


def test_metadata_problem_valid():
    entity = make_entity()
    metadata = entity_to_metadata(entity)

    assert metadata_problem(entity, metadata, "RXX", REQUIRED_METADATA_FIELDS) is None


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"scenario": "  "}, "missing values for scenario"),
        ({"aggregated_results_path": ""}, "missing values for aggregated_results_path"),
        ({"PartitionKey": "RYY"}, "dataset does not match the table partition"),
        ({"dataset": "RYY"}, "dataset does not match the table partition"),
        ({"app_version": "v5.2"}, "unsupported app_version 'v5.2'"),
        ({"app_version": "dev"}, "unsupported app_version 'dev'"),
        ({"create_datetime": "not a date"}, "invalid create_datetime 'not a date'"),
    ],
)
def test_metadata_problem_invalid(overrides, expected):
    entity = make_entity(**overrides)
    metadata = entity_to_metadata(entity)

    problem = metadata_problem(entity, metadata, "RXX", REQUIRED_METADATA_FIELDS)

    assert problem == expected


def test_metadata_problem_only_checks_required_fields():
    entity = make_entity(aggregated_results_path="")
    metadata = entity_to_metadata(entity)

    assert (
        metadata_problem(entity, metadata, "RXX", CATALOGUE_FIELDS + ("guid",)) is None
    )


def test_load_metadata_from_ats(mocker):
    # arrange
    mock_table_client = mocker.Mock()
    mock_connect = mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=mock_table_client,
    )
    mock_table_client.get_entity.return_value = make_entity()

    # act
    result = load_metadata_from_ats(
        dataset="RXX",
        guid="GUID123",
        storage_endpoint="https://example.table.core.windows.net",
        table_name="demotable",
    )

    # assert
    mock_connect.assert_called_once_with(
        "https://example.table.core.windows.net", "demotable"
    )
    mock_table_client.get_entity.assert_called_once_with(
        partition_key="RXX",
        row_key="GUID123",
    )
    assert "do_not_include" not in result
    assert len(result) == 9
    assert result["guid"] == "GUID123"
    assert result["create_datetime"] == "2026-09-25T13:31:16.442097+00:00"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"app_version": "dev"}, "unsupported app_version 'dev'"),
        ({"app_version": "v5.2"}, "unsupported app_version 'v5.2'"),
        ({"aggregated_results_path": ""}, "missing values for aggregated_results_path"),
        ({"PartitionKey": "RYY"}, "dataset does not match the table partition"),
    ],
)
def test_load_metadata_from_ats_unusable_run(mocker, overrides, message):
    mock_table_client = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=mock_table_client,
    )
    mock_table_client.get_entity.return_value = make_entity(**overrides)

    with pytest.raises(ValueError, match=message):
        load_metadata_from_ats("RXX", "GUID123", "endpoint", "demotable")


def test_load_metadata_from_ats_not_found(mocker):
    mock_table_client = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=mock_table_client,
    )
    mock_table_client.get_entity.side_effect = ResourceNotFoundError

    with pytest.raises(ResourceNotFoundError):
        load_metadata_from_ats(
            dataset="RXX",
            guid="missing-guid",
            storage_endpoint="https://example.table.core.windows.net",
            table_name="demotable",
        )


def test_load_functional_aggregations_from_ats(mocker, caplog):
    caplog.set_level(logging.INFO)
    table_client = mocker.Mock()
    mock_connect = mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=table_client,
    )
    table_client.query_entities.return_value = iter(
        [
            make_entity(RowKey="guid-1"),
            make_entity(RowKey="guid-2", scenario="Another scenario"),
            make_entity(RowKey="guid-old", app_version="v5.2"),
            make_entity(RowKey="guid-dev", app_version="dev"),
            make_entity(RowKey="guid-blank", scenario=""),
        ]
    )

    result = load_functional_aggregations_from_ats(
        "RXX",
        "https://example.table.core.windows.net",
        "catalogue",
    )

    mock_connect.assert_called_once_with(
        "https://example.table.core.windows.net", "catalogue"
    )
    table_client.query_entities.assert_called_once_with(
        query_filter="PartitionKey eq @dataset",
        parameters={"dataset": "RXX"},
        select=["PartitionKey", "RowKey", *CATALOGUE_FIELDS],
    )
    assert [run["guid"] for run in result] == ["guid-1", "guid-2"]
    assert result[1]["scenario"] == "Another scenario"
    assert "Ignored 3 model runs in dataset RXX" in caplog.text


def test_load_functional_aggregations_from_ats_nothing_ignored(mocker, caplog):
    caplog.set_level(logging.INFO)
    table_client = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=table_client,
    )
    table_client.query_entities.return_value = iter([make_entity()])

    result = load_functional_aggregations_from_ats("RXX", "endpoint", "catalogue")

    assert len(result) == 1
    assert "Ignored" not in caplog.text


def test_load_datasets_from_ats(mocker):
    table_client = mocker.Mock()
    mock_connect = mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_table",
        return_value=table_client,
    )
    table_client.list_entities.return_value = iter(
        [
            {"PartitionKey": "RYY", "app_version": "v6.0"},
            {"PartitionKey": "RXX", "app_version": "v6.1"},
            {"PartitionKey": "RXX", "app_version": "v6.0"},  # duplicate dataset
            {"PartitionKey": "RZZ", "app_version": "v5.2"},  # too old
            {"PartitionKey": "RWW", "app_version": "dev"},  # dev run
            {"PartitionKey": "RVV"},  # no app_version
        ]
    )

    result = load_datasets_from_ats(
        "https://example.table.core.windows.net", "catalogue"
    )

    mock_connect.assert_called_once_with(
        "https://example.table.core.windows.net", "catalogue"
    )
    table_client.list_entities.assert_called_once_with(
        select=["PartitionKey", "app_version"]
    )
    assert result == ["RXX", "RYY"]


@pytest.mark.parametrize("path", ["aggregated/results", "aggregated/results/"])
def test_create_aggregations_path(path):
    result = create_aggregations_path({"aggregated_results_path": path})

    assert result == "aggregated/results/functional_areas.parquet"


def test_add_run_details(mocker):
    mock_now = mocker.Mock()
    mock_now.strftime.return_value = "20250101_120000"
    mock_datetime = mocker.patch("nhp.capacity_conversion.utils.datetime.datetime")
    mock_datetime.now.return_value = mock_now
    metadata = {"dataset": "RXX", "guid": "GUID123"}

    result = add_run_details(metadata, "dev", ip_sites="ALL", op_sites="A,B")

    assert result == {
        "dataset": "RXX",
        "guid": "GUID123",
        "capacity_conversion_runtime": "20250101_120000",
        "capacity_model_version": "dev",
        "ip_sites": "ALL",
        "op_sites": "A,B",
    }
    mock_datetime.now.assert_called_once_with(tz=datetime.UTC)
    mock_now.strftime.assert_called_once_with("%Y%m%d_%H%M%S")
    assert metadata == {"dataset": "RXX", "guid": "GUID123"}  # input not modified


def test_validate_required_env_vars_success(mocker):
    # arrange
    mocker.patch("nhp.capacity_conversion.utils.load_dotenv")

    mock_env = {
        "AZ_STORAGE_EP": "endpoint",
        "AZ_STORAGE_RESULTS": "results",
        "TABLE_NAME": "table",
        "AZ_TABLE_ENDPOINT": "table_endpoint",
        "CAPACITY_MODEL_VERSION": "capacity_model_version",
    }

    mocker.patch(
        "nhp.capacity_conversion.utils.os.getenv",
        side_effect=lambda key: mock_env.get(key),
    )

    # act
    result = validate_required_env_vars()

    # assert
    assert result == mock_env


def test_validate_required_env_vars_missing(mocker):
    # arrange
    mocker.patch("nhp.capacity_conversion.utils.load_dotenv")

    mock_env = {
        "AZ_STORAGE_EP": "endpoint",
        "AZ_STORAGE_RESULTS": None,
        "TABLE_NAME": "",
        "AZ_TABLE_ENDPOINT": "table_endpoint",
    }

    mocker.patch(
        "nhp.capacity_conversion.utils.os.getenv",
        side_effect=lambda key: mock_env.get(key),
    )

    # act / assert
    with pytest.raises(EnvironmentError) as exc_info:
        validate_required_env_vars()

    error_message = str(exc_info.value)

    assert "AZ_STORAGE_RESULTS" in error_message
    assert "TABLE_NAME" in error_message


def test_load_aggregations(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mock_connection = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.utils.connect_to_container",
        return_value=mock_connection,
    )
    mock_load_parquet_file = mocker.patch(
        "nhp.capacity_conversion.utils.load_parquet_file",
        return_value=pd.DataFrame({"col": [1]}),
    )

    # act
    load_aggregations("url", "container", "path")

    # assert
    assert "Loading data from path..." in caplog.text
    mock_load_parquet_file.assert_called_once_with(mock_connection, "path")


def test_process_activity_type_with_ip_wards_preprocess(mocker):
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b"] * 3,
            "model_run": [0, 0, 1, 1, 2, 2],
            "total": [1, 2, 3, 4, 5, 6],
        }
    ).set_index(["grouping", "model_run"])

    assumptions = pd.DataFrame(
        {"Value": [100]},
        index=["test_assumption"],
    )

    data_to_save = {}

    preprocessed = pd.DataFrame(
        {
            "total": [10, 20, 30, 40, 50, 60],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("a", 0),
                ("b", 0),
                ("a", 1),
                ("b", 1),
                ("a", 2),
                ("b", 2),
            ],
            names=["grouping", "model_run"],
        ),
    )

    mock_preprocess = mocker.Mock(return_value=preprocessed)
    mock_calculate = mocker.Mock(return_value=pd.DataFrame())

    process_activity_type(
        "ip_wards",
        aggregations,
        mock_calculate,
        assumptions,
        data_to_save,
        preprocess=mock_preprocess,
    )

    # Check that the ip_wards-specific preprocess branch was used.
    mock_preprocess.assert_called_once_with(
        aggregations,
        assumptions,
    )

    # Baseline should be excluded before calculating capacity.
    expected_functional_areas = preprocessed.loc[
        preprocessed.index.get_level_values("model_run") != 0,
        :,
    ]

    mock_calculate.assert_called_once()

    actual_functional_areas, actual_assumptions = mock_calculate.call_args.args

    assert_frame_equal(
        actual_functional_areas,
        expected_functional_areas,
    )

    assert_frame_equal(
        actual_assumptions,
        assumptions,
    )

    assert_frame_equal(
        data_to_save["ip_wards_fun_area_groupings"],
        expected_functional_areas,
    )

    assert "ip_wards_capacity" in data_to_save


def test_process_activity_type_with_preprocess():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b"] * 3,
            "model_run": [0] * 2 + [1] * 2 + [2] * 2,
            "total": [1, 2, 3, 4, 5, 6],
        }
    ).set_index(["grouping", "model_run"])
    assumptions = pd.DataFrame({"Value": []})
    data_to_save = {}

    def my_preprocess(df):
        return df

    process_activity_type(
        "test_type",
        aggregations,
        lambda sa, au: pd.DataFrame(),
        assumptions,
        data_to_save,
        preprocess=my_preprocess,
    )

    assert data_to_save["test_type_fun_area_groupings"] is not None


def test_process_activity_type_with_baseline():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b"] * 3,
            "model_run": [0] * 2 + [1] * 2 + [2] * 2,
            "total": [1, 2, 3, 4, 5, 6],
        }
    ).set_index(["grouping", "model_run"])
    assumptions = pd.DataFrame({"Value": []})
    data_to_save = {}

    process_activity_type(
        "test_type",
        aggregations,
        lambda sa, au: pd.DataFrame(),
        assumptions,
        data_to_save,
        include_baseline=True,
    )

    assert data_to_save["test_type_baseline"] is not None


def test_process_activity_type_without_baseline():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b"] * 3,
            "model_run": [0] * 2 + [1] * 2 + [2] * 2,
            "total": [1, 2, 3, 4, 5, 6],
        }
    ).set_index(["grouping", "model_run"])
    assumptions = pd.DataFrame({"Value": []})
    data_to_save = {}

    process_activity_type(
        "test_type",
        aggregations,
        lambda sa, au: pd.DataFrame(),
        assumptions,
        data_to_save,
        include_baseline=False,
    )

    assert "test_type_baseline" not in data_to_save
    assert "test_type_capacity" in data_to_save


def test_run_single_activity_type(mocker):
    # Arrange
    activity_type = "activity_type"
    calculate_fn = mocker.Mock()
    preprocess = mocker.Mock()

    mock_args = mocker.Mock(
        dataset="dataset",
        guid="test-guid",
        path_to_assumptions_file="assumptions.csv",
        sites="sites",
    )

    mock_parser = mocker.Mock()
    mock_parser.parse_args.return_value = mock_args

    mocker.patch(
        "nhp.capacity_conversion.utils.argparse.ArgumentParser",
        return_value=mock_parser,
    )

    mocker.patch(
        "nhp.capacity_conversion.utils.validate_required_env_vars",
        return_value={
            "AZ_TABLE_ENDPOINT": "table-endpoint",
            "TABLE_NAME": "table-name",
            "AZ_STORAGE_EP": "storage-endpoint",
            "AZ_STORAGE_RESULTS": "storage-results",
            "CAPACITY_MODEL_VERSION": "capacity-model-version",
        },
    )

    metadata = {
        "guid": "test-guid",
        "foo": "bar",
        "aggregated_results_path": "aggregated_results_path",
    }
    load_metadata = mocker.patch(
        "nhp.capacity_conversion.utils.load_metadata_from_ats",
        return_value=metadata,
    )

    assumptions = pd.DataFrame({"a": [1]})
    mocker.patch(
        "nhp.capacity_conversion.utils.load_assumptions",
        return_value=assumptions,
    )

    aggregations = pd.DataFrame({"b": [2]})
    mock_load = mocker.patch(
        "nhp.capacity_conversion.utils.load_aggregations",
        return_value=aggregations,
    )
    mock_filter = mocker.patch(
        "nhp.capacity_conversion.utils.filter_aggregations",
        return_value=aggregations,
    )

    process_activity = mocker.patch(
        "nhp.capacity_conversion.utils.process_activity_type"
    )
    save_results = mocker.patch(
        "nhp.capacity_conversion.utils.process_and_save_results_to_excel"
    )
    mocker.patch("nhp.capacity_conversion.utils.configure_logging")

    # Act
    result = run_single_activity_type(
        activity_type=activity_type,
        calculate_fn=calculate_fn,
        preprocess=preprocess,
        include_baseline=True,
    )

    # Assert
    assert result == 0

    load_metadata.assert_called_once_with(
        "dataset",
        "test-guid",
        "table-endpoint",
        "table-name",
    )
    mock_load.assert_called_once_with(
        "storage-endpoint",
        "storage-results",
        "aggregated_results_path/functional_areas.parquet",
    )
    process_activity.assert_called_once()

    _, kwargs = process_activity.call_args

    assert kwargs["name"] == activity_type
    assert kwargs["aggregations"] is aggregations
    assert kwargs["calculate_fn"] is calculate_fn
    assert kwargs["assumptions"] is assumptions
    assert kwargs["preprocess"] is preprocess
    assert kwargs["include_baseline"] is True
    mock_filter.assert_called_once_with(aggregations, "sites", "activity_type")

    # Metadata should have been augmented with the runtime
    data_to_save = kwargs["data_to_save"]
    assert "metadata" in data_to_save
    assert "capacity_conversion_runtime" in data_to_save["metadata"].index
    assert data_to_save["metadata"]["sites"] == "sites"
    assert (
        data_to_save["metadata"]["capacity_model_version"] == "capacity-model-version"
    )
    assert data_to_save["metadata"]["foo"] == "bar"

    save_results.assert_called_once_with(data_to_save)


def test_validate_sites_invalid(filter_agg_df):
    with pytest.raises(ValueError):
        validate_sites(filter_agg_df, ["C"])


def test_validate_sites_valid(filter_agg_df):
    validate_sites(filter_agg_df, ["A"])


def test_filter_aggregations_all(filter_agg_df, mocker):
    mocker.patch(
        "nhp.capacity_conversion.utils.AGGREGATION_SUBSETS",
        {"activity_type": ["X"]},
    )
    expected = pd.DataFrame(
        {
            "model_run": [0, 0],
            "functional_area": ["X", "X"],
            "measure": ["measure_1", "measure_2"],
            "value": [2, 2],
        }
    ).set_index(["model_run", "functional_area", "measure"])
    actual = filter_aggregations(filter_agg_df, "ALL", "activity_type")
    assert_frame_equal(actual, expected)


def test_filter_aggregations(filter_agg_df, mocker):
    mocker.patch(
        "nhp.capacity_conversion.utils.AGGREGATION_SUBSETS",
        {"activity_type": ["X"]},
    )
    expected = pd.DataFrame(
        {
            "model_run": [0] * 2,
            "functional_area": [
                "X",
                "X",
            ],
            "measure": ["measure_1", "measure_2"],
            "value": [1, 1],
        }
    ).set_index(["model_run", "functional_area", "measure"])
    actual = filter_aggregations(filter_agg_df, "A", "activity_type")
    assert_frame_equal(actual, expected)


def test_filter_aggregations_missing_functional_area(filter_agg_df, mocker):
    mocker.patch(
        "nhp.capacity_conversion.utils.AGGREGATION_SUBSETS",
        {"activity_type": ["X", "Z"]},
    )
    mock_logger = mocker.patch("nhp.capacity_conversion.utils.logger")
    expected = pd.DataFrame(
        {
            "model_run": [0] * 2,
            "functional_area": [
                "X",
                "X",
            ],
            "measure": ["measure_1", "measure_2"],
            "value": [1, 1],
        }
    ).set_index(["model_run", "functional_area", "measure"])
    actual = filter_aggregations(filter_agg_df, "A", "activity_type")
    assert_frame_equal(actual, expected)
    assert mock_logger.info.call_args_list == [
        call("Filtering to activity_type"),
        call("Functional areas not found in aggregations: ['Z']"),
        call("Filtering by sites: A"),
    ]
