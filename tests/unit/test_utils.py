import pandas as pd
import pytest
from azure.core.exceptions import ResourceNotFoundError
from pandas.testing import assert_frame_equal

from nhp.capacity_conversion.utils import (
    calculate_prediction_intervals_and_mean,
    create_aggregations_path,
    get_baseline_activity,
    load_aggregations,
    load_assumptions,
    load_metadata_from_ats,
    process_activity_type,
    process_and_save_results_to_excel,
    run_single_activity_type,
    summarise_model_runs,
    validate_required_env_vars,
)


def test_summarise_model_runs():
    df = pd.DataFrame(
        {
            "model_run": list(range(11)),
            "group": ["group"] * 11,
            "value": list(range(11)),
        }
    ).set_index(["model_run", "group"])
    expected = pd.DataFrame(
        {"group": ["group"], "p10": [1.0], "mean": [5.0], "p90": [9.0]}
    ).set_index("group")
    actual = summarise_model_runs(df)
    assert_frame_equal(actual, expected)


def test_summarise_model_runs_with_multiple_cols():
    df = pd.DataFrame(
        {
            "model_run": list(range(11)),
            "grouping": ["group"] * 11,
            "value": list(range(11)),
            "value_2": list(range(11)),
        }
    ).set_index(["model_run", "grouping"])
    actual = summarise_model_runs(df)
    assert actual.index.names == ["grouping", "measure"]
    assert list(actual.index.get_level_values("measure").unique()) == [
        "value",
        "value_2",
    ]


def test_summarise_model_runs_with_multiple_indexes():
    df = pd.DataFrame(
        {
            "model_run": list(range(11)),
            "group": ["group"] * 11,
            "value": list(range(11)),
            "index_2": list(range(11)),
        }
    ).set_index(["model_run", "group", "index_2"])
    with pytest.raises(ValueError, match="Expected exactly one index column."):
        summarise_model_runs(df)


def test_get_baseline_activity():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "total": [3, 10, 100] * 3,
        }
    ).set_index("model_run")

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


def test_process_and_save_results_to_excel(mocker):
    # arrange

    mock_makedirs = mocker.patch("nhp.capacity_conversion.utils.os.makedirs")
    mocker.patch(
        "nhp.capacity_conversion.utils.os.path.join", side_effect=lambda *x: "/".join(x)
    )
    mock_wb = mocker.Mock()
    mock_ws = mocker.Mock()
    mock_cell = mocker.Mock()
    mock_cell.value = "val"
    mock_cell.column_letter = "A"
    mock_ws.columns = [(mock_cell,), (mock_cell,)]
    mock_ws.column_dimensions = {"A": mocker.Mock()}
    mocker.patch("nhp.capacity_conversion.utils.Workbook", return_value=mock_wb)
    mock_wb.active = mocker.Mock()
    mock_wb.create_sheet.return_value = mock_ws
    mocker.patch(
        "nhp.capacity_conversion.utils.dataframe_to_rows",
        return_value=[
            ["col1", "col2"],
            ["val1", "val2"],
        ],
    )
    mock_logger = mocker.patch("nhp.capacity_conversion.utils.logger")
    mock_summarise = mocker.patch("nhp.capacity_conversion.utils.summarise_model_runs")
    metadata = pd.Series(
        {
            "guid": "123",
            "capacity_conversion_runtime": "456",
        }
    )
    df = pd.DataFrame(
        {
            "model_run": list(range(11)),
            "group": ["group"] * 11,
            "value": list(range(11)),
        }
    ).set_index(["model_run", "group"])
    data_to_save = {
        "metadata": metadata,
        "results": df,
    }

    # act
    process_and_save_results_to_excel(data_to_save)

    # assert
    mock_makedirs.assert_called_once_with("results/123/456", exist_ok=True)
    mock_summarise.assert_called_once()
    mock_wb.remove.assert_called_once_with(mock_wb.active)
    assert mock_wb.create_sheet.call_count == len(data_to_save)
    mock_wb.save.assert_called_once_with(
        "results/123/456/capacity_conversion_results.xlsx"
    )
    mock_logger.info.assert_called_once()


def test_load_metadata_from_ats(mocker):
    # arrange
    guid = "GUID123"
    endpoint = "https://example.table.core.windows.net"
    table_name = "demotable"
    capacity_model_version = "dev"

    mock_credential = mocker.Mock()
    mock_table_client = mocker.Mock()

    mocker.patch(
        "nhp.capacity_conversion.utils.DefaultAzureCredential",
        return_value=mock_credential,
    )

    mocker.patch(
        "nhp.capacity_conversion.utils.TableClient",
        return_value=mock_table_client,
    )

    mock_entity = {"some_field": "some_value"}
    mock_table_client.get_entity.return_value = mock_entity

    # act
    result = load_metadata_from_ats(
        guid=guid,
        storage_endpoint=endpoint,
        table_name=table_name,
        capacity_model_version=capacity_model_version,
    )

    # assert
    mock_table_client.get_entity.assert_called_once_with(
        partition_key=capacity_model_version,
        row_key=guid,
    )

    assert result["some_field"] == "some_value"
    assert result["guid"] == guid
    assert result["capacity_model_version"] == capacity_model_version


def test_load_metadata_from_ats_not_found(mocker):
    guid = "missing-guid"
    endpoint = "https://example.table.core.windows.net"
    table_name = "demotable"
    capacity_model_version = "dev"

    mocker.patch("nhp.capacity_conversion.utils.DefaultAzureCredential")
    mock_table_client = mocker.Mock()

    mocker.patch(
        "nhp.capacity_conversion.utils.TableClient",
        return_value=mock_table_client,
    )

    mock_table_client.get_entity.side_effect = ResourceNotFoundError

    with pytest.raises(ResourceNotFoundError):
        load_metadata_from_ats(
            guid=guid,
            storage_endpoint=endpoint,
            table_name=table_name,
            capacity_model_version=capacity_model_version,
        )


def test_create_aggregations_path():
    # arrange
    metadata = {"capacity_model_version": "test", "guid": "GUID123"}

    # act
    actual = create_aggregations_path(metadata)
    expected = "functional-aggregations/test/GUID123/"

    # assert
    assert actual == expected


def test_validate_required_env_vars_success(mocker):
    # arrange
    mocker.patch("nhp.capacity_conversion.utils.load_dotenv")

    mock_env = {
        "AZ_STORAGE_EP": "endpoint",
        "AZ_STORAGE_RESULTS": "results",
        "TABLE_NAME": "table",
        "AZ_TABLE_ENDPOINT": "table_endpoint",
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
    load_aggregations("url", "container", "path", "type")

    # assert
    assert "Loading type data from path..." in caplog.text
    mock_load_parquet_file.assert_called_once_with(mock_connection, "path/type.parquet")


def test_process_activity_type_with_preprocess():
    aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b"] * 3,
            "model_run": [0] * 2 + [1] * 2 + [2] * 2,
            "total": [1, 2, 3, 4, 5, 6],
        }
    )
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
    )
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
    )
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
        guid="test-guid",
        capacity_model_version="v1",
        path_to_assumptions_file="assumptions.csv",
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
        },
    )

    metadata = {
        "PartitionKey": "pk",
        "RowKey": "rk",
        "foo": "bar",
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

    mocker.patch(
        "nhp.capacity_conversion.utils.create_aggregations_path",
        return_value="agg/path",
    )

    aggregations = pd.DataFrame({"b": [2]})
    mocker.patch(
        "nhp.capacity_conversion.utils.load_aggregations",
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
        "test-guid",
        "table-endpoint",
        "table-name",
        "v1",
    )

    process_activity.assert_called_once()

    _, kwargs = process_activity.call_args

    assert kwargs["name"] == activity_type
    assert kwargs["aggregations"] is aggregations
    assert kwargs["calculate_fn"] is calculate_fn
    assert kwargs["assumptions"] is assumptions
    assert kwargs["preprocess"] is preprocess
    assert kwargs["include_baseline"] is True

    # Metadata should have been augmented with the runtime
    data_to_save = kwargs["data_to_save"]
    assert "metadata" in data_to_save
    assert "capacity_conversion_runtime" in data_to_save["metadata"].index

    save_results.assert_called_once_with(data_to_save)
