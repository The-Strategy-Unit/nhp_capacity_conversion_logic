import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from azure.core.exceptions import ResourceNotFoundError
from nhp.capacity_conversion.utils import (
    calculate_prediction_intervals_and_mean,
    load_assumptions,
    save_results_to_excel,
    load_metadata_from_ats,
    create_aggregations_path,
    validate_required_env_vars,
)


def test_calculate_prediction_intervals_and_mean():
    # arrange
    test_activity = pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    expected = {"mean": 5.0, "p10": 1.0, "p90": 9.0}

    # act
    actual = calculate_prediction_intervals_and_mean(test_activity)

    # assert
    assert actual == expected


def test_load_assumptions(mocker):
    # arrange
    mock_read_csv = mocker.patch(
        "pandas.read_csv", return_value=pd.DataFrame({"assumption_name": []})
    )
    expected = pd.DataFrame({"assumption_name": []}).set_index("assumption_name")
    # act
    actual = load_assumptions("test_path")

    # assert
    assert_frame_equal(expected, actual)
    mock_read_csv.assert_called_once_with("test_path")


def test_save_results_to_excel(mocker):
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
    metadata = pd.Series(
        {
            "guid": "123",
            "capacity_conversion_runtime": "456",
        }
    )
    df = pd.DataFrame({"col1": ["val1"], "col2": ["val2"]})
    data_to_save = {
        "metadata": metadata,
        "results": df,
    }

    # act
    save_results_to_excel(data_to_save)

    # assert
    mock_makedirs.assert_called_once_with("results/123/456", exist_ok=True)
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
