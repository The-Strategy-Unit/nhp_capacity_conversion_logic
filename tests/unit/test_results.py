import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.results import (
    process_and_save_results_to_excel,
    summarise_model_runs,
    tidy_metadata,
)


def test_process_and_save_results_to_excel(mocker):
    # arrange

    mock_makedirs = mocker.patch("nhp.capacity_conversion.results.os.makedirs")
    mocker.patch(
        "nhp.capacity_conversion.results.os.path.join",
        side_effect=lambda *x: "/".join(x),
    )
    mock_wb = mocker.Mock()
    mock_ws = mocker.Mock()
    mock_cell = mocker.Mock()
    mock_cell.value = "val"
    mock_cell.column_letter = "A"
    mock_ws.columns = [(mock_cell,), (mock_cell,)]
    mock_ws.column_dimensions = {"A": mocker.Mock()}

    mocker.patch("nhp.capacity_conversion.results.Workbook", return_value=mock_wb)
    mock_wb.active = mocker.Mock()
    mock_wb.create_sheet.return_value = mock_ws

    mock_dataframe_to_rows = mocker.patch(
        "nhp.capacity_conversion.results.dataframe_to_rows",
        side_effect=[
            [
                ["guid", "123"],
                ["capacity_conversion_runtime", "456"],
            ],
            [
                ["col1", "col2"],
                ["val1", "val2"],
            ],
        ],
    )
    mock_logger = mocker.patch("nhp.capacity_conversion.results.logger")
    mock_summarise = mocker.patch(
        "nhp.capacity_conversion.results.summarise_model_runs"
    )
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
    assert mock_dataframe_to_rows.call_count == 2

    # metadata is a Series, so it should be written without headers
    metadata_call = mock_dataframe_to_rows.call_args_list[0]
    assert metadata_call.kwargs["index"] is False
    assert metadata_call.kwargs["header"] is False

    # results is a DataFrame, so it should include headers
    results_call = mock_dataframe_to_rows.call_args_list[1]
    assert results_call.kwargs["index"] is False
    assert results_call.kwargs["header"] is True

    mock_wb.save.assert_called_once_with(
        "results/123/456/capacity_conversion_results.xlsx"
    )
    mock_logger.info.assert_called_once()


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


def test_tidy_metadata():
    metadata = pd.Series(
        {
            "dataset": "dataset",
            "capacity_model_version": "1.2.3",
            "ip_sites": "A,B",
            "op_sites": "ALL",
            "aae_sites": "ALL",
            "capacity_conversion_runtime": "capacity_conversion_runtime",
            "app_version": "4.5.6",
            "scenario_name": "scenario_name",
            "scenario_runtime": "scenario_runtime",
            "unwanted_metadata": "remove me",
        }
    )
    results = pd.DataFrame({"value": [1, 2, 3]})

    data_to_save = {"metadata": metadata, "results": results}

    result = tidy_metadata(data_to_save)

    expected = pd.Series(
        {
            "dataset": "dataset",
            "capacity_model_version": "1.2.3",
            "ip_sites": "A,B",
            "op_sites": "ALL",
            "aae_sites": "ALL",
            "capacity_conversion_runtime": "capacity_conversion_runtime",
            "demand_model_version": "4.5.6",
            "demand_model_scenario_name": "scenario_name",
            "demand_model_scenario_runtime": "scenario_runtime",
        }
    )

    # Test that metadata has changed
    assert_series_equal(result["metadata"], expected)  # ty:ignore invalid-argument-type
    # Test that results remain unchanged
    assert_frame_equal(result["results"], results)  # ty:ignore invalid-argument-type
