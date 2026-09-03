import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from nhp.capacity_conversion.results import (
    process_and_save_results_to_excel,
    summarise_model_runs,
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
    mocker.patch(
        "nhp.capacity_conversion.results.dataframe_to_rows",
        return_value=[
            ["col1", "col2"],
            ["val1", "val2"],
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
