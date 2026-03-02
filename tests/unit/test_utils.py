import pandas as pd
from pandas.testing import assert_frame_equal
from nhp.capacity_conversion.utils import (
    calculate_prediction_intervals_and_mean,
    load_assumptions,
    save_results_to_csv,
)
import os


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


def test_save_results_to_csv(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mock_makedirs = mocker.patch("os.makedirs")
    mock_to_csv = mocker.patch("pandas.DataFrame.to_csv")
    data = pd.DataFrame()
    directory = os.path.join("results", "guid", "runtime")
    csv = os.path.join(directory, "activity_type.csv")

    # act
    save_results_to_csv(data, "guid", "runtime", "activity_type")

    # assert
    mock_makedirs.assert_called_once_with(directory, exist_ok=True)
    mock_to_csv.assert_called_once_with(csv)
    assert f"💾 Results saved to {csv}" in caplog.text
