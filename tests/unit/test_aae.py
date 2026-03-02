import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.aae import (
    map_unknown,
    load_aae_aggregations,
    process_aae,
    convert_aae_capacity,
)


def test_load_aae_aggregations(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mock_connection = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.aae.connect_to_container",
        return_value=mock_connection,
    )
    mocker.patch(
        "nhp.capacity_conversion.aae.load_parquet_file",
        return_value=pd.DataFrame({"col": [1]}),
    )

    # act
    load_aae_aggregations("url", "container", "path")

    # assert
    assert "Loading A&E data from path..." in caplog.text


def test_process_aae(mocker):
    # arrange
    aae_aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "arrivals": [3] * 3 + [4] * 3 + [5] * 3,
        }
    ).set_index("model_run")
    mocker.patch(
        "nhp.capacity_conversion.aae.map_unknown",
        return_value=aae_aggregations["grouping"],
    )

    expected = {
        "a": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
        "b": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
        "c": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
    }

    # act
    actual = process_aae(aae_aggregations)

    # assert
    assert actual == expected


def test_map_unknown():
    # arrange
    col = pd.Series(
        ["adult_unknown", "adult_minor_attendances", "sdec", "child_unknown"]
    )
    # act
    col_mapped = map_unknown(col)
    # assert
    assert_series_equal(
        col_mapped,
        pd.Series(
            [
                "adult_minor_attendances",
                "adult_minor_attendances",
                "sdec",
                "child_minor_attendances",
            ]
        ),
    )


def test_convert_aae_capacity():
    # arrange
    attendances = 10000
    assumed_los_mins = 240
    operating_weeks_per_year = 52
    operating_hours_per_week = 168
    utilisation_rate = 0.5

    expected = 40000 / 4368

    # act
    actual = convert_aae_capacity(
        attendances,
        assumed_los_mins,
        operating_weeks_per_year,
        operating_hours_per_week,
        utilisation_rate,
    )
    # assert
    assert actual == expected
