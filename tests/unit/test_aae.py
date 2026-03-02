import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.aae import map_unknown, load_aae_aggregations


def test_load_aae_aggregations(mocker, caplog):
    # Arrange
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

    # Act
    load_aae_aggregations("url", "container", "path")

    # Assert
    assert "Loading A&E data from path..." in caplog.text


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
