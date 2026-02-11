import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.aae import map_unknown


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
