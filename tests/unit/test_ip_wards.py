import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.ip_wards import derive_ward_beddays


@pytest.fixture
def assumptions_dict():
    return {
        "zero_day_los": "zero_day_los",
        "critical_care_percentage": "critical_care_percentage",
        "assessment_los": "assessment_los",
    }


@pytest.fixture
def assumptions_df():
    return pd.DataFrame(
        {
            "Value": {
                "zero_day_los": 0.5,
                "critical_care_percentage": 0.2,
                "assessment_los": 1.0,
            }
        }
    )


def make_functional_areas(grouping: str) -> pd.DataFrame:
    """Create the minimum dataframe needed by derive_ward_beddays."""
    index = pd.MultiIndex.from_tuples(
        [
            (f"{grouping}_zerolos", 1),
            (f"{grouping}_nonzerolos", 1),
        ],
        names=["grouping", "model_run"],
    )

    return pd.DataFrame(
        {
            "spells": [10, 20],
            "beddays": [0, 100],
        },
        index=index,
    )


def test_derive_ward_beddays_elective(
    mocker,
    assumptions_df,
    assumptions_dict,
):
    grouping = "adult_elective_medical"

    functional_areas_processed = make_functional_areas(grouping)

    mock_derive_beddays = mocker.patch(
        "nhp.capacity_conversion.ip_wards.derive_beddays_from_spells",
        return_value=pd.Series([5], index=pd.Index([1], name="model_run")),
    )

    result = derive_ward_beddays(
        grouping=grouping,
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions_dict=assumptions_dict,
    )

    expected_critical_care = pd.Series(
        [20.0], index=pd.Index([1], name="model_run"), name="beddays"
    )
    expected_assessment = pd.Series([0], index=pd.Index([1], name="model_run"))
    expected_ward = pd.Series([85.0], index=pd.Index([1], name="model_run"))

    assert_series_equal(
        result["critical_care_beddays"],
        expected_critical_care,
    )
    assert_series_equal(
        result["assessment_beddays"],
        expected_assessment,
    )
    assert_series_equal(
        result["ward_beddays"],
        expected_ward,
    )

    # Only zero-day spells should be passed to derive_beddays_from_spells.
    mock_derive_beddays.assert_called_once()

    spells_arg, los_arg = mock_derive_beddays.call_args.args

    assert_series_equal(
        spells_arg,
        pd.Series([10], index=pd.Index([1], name="model_run"), name="spells"),
    )
    assert los_arg == 0.5


def test_derive_ward_beddays_nonelective(
    mocker,
    assumptions_df,
    assumptions_dict,
):
    grouping = "adult_nonelective_medical"

    functional_areas_processed = make_functional_areas(grouping)

    # First call = zero-day beddays.
    # Second call = assessment beddays.
    mock_derive_beddays = mocker.patch(
        "nhp.capacity_conversion.ip_wards.derive_beddays_from_spells",
        side_effect=[
            pd.Series([5.0], pd.Index([1], name="model_run")),
            pd.Series([10.0], pd.Index([1], name="model_run")),
        ],
    )

    result = derive_ward_beddays(
        grouping=grouping,
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions_dict=assumptions_dict,
    )

    # nonzero beddays = 100
    # zero-day beddays = 5
    # critical care = 20% * 100 = 20
    # assessment = 10
    # ward = 100 + 5 - 10 - 20 = 75
    expected_ward = pd.Series([75.0], pd.Index([1], name="model_run"))
    expected_critical_care = pd.Series(
        [20.0], pd.Index([1], name="model_run"), name="beddays"
    )
    expected_assessment = pd.Series([10.0], pd.Index([1], name="model_run"))

    assert_series_equal(
        result["ward_beddays"],
        expected_ward,
    )
    assert_series_equal(
        result["critical_care_beddays"],
        expected_critical_care,
    )
    assert_series_equal(
        result["assessment_beddays"],
        expected_assessment,
    )

    assert mock_derive_beddays.call_count == 2

    # First call: zero-day spells and zero-day LOS
    zero_day_spells, zero_day_los = mock_derive_beddays.call_args_list[0].args

    assert_series_equal(
        zero_day_spells,
        pd.Series([10], pd.Index([1], name="model_run"), name="spells"),
    )
    assert zero_day_los == 0.5

    # Second call: nonzero + zero-day spells and assessment LOS
    assessment_spells, assessment_los = mock_derive_beddays.call_args_list[1].args

    assert_series_equal(
        assessment_spells,
        pd.Series([30], pd.Index([1], name="model_run"), name="spells"),
    )
    assert assessment_los == 1.0
