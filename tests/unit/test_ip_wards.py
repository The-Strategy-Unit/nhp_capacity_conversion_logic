import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_wards import (
    WARD_WORKLOAD_ASSUMPTIONS_DICT,
    derive_ward_beddays,
    group_ip_wards_beddays,
    preprocess_ip_wards_data,
)


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

    functional_areas = make_functional_areas(grouping)

    mock_derive_beddays = mocker.patch(
        "nhp.capacity_conversion.ip_wards.derive_beddays_from_spells",
        return_value=pd.Series([5], index=pd.Index([1], name="model_run")),
    )

    result = derive_ward_beddays(
        grouping=grouping,
        functional_areas=functional_areas,
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

    functional_areas = make_functional_areas(grouping)

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
        functional_areas=functional_areas,
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


def test_group_ip_wards_beddays():
    index = pd.MultiIndex.from_tuples(
        [
            ("adult_nonelective_medical", 1),
            ("adult_elective_medical", 1),
            ("adult_nonelective_surgical", 1),
            ("adult_elective_surgical", 1),
            ("paediatric_nonelective_medical", 1),
            ("paediatric_elective_medical", 1),
            ("paediatric_nonelective_surgical", 1),
            ("paediatric_elective_surgical", 1),
            ("unrelated_group", 1),
            ("adult_nonelective_medical", 2),
            ("adult_elective_medical", 2),
            ("adult_nonelective_surgical", 2),
            ("adult_elective_surgical", 2),
            ("paediatric_nonelective_medical", 2),
            ("paediatric_elective_medical", 2),
            ("paediatric_nonelective_surgical", 2),
            ("paediatric_elective_surgical", 2),
        ],
        names=["grouping", "model_run"],
    )

    df = pd.DataFrame(
        {
            "ward_beddays": [
                100,
                200,
                300,
                400,
                50,
                60,
                70,
                80,
                999,
                10,
                20,
                30,
                40,
                0,
                0,
                0,
                0,
            ],
            "critical_care_beddays": [
                10,
                20,
                30,
                40,
                5,
                6,
                7,
                8,
                999,
                1,
                2,
                3,
                4,
                0,
                0,
                0,
                0,
            ],
            "assessment_beddays": [
                11,
                21,
                31,
                41,
                15,
                16,
                17,
                18,
                999,
                2,
                3,
                4,
                5,
                0,
                0,
                0,
                0,
            ],
        },
        index=index,
    )

    result = group_ip_wards_beddays(df)

    expected = pd.DataFrame(
        {
            "total": [
                104,
                100,
                600,
                400,
                66,
                26,
                260,
                14,
                10,
                60,
                40,
                0,
                0,
                0,
            ]
        },
        index=pd.MultiIndex.from_tuples(
            [
                (1, "adult_assessment_beddays"),
                (1, "adult_critical_care_beddays"),
                (1, "adult_elective_wards_beddays"),
                (1, "adult_nonelective_wards_beddays"),
                (1, "paediatric_assessment_beddays"),
                (1, "paediatric_critical_care_beddays"),
                (1, "paediatric_wards_beddays"),
                (2, "adult_assessment_beddays"),
                (2, "adult_critical_care_beddays"),
                (2, "adult_elective_wards_beddays"),
                (2, "adult_nonelective_wards_beddays"),
                (2, "paediatric_assessment_beddays"),
                (2, "paediatric_critical_care_beddays"),
                (2, "paediatric_wards_beddays"),
            ],
            names=["model_run", "grouping"],
        ),
    )
    assert_frame_equal(result, expected)


def test_preprocess_ip_wards_data(mocker):
    functional_areas = mocker.Mock()
    assumptions_df = mocker.Mock()

    derived_beddays = {
        "ward_beddays": pd.Series(
            [100.0, 200.0], index=pd.Index([1, 2], name="model_run")
        ),
        "critical_care_beddays": pd.Series(
            [10.0, 20.0], index=pd.Index([1, 2], name="model_run")
        ),
        "assessment_beddays": pd.Series(
            [5.0, 6.0], index=pd.Index([1, 2], name="model_run")
        ),
    }

    mock_derive_ward_beddays = mocker.patch(
        "nhp.capacity_conversion.ip_wards.derive_ward_beddays",
        return_value=derived_beddays,
    )

    expected_result = pd.DataFrame(
        {"total": [321.0]},
        index=pd.MultiIndex.from_tuples(
            [(1, "grouping")],
            names=["model_run", "grouping"],
        ),
    )

    mock_group_ip_wards_beddays = mocker.patch(
        "nhp.capacity_conversion.ip_wards.group_ip_wards_beddays",
        return_value=expected_result,
    )

    result = preprocess_ip_wards_data(
        functional_areas=functional_areas,
        assumptions_df=assumptions_df,
    )

    # Check the final result is returned unchanged.
    assert_frame_equal(result, expected_result)

    # derive_ward_beddays should be called once for every configured grouping.
    assert mock_derive_ward_beddays.call_count == len(WARD_WORKLOAD_ASSUMPTIONS_DICT)

    # Check each grouping receives the correct assumptions.
    for grouping, assumptions_dict in WARD_WORKLOAD_ASSUMPTIONS_DICT.items():
        mock_derive_ward_beddays.assert_any_call(
            grouping,
            functional_areas,
            assumptions_df,
            assumptions_dict,
        )

    # group_ip_wards_beddays should be called once with the combined result.
    mock_group_ip_wards_beddays.assert_called_once()

    grouped_input = mock_group_ip_wards_beddays.call_args.args[0]

    expected_input = pd.concat(
        [
            pd.DataFrame(derived_beddays)
            .assign(grouping=grouping)
            .set_index("grouping", append=True)
            for grouping in WARD_WORKLOAD_ASSUMPTIONS_DICT
        ]
    )

    assert_frame_equal(
        grouped_input,
        expected_input,
    )
