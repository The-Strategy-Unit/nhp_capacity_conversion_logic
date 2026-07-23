import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.ip_maternity import (
    calculate_maternity_capacity,
    derive_birth_related_ward_beddays,
    main,
    preprocess_ip_maternity_data,
)


def test_derive_birth_related_ward_beddays(mocker):
    # Arrange
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells"
    )

    # First call = zero-day beddays, second call = birth room beddays
    mock_derive.side_effect = [
        pd.Series([10, 20], index=[1, 2]),
        pd.Series([3, 4], index=[1, 2]),
    ]

    functional_areas_processed = pd.DataFrame(
        {
            "grouping": [
                "maternity_zerolos",
                "maternity_zerolos",
                "maternity",
                "maternity",
                "maternity_nonzerolos",
                "maternity_nonzerolos",
            ],
            "model_run": [
                1,
                2,
                1,
                2,
                1,
                2,
            ],
            "spells": [100, 200, 50, 60, 0, 0],
            "beddays": [0, 0, 30, 40, 30, 40],
        }
    ).set_index(["grouping", "model_run"])

    assumptions_df = pd.DataFrame(
        {"Value": ["zero_day_los", "birthroom_los"]},
        index=["zero_day_los", "birthroom_los"],
    )

    assumptions = {
        "zero_day_los": "zero_day_los",
        "birthroom_los": "birthroom_los",
    }

    # Act
    actual = derive_birth_related_ward_beddays(
        grouping="maternity",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )
    # Assert
    expected = pd.Series([37, 56], index=pd.Index([1, 2], name="model_run"))
    assert_series_equal(actual, expected)

    assert mock_derive.call_count == 2


def test_derive_birth_related_ward_beddays_elective_csection(mocker):
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells",
        return_value=pd.Series([5], index=[1]),
    )

    functional_areas_processed = pd.DataFrame(
        {
            "grouping": [
                "maternity_elective_csection_nonzerolos",
                "maternity_elective_csection_zerolos",
            ],
            "model_run": [1, 1],
            "spells": [1, 2],
            "beddays": [10, 0],
        }
    ).set_index(["grouping", "model_run"])
    assumptions_df = pd.DataFrame(
        {"Value": ["zero_day_los", "birthroom_los"]},
        index=["zero_day_los", "birthroom_los"],
    )

    assumptions = {
        "zero_day_los": "zero_day_los",
        "birthroom_los": "birthroom_los",
    }
    actual = derive_birth_related_ward_beddays(
        grouping="maternity_elective_csection",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )
    expected = pd.Series([15], index=pd.Index([1], name="model_run"))

    # Only the zero-day calculation should be performed
    mock_derive.assert_called_once()
    assert_series_equal(actual, expected)


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with(
        "ip_maternity",
        calculate_maternity_capacity,
        preprocess=preprocess_ip_maternity_data,
    )
