import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_maternity import (
    calculate_maternity_capacity,
    calculate_maternity_ward_beds,
    derive_birth_related_ward_beddays,
    derive_total_maternity_ward_beddays,
    main,
    preprocess_ip_maternity_data,
    process_maternity_birth_data,
    process_theatres_obstetric_proc_data,
    ward_assumptions_dict,
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


def test_derive_total_maternity_ward_beddays(mocker):
    functional_areas_processed = pd.DataFrame(
        {
            "model_run": [1],
            "grouping": ["maternity_overnight_no_birth"],
            "beddays": [1],
        }
    ).set_index(["model_run", "grouping"])
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_birth_related_ward_beddays",
        return_value=pd.Series([1], index=pd.Index([1], name="model_run")),
    )
    expected = pd.Series([5.0], index=pd.Index([1], name="model_run"))
    actual = derive_total_maternity_ward_beddays(
        functional_areas_processed,
        assumptions_df=pd.DataFrame(),
        assumptions_dict={
            grouping: {"assumption": "assumption_name"}
            for grouping in [
                "maternity_normal_delivery",
                "maternity_assisted_delivery",
                "maternity_elective_csection",
                "maternity_nonelective_csection",
            ]
        },
    )
    assert mock_derive.call_count == 4
    assert_series_equal(actual, expected)


def test_calculate_maternity_ward_beds(mocker):
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_total_maternity_ward_beddays",
        return_value="total_maternity_ward_beddays",
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_beds",
        return_value=pd.Series([1.0], index=pd.Index([1], name="model_run")),
    )
    functional_areas_processed = pd.DataFrame()
    assumptions_df = pd.DataFrame(
        {"Value": ["MATERNITY_WARD_OCC", "MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS"]},
        index=["MATERNITY_WARD_OCC", "MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS"],
    )
    expected = pd.DataFrame(
        {"output": ["MATERNITY_WARD_BEDS"], "model_run": [1], "total": [1.0]}
    ).set_index(["output", "model_run"])
    actual = calculate_maternity_ward_beds(
        functional_areas_processed, assumptions_df, ward_assumptions_dict
    )
    mock_derive.assert_called_once_with(
        functional_areas_processed, assumptions_df, ward_assumptions_dict
    )
    mock_calculate.assert_called_once_with(
        "total_maternity_ward_beddays",
        "MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS",
        "MATERNITY_WARD_OCC",
    )
    assert_frame_equal(actual, expected)


def test_process_theatres_obstetric_proc_data():
    functional_areas = pd.DataFrame(
        {
            "grouping": [
                "maternity_elective_csection_nonzerolos",
                "maternity_nonelective_csection_nonzerolos",
                "maternity_elective_csection_zerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_group",
            ],
            "beddays": [1] * 5,
            "spells": [1] * 5,
        },
        index=pd.Index([1] * 5, name="model_run"),
    )
    expected = pd.DataFrame(
        {
            "grouping": [
                "maternity_elective_csection_nonzerolos",
                "maternity_nonelective_csection_nonzerolos",
                "maternity_elective_csection_zerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_group",
                "obstetric_theatre_procedures",
            ],
            "beddays": [1] * 5 + [4],
            "spells": [1] * 5 + [4],
        },
        index=pd.Index([1] * 6, name="model_run"),
    )
    actual = process_theatres_obstetric_proc_data(functional_areas)
    assert_frame_equal(actual, expected)


def test_process_maternity_birth_data():
    functional_areas = pd.DataFrame(
        {
            "grouping": [
                "maternity_group",
                "maternity_normal_delivery_zerolos",
                "maternity_normal_delivery_nonzerolos",
                "maternity_assisted_delivery_zerolos",
                "maternity_assisted_delivery_nonzerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_nonelective_csection_nonzerolos",
            ],
            "beddays": [0, 1, 1, 2, 2, 3, 3],
            "spells": [0, 1, 1, 2, 2, 3, 3],
        },
        index=pd.Index([1] * 7, name="model_run"),
    )
    expected = pd.DataFrame(
        {
            "grouping": [
                "maternity_group",
                "maternity_normal_delivery_zerolos",
                "maternity_normal_delivery_nonzerolos",
                "maternity_assisted_delivery_zerolos",
                "maternity_assisted_delivery_nonzerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_nonelective_csection_nonzerolos",
                "maternity_normal_delivery",
                "maternity_assisted_delivery",
                "maternity_nonelective_csection",
            ],
            "beddays": [0, 1, 1, 2, 2, 3, 3, 2, 4, 6],
            "spells": [0, 1, 1, 2, 2, 3, 3, 2, 4, 6],
        },
        index=pd.Index([1] * 10, name="model_run"),
    )
    actual = process_maternity_birth_data(functional_areas)
    assert_frame_equal(actual, expected)


def test_preprocess_ip_maternity_data(mocker):
    mock_process_theaters = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.process_theatres_obstetric_proc_data",
        return_value="processed_fun_areas",
    )
    mock_process_birth = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.process_maternity_birth_data",
        return_value="processed_fun_areas",
    )
    fun_areas = pd.DataFrame()
    preprocess_ip_maternity_data(fun_areas)
    mock_process_theaters.assert_called_once_with(fun_areas)
    mock_process_birth.assert_called_once_with("processed_fun_areas")


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
