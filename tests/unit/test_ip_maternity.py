import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_maternity import (
    MaternityConfig,
    calculate_maternity_assessment_beds,
    calculate_maternity_birth_rooms,
    calculate_maternity_capacity,
    calculate_maternity_ward_beds,
    calculate_theatres_obstetric_proc,
    derive_birth_related_ward_beddays,
    derive_total_maternity_ward_beddays,
    main,
    maternity_ward_assumptions_dict,
    preprocess_ip_maternity_data,
    process_maternity_birth_data,
    process_theatres_obstetric_proc_data,
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
            "functional_area": [
                "maternity_zerolos",
                "maternity_zerolos",
                "maternity",
                "maternity",
                "maternity_nonzerolos",
                "maternity_nonzerolos",
            ]
            * 2,
            "model_run": [1, 2] * 6,
            "value": [100, 200, 50, 60, 0, 0, 0, 0, 30, 40, 30, 40],
            "measure": ["count"] * 6 + ["duration_days"] * 6,
        }
    ).set_index(["functional_area", "model_run", "measure"])

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
        functional_area="maternity",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )
    # Assert
    expected = pd.Series([37, 56], index=pd.Index([1, 2], name="model_run"))
    assert_series_equal(actual, expected)  # ty: ignore

    assert mock_derive.call_count == 2


def test_derive_birth_related_ward_beddays_elective_csection(mocker):
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells",
        return_value=pd.Series([5], index=[1]),
    )

    functional_areas_processed = pd.DataFrame(
        {
            "functional_area": [
                "maternity_elective_csection_nonzerolos",
                "maternity_elective_csection_zerolos",
            ]
            * 2,
            "model_run": [1] * 4,
            "value": [1, 2, 10, 0],
            "measure": ["count", "count", "duration_days", "duration_days"],
        }
    ).set_index(["functional_area", "model_run", "measure"])
    assumptions_df = pd.DataFrame(
        {"Value": ["zero_day_los", "birthroom_los"]},
        index=["zero_day_los", "birthroom_los"],
    )

    assumptions = {
        "zero_day_los": "zero_day_los",
        "birthroom_los": "birthroom_los",
    }
    actual = derive_birth_related_ward_beddays(
        functional_area="maternity_elective_csection",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )
    expected = pd.Series([15], index=pd.Index([1], name="model_run"))

    # Only the zero-day calculation should be performed
    mock_derive.assert_called_once()
    assert_series_equal(actual, expected)  # ty: ignore


def test_derive_birth_related_ward_beddays_no_zero_day_los(mocker):
    """Should return 0 for zero-day beddays when *_zerolos is absent."""
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells",
        return_value=pd.Series([3, 4], index=pd.Index([1, 2], name="model_run")),
    )

    functional_areas_processed = pd.DataFrame(
        {
            "functional_area": [
                "maternity",
                "maternity",
                "maternity_nonzerolos",
                "maternity_nonzerolos",
            ],
            "model_run": [1, 2, 1, 2],
            "value": [50, 60, 30, 40],
            "measure": ["count", "count", "duration_days", "duration_days"],
        }
    ).set_index(["functional_area", "model_run", "measure"])

    assumptions_df = pd.DataFrame(
        {"Value": [0.5, 1.0]},
        index=["zero_day_los", "birthroom_los"],
    )

    assumptions = {
        "zero_day_los": "zero_day_los",
        "birthroom_los": "birthroom_los",
    }

    actual = derive_birth_related_ward_beddays(
        functional_area="maternity",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )

    # No zero-day calculation; only birth-room calculation
    mock_derive.assert_called_once()

    # 30 - 3, 40 - 4 (birth_spell_overnight_beddays - birth_room_beddays)
    expected = pd.Series([27, 36], index=pd.Index([1, 2], name="model_run"))
    assert_series_equal(actual, expected)  # ty: ignore


def test_derive_birth_related_ward_beddays_no_nonzero_day_los(mocker):
    """Should return 0 for overnight beddays when *_nonzerolos is absent."""
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells",
        side_effect=[
            pd.Series(
                [10, 20], index=pd.Index([1, 2], name="model_run")
            ),  # zero_day_beddays
            pd.Series(
                [3, 4], index=pd.Index([1, 2], name="model_run")
            ),  # birth_room_beddays
        ],
    )

    functional_areas_processed = pd.DataFrame(
        {
            "functional_area": [
                "maternity",
                "maternity",
                "maternity_zerolos",
                "maternity_zerolos",
            ],
            "model_run": [1, 2, 1, 2],
            "value": [50, 60, 30, 40],
            "measure": ["count", "count", "duration_days", "duration_days"],
        }
    ).set_index(["functional_area", "model_run", "measure"])

    assumptions_df = pd.DataFrame(
        {"Value": [0.5, 1.0]},
        index=["zero_day_los", "birthroom_los"],
    )

    assumptions = {
        "zero_day_los": "zero_day_los",
        "birthroom_los": "birthroom_los",
    }

    actual = derive_birth_related_ward_beddays(
        functional_area="maternity",
        functional_areas_processed=functional_areas_processed,
        assumptions_df=assumptions_df,
        assumptions=assumptions,
    )

    # Zero-day and birth room calculations
    assert mock_derive.call_count == 2

    # birth_spell_overnight_beddays = 0
    # 10 - 3, 20 - 4 (zero_day_beddays - birth_room_beddays)
    expected = pd.Series([7, 16], index=pd.Index([1, 2], name="model_run"))
    assert_series_equal(actual, expected)  # ty: ignore


def test_derive_total_maternity_ward_beddays(mocker):
    functional_areas_processed = pd.DataFrame(
        {
            "model_run": [1],
            "functional_area": ["maternity_overnight_no_birth"],
            "measure": ["duration_days"],
            "value": [1],
        }
    ).set_index(["model_run", "functional_area", "measure"])
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


def test_derive_total_maternity_ward_beddays_no_maternity_overnight_no_birth(mocker):
    functional_areas_processed = pd.DataFrame(
        {
            "model_run": [1],
            "functional_area": ["maternity"],
            "measure": ["duration_days"],
            "value": [1],
        }
    ).set_index(["model_run", "functional_area", "measure"])
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_birth_related_ward_beddays",
        return_value=pd.Series([1], index=pd.Index([1], name="model_run")),
    )
    expected = pd.Series([4.0], index=pd.Index([1], name="model_run"))
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
        return_value=pd.Series(
            [1.0], index=pd.Index([1], name="model_run"), name="value"
        ),
    )
    functional_areas_processed = pd.DataFrame()
    assumptions_df = pd.DataFrame(
        {"Value": ["MATERNITY_WARD_OCC", "MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS"]},
        index=["MATERNITY_WARD_OCC", "MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS"],
    )
    expected = pd.DataFrame(
        {"output": ["MATERNITY_WARD_BEDS"], "model_run": [1], "value": [1.0]}
    ).set_index(["output", "model_run"])
    actual = calculate_maternity_ward_beds(
        functional_areas_processed, assumptions_df, maternity_ward_assumptions_dict
    )
    mock_derive.assert_called_once_with(
        functional_areas_processed, assumptions_df, maternity_ward_assumptions_dict
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
            "functional_area": [
                "maternity_elective_csection_nonzerolos",
                "maternity_nonelective_csection_nonzerolos",
                "maternity_elective_csection_zerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_group",
            ]
            * 2,
            "value": [1] * 10,
            "measure": ["duration_days"] * 5 + ["count"] * 5,
            "model_run": [1] * 10,
        }
    ).set_index(["functional_area", "model_run", "measure"])
    expected = (
        pd.DataFrame(
            {
                "functional_area": [
                    "maternity_elective_csection_nonzerolos",
                    "maternity_nonelective_csection_nonzerolos",
                    "maternity_elective_csection_zerolos",
                    "maternity_nonelective_csection_zerolos",
                    "maternity_group",
                    "obstetric_theatre_procedures",
                ]
                * 2,
                "value": [1, 1, 1, 1, 1, 4] * 2,
                "measure": ["duration_days"] * 6 + ["count"] * 6,
                "model_run": [1] * 12,
            }
        )
        .set_index(
            [
                "model_run",
                "measure",
                "functional_area",
            ]
        )
        .sort_index()
    )
    actual = process_theatres_obstetric_proc_data(functional_areas)
    assert_frame_equal(actual, expected)


def test_process_maternity_birth_data():
    functional_areas = pd.DataFrame(
        {
            "functional_area": [
                "maternity_group",
                "maternity_normal_delivery_zerolos",
                "maternity_normal_delivery_nonzerolos",
                "maternity_assisted_delivery_zerolos",
                "maternity_assisted_delivery_nonzerolos",
                "maternity_nonelective_csection_zerolos",
                "maternity_nonelective_csection_nonzerolos",
            ]
            * 2,
            "measure": ["duration_days"] * 7 + ["count"] * 7,
            "model_run": [1] * 14,
            "value": [0, 1, 1, 2, 2, 3, 3] * 2,
        }
    ).set_index(["functional_area", "model_run", "measure"])
    expected = (
        pd.DataFrame(
            {
                "functional_area": [
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
                ]
                * 2,
                "measure": ["duration_days"] * 10 + ["count"] * 10,
                "value": [0, 1, 1, 2, 2, 3, 3, 2, 4, 6] * 2,
                "model_run": [1] * 20,
            }
        )
        .set_index(
            [
                "model_run",
                "measure",
                "functional_area",
            ]
        )
        .sort_index()
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


def test_calculate_maternity_birth_rooms(mocker):
    assumptions = {
        "birthroom_los": "birthroom_los",
        "birthroom_occupancy": "birthroom_occupancy",
        "birthroom_operational_days": "birthroom_operational_days",
        "output": "output",
    }
    assumptions_df = pd.DataFrame.from_dict(
        assumptions, orient="index", columns=["Value"]
    )
    functional_area_subgroup = pd.Series()
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_beddays_from_spells",
        return_value="birthroom_beddays",
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_beds",
        return_value=pd.Series([1.0], index=pd.Index([1], name="model_run")),
    )
    actual = calculate_maternity_birth_rooms(
        assumptions, functional_area_subgroup, assumptions_df
    )
    assert actual.loc[("output", 1), 0] == 1.0
    mock_derive.assert_called_once_with(functional_area_subgroup, "birthroom_los")
    mock_calculate.assert_called_once_with(
        "birthroom_beddays", "birthroom_operational_days", "birthroom_occupancy"
    )


def test_calculate_theatres_obstetric_proc(mocker):
    assumptions = {
        "procedure_time": "procedure_time",
        "theatre_utilisation": "theatre_utilisation",
        "theatre_annual_operational_hours": "theatre_annual_operational_hours",
    }
    assumptions_df = pd.DataFrame.from_dict(
        assumptions, orient="index", columns=["Value"]
    )
    functional_area_subgroup = pd.Series()
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_treatment_hours",
        return_value="treatment_hours",
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_time_util_capacity",
        return_value=pd.Series([1.0], index=pd.Index([1], name="model_run")),
    )
    actual = calculate_theatres_obstetric_proc(
        assumptions, functional_area_subgroup, assumptions_df
    )
    assert actual.loc[("OBSTETRIC_PROC_THEATRES", 1), 0] == 1.0
    mock_derive.assert_called_once_with("procedure_time", functional_area_subgroup)
    mock_calculate.assert_called_once_with(
        "treatment_hours", "theatre_annual_operational_hours", "theatre_utilisation"
    )


def test_calculate_maternity_assessment_beds(mocker):
    assumptions = {
        "recovery_time": "recovery_time",
        "recovery_occupancy": "recovery_occupancy",
        "recovery_annual_operational_hours": "recovery_annual_operational_hours",
    }
    assumptions_df = pd.DataFrame.from_dict(
        assumptions, orient="index", columns=["Value"]
    )
    functional_area_subgroup = pd.Series()
    mock_derive = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.derive_recovery_occupancy_hours",
        return_value="occupancy_hours",
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_recovery_capacity",
        return_value=pd.Series([1.0], index=pd.Index([1], name="model_run")),
    )
    actual = calculate_maternity_assessment_beds(
        assumptions, functional_area_subgroup, assumptions_df
    )
    assert actual.loc[("MATERNITY_ASSESSMENT_BEDS", 1), 0] == 1.0
    mock_derive.assert_called_once_with(functional_area_subgroup, "recovery_time")
    mock_calculate.assert_called_once_with(
        "occupancy_hours", "recovery_annual_operational_hours", "recovery_occupancy"
    )


def test_calculate_maternity_capacity(mocker):
    def mock_formula(
        assumptions,
        functional_area_subgroup,
        assumptions_df,
    ):
        return pd.DataFrame(
            {"output": ["output"], "model_run": [1], "value": [1]}
        ).set_index(["model_run", "output"])

    fake_config = {
        "output": MaternityConfig(
            subgroup="subgroup",
            measure="count",
            formula=mock_formula,
            assumptions={"assumption": "assumption"},
        )
    }

    functional_areas = pd.DataFrame(
        {
            "model_run": [1, 1],
            "functional_area": ["subgroup", "subgroup"],
            "measure": ["count", "duration_days"],
            "value": [1, 2],
        }
    ).set_index(["model_run", "functional_area", "measure"])

    assumptions_df = pd.DataFrame({"Value": {"some": 10}})
    mock_calculate_ward_beds = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_maternity_ward_beds",
        return_value=pd.DataFrame(
            {"output": ["ward_beds"], "model_run": [1], "value": [1]}
        ).set_index(["model_run", "output"]),
    )
    actual = calculate_maternity_capacity(
        functional_areas,
        assumptions_df,
        config=fake_config,
    )
    mock_calculate_ward_beds.assert_called_once_with(
        functional_areas, assumptions_df, maternity_ward_assumptions_dict
    )
    expected = pd.DataFrame(
        {"output": ["output", "ward_beds"], "value": [1, 1], "model_run": [1, 1]}
    ).set_index(["model_run", "output"])
    assert_frame_equal(actual, expected)


def test_calculate_maternity_capacity_skips_missing_subgroup(mocker):
    def mock_formula(
        assumptions,
        functional_area_subgroup,
        assumptions_df,
    ):
        return pd.DataFrame(
            {"output": ["output"], "model_run": [1], "value": [1]}
        ).set_index(["model_run", "output"])

    fake_config = {
        "output": MaternityConfig(
            subgroup="subgroup",
            measure="count",
            formula=mock_formula,
            assumptions={"assumption": "assumption"},
        )
    }

    functional_areas = pd.DataFrame(
        {
            "model_run": [1],
            "functional_area": ["different_subgroup"],
            "measure": ["count"],
            "value": [1],
        }
    ).set_index(["model_run", "functional_area", "measure"])

    assumptions_df = pd.DataFrame({"Value": {"some": 10}})
    mock_calculate_ward_beds = mocker.patch(
        "nhp.capacity_conversion.ip_maternity.calculate_maternity_ward_beds",
        return_value=pd.DataFrame(
            {"output": ["ward_beds"], "model_run": [1], "value": [1]}
        ).set_index(["model_run", "output"]),
    )

    actual = calculate_maternity_capacity(
        functional_areas,
        assumptions_df,
        config=fake_config,
    )

    mock_calculate_ward_beds.assert_called_once_with(
        functional_areas, assumptions_df, maternity_ward_assumptions_dict
    )

    expected = pd.DataFrame(
        {"output": ["ward_beds"], "value": [1], "model_run": [1]}
    ).set_index(["model_run", "output"])

    assert_frame_equal(actual, expected)


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
