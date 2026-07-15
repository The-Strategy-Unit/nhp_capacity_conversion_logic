import pandas as pd
from pandas.testing import assert_frame_equal

from nhp.capacity_conversion.ip_daycase import (
    DaycaseConfig,
    calculate_daycase_capacity,
    calculate_daycase_frm_recovery_occupancy,
    calculate_daycase_frm_session_capacity,
    calculate_daycase_frm_time_util,
    main,
)


def test_calculate_daycase_frm_time_util(mocker):
    subgroup = "test_subgroup"
    assumptions = {
        "treatment_time": "treatment_time",
        "treatment_utilisation": "treatment_utilisation",
        "treatment_annual_operational_hours": "treatment_annual_operational_hours",
        "output_frm_time_util": "output_frm_time_util",
    }
    functional_area_subgroup = pd.Series()
    assumptions_df = pd.DataFrame(
        {
            "Value": [
                "treatment_time",
                "treatment_utilisation",
                "treatment_annual_operational_hours",
                "output_frm_time_util",
            ]
        },
        index=[
            "treatment_time",
            "treatment_utilisation",
            "treatment_annual_operational_hours",
            "output_frm_time_util",
        ],
    )
    mock_workload = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.derive_treatment_hours",
        return_value="treatment_hours",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.calculate_time_util_capacity",
        return_value=pd.Series([1], name="total").rename_axis("model_run"),
    )
    expected = pd.DataFrame(
        {"model_run": [0], "total": [1], "output": ["output_frm_time_util"]}
    ).set_index(["output", "model_run"])
    actual = calculate_daycase_frm_time_util(
        subgroup,
        assumptions,
        functional_area_subgroup,
        assumptions_df,
    )
    assert_frame_equal(actual, expected)
    mock_workload.assert_called_once_with("treatment_time", functional_area_subgroup)
    mock_convert.assert_called_once_with(
        "treatment_hours", "treatment_annual_operational_hours", "treatment_utilisation"
    )


def test_calculate_daycase_frm_recovery_occupancy(mocker):
    subgroup = "test_subgroup"
    assumptions = {
        "recovery_time": "recovery_time",
        "recovery_occupancy": "recovery_occupancy",
        "recovery_annual_operational_hours": "recovery_annual_operational_hours",
        "output_frm_recovery_occupancy": "output_frm_recovery_occupancy",
    }
    functional_area_subgroup = pd.Series()
    assumptions_df = pd.DataFrame(
        {
            "Value": [
                "recovery_time",
                "recovery_occupancy",
                "recovery_annual_operational_hours",
                "output_frm_recovery_occupancy",
            ]
        },
        index=[
            "recovery_time",
            "recovery_occupancy",
            "recovery_annual_operational_hours",
            "output_frm_recovery_occupancy",
        ],
    )
    mock_workload = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.derive_recovery_occupancy_hours",
        return_value="occupancy_hours",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.calculate_recovery_capacity",
        return_value=pd.Series([1], name="total").rename_axis("model_run"),
    )
    expected = pd.DataFrame(
        {"model_run": [0], "total": [1], "output": ["output_frm_recovery_occupancy"]}
    ).set_index(["output", "model_run"])
    actual = calculate_daycase_frm_recovery_occupancy(
        subgroup, assumptions, functional_area_subgroup, assumptions_df
    )
    assert_frame_equal(actual, expected)
    mock_workload.assert_called_once_with(functional_area_subgroup, "recovery_time")
    mock_convert.assert_called_once_with(
        "occupancy_hours", "recovery_annual_operational_hours", "recovery_occupancy"
    )


def test_calculate_daycase_frm_session_capacity(mocker):
    subgroup = "test_subgroup"
    assumptions = {
        "annual_session_capacity": "annual_session_capacity",
        "output_frm_session_capacity": "output_frm_session_capacity",
    }
    functional_area_subgroup = pd.Series()
    assumptions_df = pd.DataFrame(
        {
            "Value": [
                "annual_session_capacity",
                "output_frm_session_capacity",
            ]
        },
        index=[
            "annual_session_capacity",
            "output_frm_session_capacity",
        ],
    )

    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.calculate_beds_from_session_capacity",
        return_value=pd.Series([1], name="total").rename_axis("model_run"),
    )
    expected = pd.DataFrame(
        {"model_run": [0], "total": [1], "output": ["output_frm_session_capacity"]}
    ).set_index(["output", "model_run"])
    actual = calculate_daycase_frm_session_capacity(
        subgroup, assumptions, functional_area_subgroup, assumptions_df
    )
    assert_frame_equal(actual, expected)
    mock_convert.assert_called_once_with(
        functional_area_subgroup, "annual_session_capacity"
    )


def test_calculate_daycase_capacity():
    def mock_formula(
        subgroup,
        assumptions,
        functional_area_subgroup,
        assumptions_df,
    ):
        return pd.DataFrame({"output": [subgroup]}).set_index("output")

    fake_config = {
        "subgroup": [
            DaycaseConfig(
                formula=mock_formula,
                assumptions={"assumption": "assumption"},
            )
        ],
        "subgroup_2": [
            DaycaseConfig(
                formula=mock_formula,
                assumptions={"assumption": "assumption"},
            )
        ],
    }

    functional_areas = pd.DataFrame(
        {"model_run": [1] * 2, "grouping": ["subgroup", "subgroup_2"], "total": [0] * 2}
    ).set_index(["model_run", "grouping"])

    assumptions_df = pd.DataFrame({"Value": {"some": 10}})
    expected = pd.DataFrame({"output": ["subgroup", "subgroup_2"]}).set_index("output")
    actual = calculate_daycase_capacity(
        functional_areas,
        assumptions_df,
        config=fake_config,
    )
    assert_frame_equal(actual, expected)


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with("ip_daycase", calculate_daycase_capacity)
