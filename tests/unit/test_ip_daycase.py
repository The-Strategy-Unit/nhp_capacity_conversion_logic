from unittest.mock import call

import pandas as pd

from nhp.capacity_conversion.ip_daycase import (
    calculate_daycase_frm_recovery_occupancy,
    calculate_daycase_frm_time_util,
)


def test_calculate_daycase_frm_time_util(mocker):
    subgroup = "test_subgroup"
    assumptions = {
        "treatment_time": "treatment_time",
        "treatment_utilisation": "treatment_utilisation",
        "treatment_annual_operational_hours": "treatment_annual_operational_hours",
        "output_frm_time_util": "output_frm_time_util",
    }
    functional_areas_summarised = {
        "test_subgroup": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
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
        return_value="capacity",
    )
    expected = {
        "output_frm_time_util": {
            "p10": "capacity",
            "mean": "capacity",
            "p90": "capacity",
        }
    }
    actual = calculate_daycase_frm_time_util(
        subgroup, assumptions, functional_areas_summarised, assumptions_df
    )
    assert actual == expected
    mock_workload.assert_has_calls(
        [
            call("treatment_time", 100),
            call("treatment_time", 200),
            call("treatment_time", 300),
        ],
        any_order=False,
    )
    mock_convert.assert_has_calls(
        [
            call(
                "treatment_hours",
                "treatment_annual_operational_hours",
                "treatment_utilisation",
            )
        ]
        * 3
    )


def test_calculate_daycase_frm_recovery_occupancy(mocker):
    subgroup = "test_subgroup"
    assumptions = {
        "recovery_time": "recovery_time",
        "recovery_occupancy": "recovery_occupancy",
        "recovery_annual_operational_hours": "recovery_annual_operational_hours",
        "output_frm_recovery_occupancy": "output_frm_recovery_occupancy",
    }
    functional_areas_summarised = {
        "test_subgroup": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
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
        return_value="capacity",
    )
    expected = {
        "output_frm_recovery_occupancy": {
            "p10": "capacity",
            "mean": "capacity",
            "p90": "capacity",
        }
    }
    actual = calculate_daycase_frm_recovery_occupancy(
        subgroup, assumptions, functional_areas_summarised, assumptions_df
    )
    assert actual == expected
    mock_workload.assert_has_calls(
        [
            call(100, "recovery_time"),
            call(200, "recovery_time"),
            call(300, "recovery_time"),
        ],
        any_order=False,
    )
    mock_convert.assert_has_calls(
        [
            call(
                "occupancy_hours",
                "recovery_annual_operational_hours",
                "recovery_occupancy",
            )
        ]
        * 3
    )
