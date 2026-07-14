import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.aae import (
    calculate_aae_capacity,
    convert_aae_capacity,
    derive_aae_workload,
    main,
)


def test_derive_aae_workload():
    # arrange
    attendances = 10
    assumed_los_mins = 120
    expected = 20
    # act
    actual = derive_aae_workload(attendances, assumed_los_mins)
    # assert
    assert actual == expected


def test_convert_aae_capacity():
    # arrange
    occupancy_hours = 100
    annual_operational_hours = 20
    utilisation = 0.5

    expected = 10

    # act
    actual = convert_aae_capacity(
        occupancy_hours,
        annual_operational_hours,
        utilisation,
    )
    # assert
    assert actual == expected


def test_calculate_aae_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mocker.patch(
        "nhp.capacity_conversion.aae.ASSUMPTIONS_MAPPING",
        {
            "test_subgroup": {
                "los": "LOS",
                "hours": "HOURS",
                "util": "UTIL",
                "output": "output_spaces",
            }
        },
    )

    functional_areas = pd.DataFrame(
        {"total": [0]},
        index=pd.MultiIndex.from_tuples(
            [("test_subgroup", 1)],
            names=["grouping", "model_run"],
        ),
    )
    mock_workload = mocker.patch(
        "nhp.capacity_conversion.aae.derive_aae_workload",
        return_value="workload",
    )
    returned_capacity = pd.DataFrame(
        {"total": [0]},
        index=pd.Index([1], name="model_run"),
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.aae.convert_aae_capacity",
        return_value=returned_capacity,
    )

    assumptions_df = pd.DataFrame(
        {"Value": ["LOS", "HOURS", "UTIL"]},
        index=["LOS", "HOURS", "UTIL"],
    )

    # act
    result = calculate_aae_capacity(
        functional_areas,
        assumptions_df,
    )

    # assert

    assert "Calculating A&E capacity" in caplog.text

    # test calls to mocked functions
    args, kwargs = mock_workload.call_args

    assert args[1] == "LOS"
    assert_series_equal(
        args[0],
        functional_areas.xs("test_subgroup", level="grouping")["total"],
    )

    mock_convert.assert_called_once_with(
        "workload", annual_operational_hours="HOURS", utilisation="UTIL"
    )

    # output structure
    expected = pd.DataFrame(
        {"total": [0]},
        index=pd.MultiIndex.from_tuples(
            [("output_spaces", 1)],
            names=["output", "model_run"],
        ),
    )

    pd.testing.assert_frame_equal(result, expected)


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.aae.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with("aae", calculate_aae_capacity)
