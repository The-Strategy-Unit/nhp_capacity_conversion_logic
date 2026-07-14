import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.op import (
    calculate_op_capacity,
    convert_op_capacity,
    derive_op_workload,
    main,
)


def test_derive_op_workload():
    # arrange
    time = 20
    dna_rate = 0.1
    dna_time = 20
    attendances = 60
    expected = 22
    # act
    actual = derive_op_workload(time, dna_rate, dna_time, attendances)
    # assert
    assert actual == expected


def test_convert_op_capacity():
    # arrange
    workload_hours = 100
    operational_hours = 50
    utilisation_rate = 0.1
    expected = 20

    # act
    actual = convert_op_capacity(workload_hours, operational_hours, utilisation_rate)
    # assert
    assert actual == expected


def test_calculate_op_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mocker.patch(
        "nhp.capacity_conversion.op.ASSUMPTIONS_MAPPING",
        {
            "test_subgroup": {
                "time": "TIME",
                "dna_rate": "DNA_RATE",
                "dna_time": "DNA_TIME",
                "util": "UTIL",
                "operational_hours": "OPERATIONAL_HOURS",
                "output": "OUTPUT",
            }
        },
    )

    mock_workload = mocker.patch(
        "nhp.capacity_conversion.op.derive_op_workload",
        return_value="workload",
    )
    returned_capacity = pd.DataFrame(
        {"total": [0]},
        index=pd.Index([1], name="model_run"),
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.op.convert_op_capacity",
        return_value=returned_capacity,
    )
    functional_areas = pd.DataFrame(
        {"total": [0]},
        index=pd.MultiIndex.from_tuples(
            [("test_subgroup", 1)],
            names=["grouping", "model_run"],
        ),
    )
    assumptions_df = pd.DataFrame(
        {
            "Value": [
                "TIME",
                "DNA_RATE",
                "DNA_TIME",
                "UTIL",
                "OPERATIONAL_HOURS",
                "OUTPUT",
            ]
        },
        index=["TIME", "DNA_RATE", "DNA_TIME", "UTIL", "OPERATIONAL_HOURS", "OUTPUT"],
    )

    # act
    result = calculate_op_capacity(
        functional_areas,
        assumptions_df,
    )

    assert "Calculating OP capacity" in caplog.text
    # assert calls
    args, kwargs = mock_workload.call_args

    assert args[:3] == ("TIME", "DNA_RATE", "DNA_TIME")
    assert_series_equal(
        args[3],
        functional_areas.xs("test_subgroup", level="grouping")["total"],
    )
    assert kwargs == {}
    mock_convert.assert_called_once_with("workload", "OPERATIONAL_HOURS", "UTIL")

    # output structure
    expected = pd.DataFrame(
        {"total": [0]},
        index=pd.MultiIndex.from_tuples(
            [("OUTPUT", 1)],
            names=["output", "model_run"],
        ),
    )

    pd.testing.assert_frame_equal(result, expected)


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.op.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with("op", calculate_op_capacity)
