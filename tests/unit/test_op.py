import pandas as pd

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
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.op.convert_op_capacity",
        return_value=pd.DataFrame({"model_run": [1], "total": [0]}).set_index(
            "model_run"
        ),
    )
    functional_areas = pd.DataFrame(
        {"model_run": [1], "grouping": ["test_subgroup"], "total": [0]}
    ).set_index(["grouping", "model_run"])
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

    # assert calls
    mock_workload.assert_called_once()
    mock_convert.assert_called_once_with("workload", "OPERATIONAL_HOURS", "UTIL")

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert result.index == [("OUTPUT", 1)]


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.op.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with("op", calculate_op_capacity)
