import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal

from nhp.capacity_conversion.op import (
    convert_op_capacity,
    map_op_capacity_to_functional_area,
    calculate_op_capacity,
    main,
)


def test_convert_op_capacity():
    # arrange
    attendances = 100000
    duration_mins = 38
    dna_rate = 0.07
    dna_time_mins = 20
    operational_hours = 40
    operational_weeks = 48
    utilisation_rate = 0.85

    expected = (3940000 / 60) / 1632

    actual = convert_op_capacity(
        attendances,
        duration_mins,
        dna_rate,
        dna_time_mins,
        operational_hours,
        operational_weeks,
        utilisation_rate,
    )
    # assert
    assert actual == expected


def test_map_op_capacity_to_functional_area():
    # arrange
    capacity_requirement_strings = ["op_procedures", "op_first"]
    expected = ["outpatient_procedures", "outpatient_first_attendances"]
    # act
    actual = [
        map_op_capacity_to_functional_area(var_name)
        for var_name in capacity_requirement_strings
    ]
    # assert
    assert actual == expected


def test_calculate_op_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")

    capacity_requirements = [
        "op_first",
        "op_followup",
        "op_virtual",
        "op_procedures",
    ]
    mocker.patch(
        "nhp.capacity_conversion.op.map_op_capacity_to_functional_area",
        return_value="mock_functional_area",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.op.convert_op_capacity",
        return_value=999,
    )
    functional_areas_summarised = {
        "mock_functional_area": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
    assumptions_data = {}
    for req in capacity_requirements:
        assumptions_data[f"{req}_duration_mins"] = {"assumption_value": 1}
        assumptions_data[f"{req}_dna_rate"] = {"assumption_value": 2}
        assumptions_data[f"{req}_dna_time_mins"] = {"assumption_value": 3}
        assumptions_data[f"{req}_operational_hours"] = {"assumption_value": 4}
        assumptions_data[f"{req}_operational_weeks"] = {"assumption_value": 5}
        assumptions_data[f"{req}_utilisation_rate"] = {"assumption_value": 6}

    assumptions_df = pd.DataFrame.from_dict(assumptions_data, orient="index")

    output_index = [
        "op_first",
        "op_followup",
        "op_virtual_consultation_rooms",
        "op_procedure_rooms",
        "op_consultation_rooms",
    ]

    # act
    result = calculate_op_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # convert_op_capacity should be called 12 times (4 × 3)
    assert mock_convert.call_count == 12

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == output_index

    # all values should be mocked return value except op_consultation_rooms
    assert (result.loc[output_index[:-1]] == 999).all().all()
    assert (result.loc[output_index[-1]] == 999 * 2).all().all()

    # check arguments in calls to convert_op_capacity
    first_call = mock_convert.call_args_list[0]
    args = first_call.args
    assert args[1] == 1  # duration
    assert args[2] == 2  # dna_rate
    assert args[3] == 3  # dna_time
    assert args[4] == 4  # operational_hours
    assert args[5] == 5  # operational_weeks
    assert args[6] == 6  # utilisation_rate
