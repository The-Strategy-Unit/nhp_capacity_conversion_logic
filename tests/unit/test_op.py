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
