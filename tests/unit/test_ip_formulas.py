from nhp.capacity_conversion.ip_formulas import (
    calculate_beds,
    calculate_beds_from_session_capacity,
    calculate_recovery_capacity,
    calculate_time_util_capacity,
    derive_beddays_from_spells,
    derive_recovery_occupancy_hours,
    derive_treatment_hours,
)


def test_derive_beddays_from_spells():
    zero_day_spells = 2
    zero_day_los = 2880
    expected = 4
    actual = derive_beddays_from_spells(zero_day_spells, zero_day_los)
    assert actual == expected


def test_calculate_beds():
    beddays = 20
    operational_days = 2
    occupancy = 5
    expected = 2
    actual = calculate_beds(beddays, operational_days, occupancy)
    assert actual == expected


def test_derive_treatment_hours():
    # arrange
    time = 120
    procedures = 10
    expected = 20
    # act
    actual = derive_treatment_hours(time, procedures)
    # assert
    assert actual == expected


def test_calculate_time_util_capacity():
    treatment_hours = 100
    annual_operational_hours = 20
    utilisation = 0.5
    expected = 10
    actual = calculate_time_util_capacity(
        treatment_hours, annual_operational_hours, utilisation
    )
    assert actual == expected


def test_derive_recovery_occupancy_hours():
    spells = 10
    recovery_time = 120
    expected = 20
    actual = derive_recovery_occupancy_hours(spells, recovery_time)
    assert actual == expected


def test_calculate_recovery_capacity():
    occupancy_hours = 100
    annual_operational_hours = 20
    occupancy_rate = 0.5
    expected = 10
    actual = calculate_recovery_capacity(
        occupancy_hours, annual_operational_hours, occupancy_rate
    )
    assert actual == expected


def test_calculate_beds_from_session_capacity():
    treatment_sessions = 20
    annual_session_capacity = 5
    expected = 4
    actual = calculate_beds_from_session_capacity(
        treatment_sessions, annual_session_capacity
    )
    assert actual == expected
