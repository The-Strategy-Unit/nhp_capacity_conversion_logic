from typing import overload

import pandas as pd


@overload
def derive_treatment_hours(time: float, procedures: float) -> float: ...


@overload
def derive_treatment_hours(time: float, procedures: pd.Series) -> pd.Series: ...


def derive_treatment_hours(
    time: float, procedures: float | pd.Series
) -> float | pd.Series:
    """Formula used for converting treatment procedure counts into workload.
    Aligns with FRM_TIME_UTIL in conversion archetypes catalogue.

    Args:
        time (float): Indicative treatment (mins)
        procedures (float | pd.Series): Number of procedures

    Returns:
        float | pd.Series: Calculated workload requirement
    """
    treatment_hours = procedures * (time / 60)
    return treatment_hours


@overload
def calculate_time_util_capacity(
    treatment_hours: float,
    annual_operational_hours: float,
    utilisation: float,
) -> float: ...


@overload
def calculate_time_util_capacity(
    treatment_hours: pd.Series,
    annual_operational_hours: float,
    utilisation: float,
) -> pd.Series: ...


def calculate_time_util_capacity(
    treatment_hours: float | pd.Series,
    annual_operational_hours: float,
    utilisation: float,
) -> float | pd.Series:
    """Converts calculated treatment hours to space requirements.
    Aligns with FRM_TIME_UTIL in conversion archetypes catalogue.

    Args:
        treatment_hours (float | pd.Series): Treatment time in hours
        annual_operational_hours (float): Operational hours per year
        utilisation (float): Utilisation rate

    Returns:
        float | pd.Series: Calculated capacity requirements
    """
    return treatment_hours / (annual_operational_hours * utilisation)


@overload
def derive_recovery_occupancy_hours(spells: float, recovery_time: float) -> float: ...


@overload
def derive_recovery_occupancy_hours(
    spells: pd.Series, recovery_time: float
) -> pd.Series: ...


def derive_recovery_occupancy_hours(
    spells: float | pd.Series, recovery_time: float
) -> float | pd.Series:
    """Formula used for calculating occupancy hours from number of spells and
    estimated recovery time. Aligns with FRM_RECOVERY_OCCUPANCY in conversion
    archetypes catalogue.

    Args:
        spells (float | pd.Series): Number of spells
        recovery_time (float): Estimated recovery time in minutes

    Returns:
        float | pd.Series: Calculated occupancy hours workload requirement
    """
    return spells * (recovery_time / 60)


@overload
def calculate_recovery_capacity(
    occupancy_hours: float,
    annual_operational_hours: float,
    occupancy_rate: float,
) -> float: ...


@overload
def calculate_recovery_capacity(
    occupancy_hours: pd.Series,
    annual_operational_hours: float,
    occupancy_rate: float,
) -> pd.Series: ...


def calculate_recovery_capacity(
    occupancy_hours: float | pd.Series,
    annual_operational_hours: float,
    occupancy_rate: float,
) -> float | pd.Series:
    """Formula used for calculating recovery capacity requirements from number of spells and
    estimated recovery time. Aligns with FRM_RECOVERY_OCCUPANCY in conversion archetypes catalogue.

    Args:
        occupancy_hours (float | pd.Series): Occupancy hours
        annual_operational_hours (float): Annual operational hours
        occupancy_rate (float): Occupancy rate

    Returns:
        float | pd.Series: Calculated capacity requirements
    """
    return occupancy_hours / (annual_operational_hours * occupancy_rate)


@overload
def calculate_beds_from_session_capacity(
    treatment_sessions: float, annual_session_capacity: float
) -> float: ...


@overload
def calculate_beds_from_session_capacity(
    treatment_sessions: pd.Series, annual_session_capacity: float
) -> pd.Series: ...


def calculate_beds_from_session_capacity(
    treatment_sessions: float | pd.Series, annual_session_capacity: float
) -> float | pd.Series:
    """Formula used for calculating capacity required in terms of treatment sessions.
    Aligns with FRM_SESSION_CAPACITY in conversion archetypes catalogue.


    Args:
        treatment_sessions (float | pd.Series): Number of renal daycase spells / treatment sessions
        annual_session_capacity (float): Annual session capacity per bed

    Returns:
        float | pd.Series: Calculated capacity requirements
    """
    return treatment_sessions / annual_session_capacity
