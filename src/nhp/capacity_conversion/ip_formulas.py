def derive_treatment_hours(time: float, procedures: float) -> float:
    """Formula used for converting all OP functional area activity to workload.
    Aligns with FRM_TIME_UTIL in conversion archetypes catalogue.

    Args:
        time (float): Indicative treatment (mins)
        procedures (float): Number of procedures

    Returns:
        float: Calculated workload requirement
    """
    treatment_hours = procedures * (time / 60)
    return treatment_hours


def convert_time_util_capacity(
    treatment_hours: float, annual_operational_hours: float, utilisation: float
) -> float:
    """Converts calculated treatment hours to space requirements.
    Aligns with FRM_TIME_UTIL in conversion archetypes catalogue.

    Args:
        treatment_hours (float): Treatment time in hours
        annual_operational_hours (float): Operational hours per year
        utilisation (float): Utilisation rate

    Returns:
        float: Calculated capacity requirements
    """
    return treatment_hours / (annual_operational_hours * utilisation)
