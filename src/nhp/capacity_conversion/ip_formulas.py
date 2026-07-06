def derive_treatment_hours(time: float, procedures: float) -> float:
    """Formula used for converting treatment procedure counts into workload.
    Aligns with FRM_TIME_UTIL in conversion archetypes catalogue.

    Args:
        time (float): Indicative treatment (mins)
        procedures (float): Number of procedures

    Returns:
        float: Calculated workload requirement
    """
    treatment_hours = procedures * (time / 60)
    return treatment_hours


def calculate_time_util_capacity(
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


def derive_recovery_occupancy_hours(spells: float, recovery_time: float) -> float:
    """Formula used for calculating occupancy hours from number of spells and
    estimated recovery time. Aligns with FRM_RECOVERY_OCCUPANCY in conversion
    archetypes catalogue.

    Args:
        spells (float): Number of spells
        recovery_time (float): Estimated recovery time in minutes

    Returns:
        float: Calculated occupancy hours workload requirement
    """
    return spells * (recovery_time / 60)


def calculate_recovery_capacity(
    occupancy_hours: float, annual_operational_hours: float, occupancy_rate: float
) -> float:
    """Formula used for calculating recovery capacity requirements from number of spells and
    estimated recovery time. Aligns with FRM_RECOVERY_OCCUPANCY in conversion archetypes catalogue.

    Args:
        occupancy_hours (float): Occupancy hours
        annual_operational_hours (float): Annual operational hours
        occupancy_rate (float): Occupancy rate

    Returns:
        float: Calculated capacity requirements
    """
    return occupancy_hours / (annual_operational_hours * occupancy_rate)
