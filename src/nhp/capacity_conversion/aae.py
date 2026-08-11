import logging
import sys
from typing import cast, overload

import pandas as pd

from nhp.capacity_conversion.utils import run_single_activity_type

logger = logging.getLogger(__name__)


ASSUMPTIONS_MAPPING = {
    "adult_major_attendances": {
        "los": "AE_ADULT_MAJOR_LOS",
        "util": "AE_ADULT_MAJOR_UTIL",
        "hours": "AE_BEDS_ANNUAL_OPERATIONAL_HOURS",
        "output": "ADULT_MAJOR_AE_BEDS",
    },
    "adult_minor_attendances": {
        "los": "AE_ADULT_MINOR_LOS",
        "util": "AE_ADULT_MINOR_UTIL",
        "hours": "AE_BAYS_ANNUAL_OPERATIONAL_HOURS",
        "output": "ADULT_MINOR_AE_BAYS",
    },
    "child_major_attendances": {
        "los": "AE_CHILD_MAJOR_LOS",
        "util": "AE_CHILD_MAJOR_UTIL",
        "hours": "AE_BEDS_ANNUAL_OPERATIONAL_HOURS",
        "output": "CHILD_MAJOR_AE_BEDS",
    },
    "child_minor_attendances": {
        "los": "AE_CHILD_MINOR_LOS",
        "util": "AE_CHILD_MINOR_UTIL",
        "hours": "AE_BAYS_ANNUAL_OPERATIONAL_HOURS",
        "output": "CHILD_MINOR_AE_BAYS",
    },
    "resus_attendances": {
        "los": "AE_RESUS_LOS",
        "util": "AE_RESUS_UTIL",
        "hours": "AE_BEDS_ANNUAL_OPERATIONAL_HOURS",
        "output": "RESUS_AE_BEDS",
    },
    "sdec_attendances": {
        "los": "SDEC_SPACES_LOS",
        "util": "SDEC_SPACES_UTIL",
        "hours": "SDEC_SPACES_ANNUAL_OPERATIONAL_HOURS",
        "output": "SDEC_SPACES",
    },
}


@overload
def derive_aae_workload(attendances: float, assumed_los_mins: float) -> float: ...


@overload
def derive_aae_workload(
    attendances: pd.Series, assumed_los_mins: float
) -> pd.Series: ...


def derive_aae_workload(
    attendances: float | pd.Series, assumed_los_mins: float
) -> float | pd.Series:
    """Formula used for converting all A&E functional area activity to workload

    Args:
        attendances (float): Number of attendances
        assumed_los_mins (float): Assumed length of stay in emergency department in minutes

    Returns:
        float | pd.Series: Calculated workload in occupancy hours
    """
    return attendances * assumed_los_mins / 60


@overload
def convert_aae_capacity(
    occupancy_hours: float,
    annual_operational_hours: float,
    utilisation: float,
) -> float: ...


@overload
def convert_aae_capacity(
    occupancy_hours: pd.Series,
    annual_operational_hours: float,
    utilisation: float,
) -> pd.Series: ...


def convert_aae_capacity(
    occupancy_hours: float | pd.Series,
    annual_operational_hours: float,
    utilisation: float,
) -> float | pd.Series:
    """Formula used for converting A&E workload to capacity requirements

    Args:
        occupancy_hours (float): Number of occupancy hours per year
        annual_operational_hours (float): Number of operating hours per year
        utilisation (float): Utilisation of the resource, expressed as a decimal

    Returns:
        float | pd.Series: Calculated capacity requirement
    """
    return occupancy_hours / (annual_operational_hours * utilisation)


def calculate_aae_capacity(
    functional_areas: pd.DataFrame, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated A&E capacity requirements
    """
    logger.info("Calculating A&E capacity")
    results_list = []
    for subgroup in functional_areas.index.get_level_values("grouping").unique():
        fa_df = functional_areas.loc[subgroup, :]
        assumed_los_mins = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["los"], "Value"],
        )
        annual_operational_hours = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["hours"], "Value"],
        )
        utilisation = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["util"], "Value"],
        )

        occupancy_hours = derive_aae_workload(fa_df["total"], assumed_los_mins)
        results = convert_aae_capacity(
            occupancy_hours,
            annual_operational_hours=annual_operational_hours,
            utilisation=utilisation,
        )
        results_df = pd.DataFrame(results)
        results_df.loc[:, "output"] = ASSUMPTIONS_MAPPING[subgroup]["output"]
        results_list.append(results_df.reset_index().set_index(["output", "model_run"]))
    return pd.concat(results_list)


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type("aae", calculate_aae_capacity)


if __name__ == "__main__":
    sys.exit(main())
