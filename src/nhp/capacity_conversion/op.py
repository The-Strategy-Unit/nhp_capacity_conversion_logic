import sys
from typing import cast

import pandas as pd
from nhpy.utils import get_logger

from nhp.capacity_conversion.utils import run_single_activity_type

logger = get_logger()


ASSUMPTIONS_MAPPING = {
    "op_procedures": {
        "time": "OP_PROC_TIME",
        "dna_rate": "OP_PROC_DNA_RATE",
        "dna_time": "OP_PROC_DNA_TIME",
        "util": "OP_PROC_UTIL",
        "operational_hours": "OP_PROC_ANNUAL_OPERATIONAL_HOURS",
        "output": "OP_PROC_ROOMS",
    },
    "op_first_attendances": {
        "time": "OP_CONSULT_FIRST_TIME",
        "dna_rate": "OP_CONSULT_FIRST_DNA_RATE",
        "dna_time": "OP_CONSULT_FIRST_DNA_TIME",
        "util": "OP_CONSULT_UTIL",
        "operational_hours": "OP_CONSULT_ANNUAL_OPERATIONAL_HOURS",
        "output": "FIRST_OP_CONSULT_ROOMS",
    },
    "op_follow_up_attendances": {
        "time": "OP_CONSULT_FOLLOW_UP_TIME",
        "dna_rate": "OP_CONSULT_FOLLOW_UP_DNA_RATE",
        "dna_time": "OP_CONSULT_FOLLOW_UP_DNA_TIME",
        "util": "OP_CONSULT_UTIL",
        "operational_hours": "OP_CONSULT_ANNUAL_OPERATIONAL_HOURS",
        "output": "FOLLOW_UP_OP_CONSULT_ROOMS",
    },
    "op_virtual_attendances": {
        "time": "OP_VIRTUAL_CONSULT_TIME",
        "dna_rate": "OP_VIRTUAL_CONSULT_DNA_RATE",
        "dna_time": "OP_VIRTUAL_CONSULT_DNA_TIME",
        "util": "OP_VIRTUAL_CONSULT_UTIL",
        "operational_hours": "OP_VIRTUAL_CONSULT_ANNUAL_OPERATIONAL_HOURS",
        "output": "OP_VIRTUAL_CONSULT_ROOMS",
    },
}


def derive_op_workload(
    time: float, dna_rate: float, dna_time: float, attendances: float
) -> float:
    """Formula used for converting all OP functional area activity to workload

    Args:
        time (float): Indicative appointment duration (mins)
        dna_rate (float): Indicative DNA rate
        dna_time (float): Indicative DNA time consumed (mins)
        attendances (float): Number of attendances

    Returns:
        float: Calculated workload requirement
    """
    effective_time_mins = time + (dna_rate * dna_time)
    workload_hours = (attendances * effective_time_mins) / 60
    return workload_hours


def convert_op_capacity(
    workload_hours: float,
    operational_hours: float,
    utilisation_rate: float,
) -> float:
    """Formula used for converting all OP functional area activity to capacity requirements

    Args:
        workload_hours (float): Workload hours per year
        operational_hours (float): Room operational hours per year
        utilisation_rate (float): Room utilisation rate

    Returns:
        float: Calculated capacity requirement
    """
    return workload_hours / (operational_hours * utilisation_rate)


def calculate_op_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated OP capacity requirements
    """
    logger.info("Calculating OP capacity")
    results_dict = {}
    for subgroup in functional_areas_summarised.keys():
        results = {}

        time = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["time"], "Value"],
        )
        dna_rate = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["dna_rate"], "Value"],
        )
        dna_time = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["dna_time"], "Value"],
        )
        utilisation_rate = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["util"], "Value"],
        )
        operational_hours = cast(
            float,
            assumptions_df.at[
                ASSUMPTIONS_MAPPING[subgroup]["operational_hours"], "Value"
            ],
        )
        output = ASSUMPTIONS_MAPPING[subgroup]["output"]
        for value in ["p10", "mean", "p90"]:
            workload_hours = derive_op_workload(
                time, dna_rate, dna_time, functional_areas_summarised[subgroup][value]
            )
            results[value] = convert_op_capacity(
                workload_hours, operational_hours, utilisation_rate
            )
        results_dict[output] = results
    return pd.DataFrame.from_dict(results_dict, orient="index")


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type("op", calculate_op_capacity)


if __name__ == "__main__":
    sys.exit(main())
