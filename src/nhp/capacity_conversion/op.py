import argparse
import sys
from datetime import datetime
from logging import INFO
from typing import cast

import pandas as pd
from nhpy.utils import (
    configure_logging,
    get_logger,
)

from nhp.capacity_conversion.config import ASSUMPTIONS_URL
from nhp.capacity_conversion.utils import (
    create_aggregations_path,
    load_aggregations,
    load_assumptions,
    load_metadata_from_ats,
    save_results_to_excel,
    summarise_functional_areas,
    validate_required_env_vars,
)

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
    configure_logging(INFO)
    capacity_conversion_runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Generate OP capacity outputs given functional area aggregations of OP activity"
    )
    parser.add_argument(
        "guid",
        help="GUID of functional area aggregation to convert into capacity",
    )
    parser.add_argument(
        "--capacity_model_version",
        help="Capacity model version",
        default="dev",
    )
    parser.add_argument(
        "--path_to_assumptions_file",
        help=f"Path to assumptions file (default: '{ASSUMPTIONS_URL}')",
        default=ASSUMPTIONS_URL,
    )
    args = parser.parse_args()
    config = validate_required_env_vars()
    data_to_save = {}
    metadata = load_metadata_from_ats(
        args.guid,
        config["AZ_TABLE_ENDPOINT"],
        config["TABLE_NAME"],
        args.capacity_model_version,
    )
    metadata["capacity_conversion_runtime"] = capacity_conversion_runtime
    data_to_save["metadata"] = pd.Series(metadata).drop(["PartitionKey", "RowKey"])
    assumptions = load_assumptions(args.path_to_assumptions_file)
    data_to_save["assumptions"] = assumptions
    aggregations_path = create_aggregations_path(metadata)
    op_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "op"
    )
    functional_areas_summarised = summarise_functional_areas(op_aggregations)
    data_to_save["op_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    op_capacity_df = calculate_op_capacity(functional_areas_summarised, assumptions)
    data_to_save["op_capacity"] = op_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
