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


def derive_aae_workload(attendances: float, assumed_los_mins: float) -> float:
    """Formula used for converting all A&E functional area activity to capacity requirements

    Args:
        attendances (float): Number of attendances
        assumed_los_mins (float): Assumed length of stay in emergency department in minutes

    Returns:
        float: Calculated workload in occupancy hours
    """
    return attendances * assumed_los_mins / 60


def convert_aae_capacity(
    occupancy_hours: float,
    annual_operational_hours: float,
    utilisation: float,
) -> float:
    """Formula used for converting A&E workload to capacity requirements

    Args:
        occupancy_hours (float): Number of occupancy hours per year
        annual_operational_hours (float): Number of operating hours per year
        utilisation (float): Utilisation of the resource, expressed as a decimal

    Returns:
        float: Calculated capacity requirement
    """
    return occupancy_hours / (annual_operational_hours * utilisation)


def calculate_aae_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated A&E capacity requirements
    """
    logger.info("Calculating A&E capacity")
    results_dict = {}
    for subgroup in functional_areas_summarised.keys():
        results = {}
        assumed_los_mins = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["los"], "assumption_value"],
        )
        annual_operational_hours = cast(
            float,
            assumptions_df.at[
                ASSUMPTIONS_MAPPING[subgroup]["hours"], "assumption_value"
            ],
        )
        utilisation = cast(
            float,
            assumptions_df.at[
                ASSUMPTIONS_MAPPING[subgroup]["util"], "assumption_value"
            ],
        )

        for value in ["p10", "mean", "p90"]:
            occupancy_hours = derive_aae_workload(
                functional_areas_summarised[subgroup][value], assumed_los_mins
            )
            results[value] = convert_aae_capacity(
                occupancy_hours,
                annual_operational_hours=annual_operational_hours,
                utilisation=utilisation,
            )
        results_dict[ASSUMPTIONS_MAPPING[subgroup]["output"]] = results
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
        description="Generate A&E capacity outputs given functional area aggregations of A&E activity"
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
        help="Path to assumptions file (default: 'data/reference/default_assumptions.csv')",
        default="data/reference/default_assumptions.csv",
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
    aae_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "aae"
    )
    functional_areas_summarised = summarise_functional_areas(aae_aggregations)
    data_to_save["aae_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    aae_capacity_df = calculate_aae_capacity(functional_areas_summarised, assumptions)
    data_to_save["aae_capacity"] = aae_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
