from nhpy.utils import (
    configure_logging,
    get_logger,
)
import pandas as pd
from nhp.capacity_conversion.utils import (
    load_assumptions,
    summarise_functional_areas,
    save_results_to_excel,
    load_metadata_from_ats,
    create_aggregations_path,
    validate_required_env_vars,
    load_aggregations,
)
import argparse
from typing import cast
import sys
from logging import INFO
from datetime import datetime

logger = get_logger()


def convert_ip_daycase_capacity(
    daycase_spells: float,
    assumed_los_hours: float,
    operational_hours: float,
    operational_days: float,
    occupancy_rate: float,
) -> float:
    """Formula used for converting IP daycase functional area activity to capacity requirements

    Args:
        daycase_spells (float): Number of daycase spells
        assumed_los_hours (float): Indicative stay in hours
        operational_hours (float): Operational hours per day
        operational_days (float): Operational days per year
        occupancy_rate (float): Occupancy rate

    Returns:
        float: Calculated capacity requirement
    """
    return (daycase_spells * (assumed_los_hours)) / (
        operational_hours * operational_days * occupancy_rate
    )


def calculate_ip_daycase_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated IP daycase capacity requirements
    """
    logger.info("Calculating IP daycase capacity")
    results_dict = {}
    for capacity_requirement in [
        "adult_surgical_daycase",
        "adult_medical_daycase",
        "paediatric_surgical_daycase",
        "paediatric_medical_daycase",
    ]:
        results = {}
        assumed_los_hours = cast(
            float,
            assumptions_df.at[capacity_requirement + "_stay_hours", "assumption_value"],
        )

        operational_hours = cast(
            float,
            assumptions_df.at[
                capacity_requirement.split("_")[0] + "_daycase_operational_hours",
                "assumption_value",
            ],
        )
        operational_days = cast(
            float,
            assumptions_df.at[
                capacity_requirement.split("_")[0] + "_daycase_operational_days",
                "assumption_value",
            ],
        )
        occupancy_rate = cast(
            float,
            assumptions_df.at[
                capacity_requirement.split("_")[0] + "_daycase_recovery_occupancy_rate",
                "assumption_value",
            ],
        )
        for value in ["p10", "mean", "p90"]:
            results[value] = convert_ip_daycase_capacity(
                functional_areas_summarised[capacity_requirement][value],
                assumed_los_hours,
                operational_hours,
                operational_days,
                occupancy_rate,
            )
        results_dict[capacity_requirement] = results
    ip_daycase_capacity = pd.DataFrame.from_dict(results_dict, orient="index")
    ip_daycase_capacity.index = [i + "_beds" for i in ip_daycase_capacity.index]
    return ip_daycase_capacity


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    configure_logging(INFO)
    capacity_conversion_runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Generate IP capacity outputs given functional area aggregations of IP activity"
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
    ip_daycase_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        aggregations_path,
        "ip_daycase",
    )
    functional_areas_summarised = summarise_functional_areas(ip_daycase_aggregations)
    data_to_save["ip_daycase_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    ip_daycase_capacity_df = calculate_ip_daycase_capacity(
        functional_areas_summarised, assumptions
    )
    data_to_save["ip_daycase_capacity"] = ip_daycase_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
