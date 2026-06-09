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


def calculate_unknown_theatres_duration(
    number_of_admissions: float, average_theatre_time_mins: float
) -> float:
    """Calculate total theatre time in minutes for surgical admissions without known theatre time

    Args:
        number_of_admissions (float): Number of surgical admissions with unknown theatre time
        average_theatre_time_mins (float): Average theatre time for that type of admission

    Returns:
        float: Approximated total theatre time in minutes for surgical admissions with unknown theatre time
    """
    return number_of_admissions * average_theatre_time_mins


def convert_ip_theatres_capacity(
    total_theatres_duration_mins: float,
    operational_hours: float,
    operational_days: float,
    utilisation_rate: float,
) -> float:
    """Formula used for converting IP daycase functional area activity to capacity requirements

    Args:
        total_theatres_duration_mins (float): Number of minutes spent by patients in theatres
        operational_hours (float): Operational hours per day
        operational_days (float): Operational days per year
        utilisation_rate (float): Utilisation rate

    Returns:
        float: Calculated capacity requirement
    """
    return (total_theatres_duration_mins / 60) / (
        operational_hours * operational_days * utilisation_rate
    )


def calculate_ip_theatres_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated IP daycase capacity requirements
    """
    logger.info("Calculating IP theatres capacity")
    results_dict = {}
    for capacity_requirement in [
        "adult_nonelective_theatres",
        "adult_elective_theatres",
        "paediatric_nonelective_theatres",
        "paediatric_elective_theatres",
    ]:
        results = {}

        operational_hours = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operational_hours",
                "assumption_value",
            ],
        )
        operational_days = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operational_days",
                "assumption_value",
            ],
        )
        utilisation_rate = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_utilisation_rate",
                "assumption_value",
            ],
        )
        functional_area_name = capacity_requirement.replace(
            "theatres", "surgical_procedures"
        )
        average_theatre_time_mins = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_average_time_mins",
                "assumption_value",
            ],
        )
        for value in ["p10", "mean", "p90"]:
            # get minutes
            unknown_theatres_duration = calculate_unknown_theatres_duration(
                functional_areas_summarised[functional_area_name + "_unknown_time"][
                    value
                ],
                average_theatre_time_mins,
            )
            known_theatres_duration = functional_areas_summarised[functional_area_name][
                value
            ]
            results[value] = convert_ip_theatres_capacity(
                unknown_theatres_duration + known_theatres_duration,
                operational_hours,
                operational_days,
                utilisation_rate,
            )
        results_dict[capacity_requirement] = results
    ip_theatres_capacity = pd.DataFrame.from_dict(results_dict, orient="index")
    # ip_theatres_capacity.index = [i + "_beds" for i in ip_theatres_capacity.index]
    return ip_theatres_capacity


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
    ip_theatres_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        aggregations_path,
        "ip_theatres",
    )
    functional_areas_summarised = summarise_functional_areas(ip_theatres_aggregations)
    data_to_save["ip_theatres_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    ip_theatres_capacity_df = calculate_ip_theatres_capacity(
        functional_areas_summarised, assumptions
    )
    data_to_save["ip_theatres_capacity"] = ip_theatres_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
