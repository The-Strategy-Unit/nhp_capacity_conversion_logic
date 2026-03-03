from nhpy.utils import (
    configure_logging,
    get_logger,
)
import pandas as pd
from nhpy.az import connect_to_container, load_parquet_file
from nhp.capacity_conversion.utils import (
    load_assumptions,
    save_results_to_csv,
    calculate_prediction_intervals_and_mean,
    load_metadata_from_ats,
    create_aggregations_path,
    validate_required_env_vars,
)
import argparse
from typing import cast
import sys
import os
from dotenv import load_dotenv
from logging import INFO
from datetime import datetime

logger = get_logger()


def load_aae_aggregations(
    account_url: str, results_container: str, aggregations_path: str
) -> pd.DataFrame:
    """Loads aggregated A&E data from Azure

    Args:
        account_url (str): Azure Storage account URL
        results_container (str): Azure Storage container name with results
        aggregations_path (str): Path to "folder" with data to load

    Returns:
        pd.DataFrame: Loads aggregated A&E data
    """
    logger.info(f"Loading A&E data from {aggregations_path}...")
    results_connection = connect_to_container(account_url, results_container)
    aae_aggregations = load_parquet_file(
        results_connection, f"{aggregations_path}/aae.parquet"
    )
    return aae_aggregations


def map_unknown(groupings_column: pd.Series) -> pd.Series:
    """Map "unknown" activity in A&E to different functional area

    Returns:
        pd.Series: Column with activity mapped
    """

    # TODO: Mapping 'unknown' to 'minor' is a temporary workaround. See issue #6
    return groupings_column.replace(
        to_replace={
            "adult_unknown": "adult_minor_attendances",
            "child_unknown": "child_minor_attendances",
        },
    )


def process_aae(aae_aggregations: pd.DataFrame) -> dict[str, dict]:
    """Process A&E data ready for conversion to capacity

    Args:
        aae_aggregations (pd.DataFrame): Dataframe with A&E functional areas and activity

    Returns:
        dict[str, dict]: Dictionary with p10, p90 and mean for each functional area
    """
    aae_aggregations.loc[:, "grouping"] = map_unknown(aae_aggregations["grouping"])
    aae_aggregations = (
        aae_aggregations.reset_index()
        .groupby(["model_run", "grouping"])
        .sum(numeric_only=True)
    )
    aae = aae_aggregations.drop([0], axis=0)  # model_run 0 is baseline
    functional_areas_summarised = {}
    for grouping in aae.index.unique(level="grouping"):
        functional_areas_summarised[grouping] = calculate_prediction_intervals_and_mean(
            aae.loc[(slice(None), grouping), :]["arrivals"]
        )
    return functional_areas_summarised


def convert_aae_capacity(
    attendances: float,
    assumed_los_mins: float,
    operating_weeks_per_year: float,
    operating_hours_per_week: float,
    utilisation_rate: float,
) -> float:
    """Formula used for converting all A&E functional area activity to capacity requirements

    Args:
        attendances (float): Number of attendances
        assumed_los_mins (float): Assumed length of stay in emergency department in minutes
        operating_weeks_per_year (float): Number of operating weeks per year
        operating_hours_per_week (float): Number of operating hours per week
        utilisation_rate (float): Utilisation rate of the resource

    Returns:
        float: Calculated capacity requirement
    """
    return (attendances * assumed_los_mins / 60) / (
        operating_weeks_per_year * operating_hours_per_week * utilisation_rate
    )


def map_aae_capacity_to_functional_area(capacity_requirement_string: str) -> str:
    """Alters string so that we can look up the correct functional area to use for
    each capacity requirement

    Args:
        capacity_requirement_string (str): Capacity requirement name

    Returns:
        str: Corresponding functional area name
    """
    words_to_replace = ["beds", "bays", "spaces"]

    for word in words_to_replace:
        capacity_requirement_string = capacity_requirement_string.replace(
            word, "attendances"
        )
    return capacity_requirement_string


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
    for capacity_requirement in [
        "adult_major_spaces",
        "adult_minor_spaces",
        "child_major_spaces",
        "child_minor_spaces",
        "sdec_spaces",
        "resus_spaces",
    ]:
        results = {}
        assumed_los_mins = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_assumed_los", "assumption_value"
            ],
        )
        operating_hours_per_week = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operating_hours", "assumption_value"
            ],
        )
        operating_weeks_per_year = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operating_weeks", "assumption_value"
            ],
        )
        utilisation_rate = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_utilisation", "assumption_value"
            ],
        )

        for value in ["p10", "mean", "p90"]:
            functional_area = map_aae_capacity_to_functional_area(capacity_requirement)
            results[value] = convert_aae_capacity(
                functional_areas_summarised[functional_area][value],
                assumed_los_mins,
                operating_weeks_per_year,
                operating_hours_per_week,
                utilisation_rate,
            )
        results_dict[capacity_requirement] = results
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
    metadata = load_metadata_from_ats(
        args.guid,
        config["AZ_TABLE_ENDPOINT"],
        config["TABLE_NAME"],
        args.capacity_model_version,
    )
    assumptions = load_assumptions(args.path_to_assumptions_file)
    aggregations_path = create_aggregations_path(metadata)
    aae_aggregations = load_aae_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path
    )
    functional_areas_summarised = process_aae(aae_aggregations)
    aae_capacity_df = calculate_aae_capacity(functional_areas_summarised, assumptions)
    save_results_to_csv(aae_capacity_df, args.guid, capacity_conversion_runtime, "aae")


if __name__ == "__main__":
    sys.exit(main())
