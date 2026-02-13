# Command line argument - guid for the folder to convert to capacity
# Load the data from Azure
# Convert data into capacity - separate functions for:
# adult_minor_bays
# adult_major_beds
# resus_beds
# children_minor_bays
# children_major_beds
# sdec_spaces
# Output data into Excel spreadsheet together with all assumptions and original data


from nhpy.utils import configure_logging, get_logger
import pandas as pd
from nhpy.az import connect_to_container, load_parquet_file
import argparse
import sys
import os
from dotenv import load_dotenv
from logging import INFO

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


def calculate_prediction_intervals_and_mean(
    arrivals_column: pd.Series,
) -> dict[str, float]:
    """Calculate p10, p90 and mean for activity in each functional area

    Args:
        arrivals_column (pd.Series): Column with activity counts for each functional area

    Returns:
        dict[str, float]: Dictionary with p10, p90 and mean as keys
    """
    results_dict = {"mean": float(arrivals_column.mean())}
    results_dict["p10"] = float(arrivals_column.quantile(0.1))
    results_dict["p90"] = float(arrivals_column.quantile(0.9))
    return results_dict


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
    aae_summarised = {}
    for grouping in aae.index.unique(level="grouping"):
        aae_summarised[grouping] = calculate_prediction_intervals_and_mean(
            aae.loc[(slice(None), grouping), :]["arrivals"]
        )
    return aae_summarised


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


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    configure_logging(INFO)
    parser = argparse.ArgumentParser(
        description="Generate A&E capacity outputs given functional area aggregations of A&E activity"
    )
    parser.add_argument(
        "aggregations_path",
        help="Path to existing functional area aggregations \
        (e.g. 'functional-aggregations/CAPACITY_MODEL_VERSION/GUID/')",
    )
    args = parser.parse_args()
    load_dotenv()
    account_url = os.getenv("AZ_STORAGE_EP", "")
    results_container = os.getenv("AZ_STORAGE_RESULTS", "")
    aae_aggregations = load_aae_aggregations(
        account_url, results_container, args.aggregations_path
    )
    print(process_aae(aae_aggregations))


if __name__ == "__main__":
    sys.exit(main())
