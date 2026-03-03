import pandas as pd
from nhpy.utils import get_logger
import os

logger = get_logger()


def calculate_prediction_intervals_and_mean(
    activity_column: pd.Series,
) -> dict[str, float]:
    """Calculate p10, p90 and mean for activity in each functional area

    Args:
        activity_column (pd.Series): Column with activity counts for each functional area

    Returns:
        dict[str, float]: Dictionary with p10, p90 and mean as keys
    """
    results_dict = {"mean": float(activity_column.mean())}
    results_dict["p10"] = float(activity_column.quantile(0.1))
    results_dict["p90"] = float(activity_column.quantile(0.9))
    return results_dict


def load_assumptions(path_to_csv: str) -> pd.DataFrame:
    """Loads assumptions for use in model
    TODO: #7 Currently loads from CSV but later we should allow users to set in another way

    Args:
        path_to_csv (str): Path to assumptions csv file

    Returns:
        pd.DataFrame: Dataframe with assumption values and variable names
    """
    logger.info(f"Loading assumptions from {path_to_csv}...")
    return pd.read_csv(path_to_csv).set_index("assumption_name").sort_index()


def save_results_to_csv(
    data: pd.DataFrame,
    guid: str,
    capacity_conversion_runtime: str,
    activity_type: str,
) -> None:
    """Saves results as CSV files to filepath "results/GUID/CAPACITY_CONVERSION_RUNTIME/"

    Args:
        data (pd.DataFrame): Data to be saved
        guid (str): GUID for functional aggregations used for capacity conversion
        capacity_conversion_runtime (str): Runtime of capacity conversion
        activity_type (str): Activity type: one of aae, op, ip
    """
    directory = os.path.join("results", guid, capacity_conversion_runtime)
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, f"{activity_type}.csv")
    data.to_csv(filepath)
    logger.info(f"💾 Results saved to {filepath}")
