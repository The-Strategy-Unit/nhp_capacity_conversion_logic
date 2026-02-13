import pandas as pd
from nhpy.utils import get_logger

logger = get_logger()


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


def save_results_to_csv(data, filename):
    data.to_csv(f"{filename}.csv")
