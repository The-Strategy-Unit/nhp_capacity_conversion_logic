from nhpy.utils import (
    configure_logging,
    get_logger,
)
import pandas as pd
from nhp.capacity_conversion.utils import (
    load_assumptions,
    save_results_to_excel,
    calculate_prediction_intervals_and_mean,
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


def summarise_op_functional_areas(op_aggregations: pd.DataFrame) -> dict[str, dict]:
    """Process OP data ready for conversion to capacity

    Args:
        op_aggregations (pd.DataFrame): Dataframe with OP functional areas and activity

    Returns:
        dict[str, dict]: Dictionary with p10, p90 and mean for each functional area
    """
    op_aggregations = (
        op_aggregations.reset_index()
        .groupby(["model_run", "grouping"])
        .sum(numeric_only=True)
    )
    op = op_aggregations.drop([0], axis=0)  # model_run 0 is baseline
    functional_areas_summarised = {}
    for grouping in op.index.unique(level="grouping"):
        functional_areas_summarised[grouping] = calculate_prediction_intervals_and_mean(
            op.loc[(slice(None), grouping), :]["arrivals"]
        )
    return functional_areas_summarised


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
    op_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "op"
    )
    functional_areas_summarised = summarise_op_functional_areas(op_aggregations)
    return functional_areas_summarised
    # data_to_save["op_functional_areas"] = pd.DataFrame.from_dict(
    #     functional_areas_summarised, orient="index"
    # )
    # op_capacity_df = calculate_op_capacity(functional_areas_summarised, assumptions)
    # data_to_save["op_capacity"] = op_capacity_df
    # save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
