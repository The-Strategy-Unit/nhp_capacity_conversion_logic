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


def convert_op_capacity(
    attendances: float,
    duration: float,
    dna_rate: float,
    dna_time: float,
    operational_hours: float,
    operational_weeks: float,
    utilisation_rate: float,
) -> float:
    """Formula used for converting all OP functional area activity to capacity requirements

    Args:
        attendances (float): Number of attendances
        duration (float): Indicative appointment duration (mins)
        dna_rate (float): Indicative DNA rate
        dna_time (float): Indicative DNA time consumed (mins)
        operational_hours (float): Room operational hours per week
        operational_weeks (float): Room operational weeks per year
        utilisation_rate (float): Room utilisation rate

    Returns:
        float: Calculated capacity requirement
    """
    return (((attendances * duration) + (dna_rate * attendances * dna_time)) / 60) / (
        operational_hours * operational_weeks * utilisation_rate
    )


def map_op_capacity_to_functional_area(capacity_requirement_string: str) -> str:
    """Alters string so that we can look up the correct functional area to use for
    each capacity requirement

    Args:
        capacity_requirement_string (str): Capacity requirement name

    Returns:
        str: Corresponding functional area name
    """
    capacity_requirement_string = capacity_requirement_string.replace(
        "op_", "outpatient_"
    )
    if "procedures" not in capacity_requirement_string:
        capacity_requirement_string += "_attendances"

    return capacity_requirement_string


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
    for capacity_requirement in [
        "op_first",
        "op_followup",
        "op_virtual",
        "op_procedures",
    ]:
        results = {}
        duration = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_duration_mins", "assumption_value"
            ],
        )
        dna_rate = cast(
            float,
            assumptions_df.at[capacity_requirement + "_dna_rate", "assumption_value"],
        )
        dna_time = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_dna_time_mins", "assumption_value"
            ],
        )
        operational_hours = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operational_hours", "assumption_value"
            ],
        )
        operational_weeks = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_operational_weeks", "assumption_value"
            ],
        )
        utilisation_rate = cast(
            float,
            assumptions_df.at[
                capacity_requirement + "_utilisation_rate", "assumption_value"
            ],
        )
        for value in ["p10", "mean", "p90"]:
            functional_area = map_op_capacity_to_functional_area(capacity_requirement)
            results[value] = convert_op_capacity(
                functional_areas_summarised[functional_area][value],
                duration,
                dna_rate,
                dna_time,
                operational_hours,
                operational_weeks,
                utilisation_rate,
            )
        results_dict[capacity_requirement] = results
    op_capacity = pd.DataFrame.from_dict(results_dict, orient="index")
    rename_dict = {
        "op_virtual": "op_virtual_consultation_rooms",
        "op_procedures": "op_procedure_rooms",
    }
    op_capacity.loc["op_consultation_rooms"] = (
        op_capacity.loc["op_first"] + op_capacity.loc["op_followup"]
    )
    return op_capacity.rename(index=rename_dict)


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
    functional_areas_summarised = summarise_functional_areas(op_aggregations)
    data_to_save["op_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    op_capacity_df = calculate_op_capacity(functional_areas_summarised, assumptions)
    data_to_save["op_capacity"] = op_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
