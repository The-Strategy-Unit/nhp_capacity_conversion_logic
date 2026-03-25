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
from nhp.capacity_conversion.op import calculate_op_capacity
from nhp.capacity_conversion.aae import calculate_aae_capacity, map_unknown
from nhp.capacity_conversion.ip_daycase import calculate_ip_daycase_capacity
import argparse
import sys
from logging import INFO
from datetime import datetime

logger = get_logger()


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    configure_logging(INFO)
    capacity_conversion_runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Generate capacity outputs for all available activity types"
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
    # OP
    op_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "op"
    )
    functional_areas_summarised = summarise_functional_areas(op_aggregations)
    data_to_save["op_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    op_capacity_df = calculate_op_capacity(functional_areas_summarised, assumptions)
    data_to_save["op_capacity"] = op_capacity_df
    # AAE
    aae_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "aae"
    )
    aae_aggregations.loc[:, "grouping"] = map_unknown(aae_aggregations["grouping"])
    functional_areas_summarised = summarise_functional_areas(aae_aggregations)
    data_to_save["aae_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    aae_capacity_df = calculate_aae_capacity(functional_areas_summarised, assumptions)
    data_to_save["aae_capacity"] = aae_capacity_df
    # ip_daycase
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
