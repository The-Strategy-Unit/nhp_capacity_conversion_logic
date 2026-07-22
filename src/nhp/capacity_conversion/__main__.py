import argparse
import sys
from datetime import datetime
from logging import INFO

import pandas as pd
from nhpy.utils import (
    configure_logging,
    get_logger,
)

from nhp.capacity_conversion.aae import calculate_aae_capacity
from nhp.capacity_conversion.config import ASSUMPTIONS_URL
from nhp.capacity_conversion.ip_daycase import calculate_daycase_capacity
from nhp.capacity_conversion.ip_maternity import (
    calculate_maternity_capacity,
    preprocess_ip_maternity_data,
)
from nhp.capacity_conversion.op import calculate_op_capacity
from nhp.capacity_conversion.utils import (
    create_aggregations_path,
    load_aggregations,
    load_assumptions,
    load_metadata_from_ats,
    process_activity_type,
    process_and_save_results_to_excel,
    validate_required_env_vars,
)

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
        help=f"Path to assumptions file (default: '{ASSUMPTIONS_URL}')",
        default=ASSUMPTIONS_URL,
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
    process_activity_type(
        "op", op_aggregations, calculate_op_capacity, assumptions, data_to_save
    )

    aae_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"], config["AZ_STORAGE_RESULTS"], aggregations_path, "aae"
    )
    process_activity_type(
        "aae", aae_aggregations, calculate_aae_capacity, assumptions, data_to_save
    )

    ip_daycase_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        aggregations_path,
        "ip_daycase",
    )
    process_activity_type(
        "ip_daycase",
        ip_daycase_aggregations,
        calculate_daycase_capacity,
        assumptions,
        data_to_save,
    )
    ip_maternity_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        aggregations_path,
        "ip_maternity",
    )
    process_activity_type(
        "ip_maternity",
        ip_maternity_aggregations,
        calculate_maternity_capacity,
        assumptions,
        data_to_save,
        preprocess=preprocess_ip_maternity_data,
    )

    process_and_save_results_to_excel(data_to_save)
    return 0


if __name__ == "__main__":
    sys.exit(main())
