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
from nhpy.az import connect_to_container, load_parquet_file
import argparse
import sys
import os
from dotenv import load_dotenv
from logging import INFO

logger = get_logger()


def load_aae_aggregations(account_url, results_container, aggregations_path):
    logger.info(f"Loading A&E data from {aggregations_path}...")
    results_connection = connect_to_container(account_url, results_container)
    aae_aggregations = load_parquet_file(
        results_connection, f"{aggregations_path}/aae.parquet"
    )
    return aae_aggregations


def process_aae(aae_aggregations):
    # TODO: Mapping 'unknown' to 'minor' is a temporary workaround. See issue #6
    aae = aae_aggregations.drop([0], axis=0)
    return aae


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
