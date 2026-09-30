import argparse
import datetime
import logging
import os
import re
from collections.abc import Callable, Iterable, Mapping
from io import BytesIO
from typing import cast

import pandas as pd
from azure.data.tables import TableClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

from nhp.capacity_conversion.config import (
    AGGREGATION_SUBSETS,
    ASSUMPTIONS_URL,
    CATALOGUE_FIELDS,
    METADATA_FIELDS,
    MINIMUM_APP_VERSION,
    REQUIRED_METADATA_FIELDS,
    RESULTS_FILE_NAME,
    RUNTIME_FORMAT,
)
from nhp.capacity_conversion.results import process_and_save_results_to_excel

logger = logging.getLogger(__name__)

# Suppression methodology follows NHS England HES standard:
# https://digital.nhs.uk/data-and-information/publications/statistical/
# hospital-admitted-patient-care-activity/supporting-information#suppression-methodology
SUPPRESSION_THRESHOLD = 7  # suppress counts 1–7; round all others to nearest 5


def configure_logging(level: int = logging.INFO) -> None:
    """Configure concise CLI logging and suppress verbose Azure SDK logs."""
    logging.basicConfig(level=level, format="%(message)s", force=True)
    logging.getLogger("azure").setLevel(logging.WARNING)


def connect_to_container(
    account_url: str | None,
    container_name: str | None,
) -> ContainerClient:
    """Create an authenticated Azure Blob Storage container client."""
    if not account_url:
        raise ValueError("An account URL is required and cannot be empty")
    if not container_name:
        raise ValueError("A container name is required and cannot be empty")

    return ContainerClient(
        account_url=account_url,
        container_name=container_name,
        credential=DefaultAzureCredential(),
    )


def load_parquet_file(
    container_client: ContainerClient,
    path_to_file: str,
) -> pd.DataFrame:
    """Download a Parquet blob and load it into a DataFrame."""
    parquet_bytes = cast(
        bytes,
        container_client.get_blob_client(path_to_file).download_blob().readall(),
    )
    return pd.read_parquet(BytesIO(parquet_bytes), engine="pyarrow")


def get_baseline_activity(aggregations: pd.DataFrame) -> pd.DataFrame:
    """Extract baseline (model run 0) total activity per functional area.

    Applies NHS England HES suppression rules:
    - Values 1–7 are replaced with None (displayed as blank in Excel)
    - All other non-zero values are rounded to the nearest 5

    Args:
        aggregations (pd.DataFrame): Raw aggregations with model_run index,
            grouping and total columns

    Returns:
        pd.DataFrame: Baseline activity per grouping, suppressed and rounded
    """
    value_columns = aggregations.select_dtypes("number").columns.tolist()
    groupby_col = [
        name
        for name in aggregations.index.names
        if name not in ["model_run", "sitetret"]
    ]
    baseline = (
        aggregations.loc[aggregations.index.get_level_values("model_run") == 0, :]
        .groupby(groupby_col)[value_columns]
        .sum()
    )

    def _suppress_and_round(x: float) -> float | None:
        if 1 <= x <= SUPPRESSION_THRESHOLD:
            return None
        return round(x / 5) * 5

    return baseline.map(_suppress_and_round)


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
    """Loads assumptions for use in model. Defaults to assumptions published online at
    https://the-strategy-unit.github.io/open-plan-docs/reference/functional-area-catalogue/

    Args:
        path_to_csv (str): Path to assumptions csv file

    Returns:
        pd.DataFrame: Dataframe with assumption values and variable names
    """
    logger.info(f"Loading assumptions from {path_to_csv}...")
    return pd.read_csv(path_to_csv).set_index("Assumption ID")[["Value"]].sort_index()


_APP_VERSION_PATTERN = re.compile(r"v?(\d+(?:\.\d+)*)", re.IGNORECASE)


def parse_app_version(value: object) -> tuple[int, ...] | None:
    """Parse an app version such as "v6.0" into a tuple of integers.

    Returns None for anything that is not a numeric version, such as "dev".
    """
    match = _APP_VERSION_PATTERN.fullmatch(str(value).strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def is_supported_app_version(value: object) -> bool:
    """Return whether an app version is numeric and at least MINIMUM_APP_VERSION.

    Versions are compared numerically, so "v10.0" is newer than "v6.0". Values
    that cannot be parsed (e.g. "dev") are not supported.
    """
    parsed = parse_app_version(value)
    if parsed is None:
        return False
    width = max(len(parsed), len(MINIMUM_APP_VERSION))

    def pad(version: tuple[int, ...]) -> tuple[int, ...]:
        return version + (0,) * (width - len(version))

    return pad(parsed) >= pad(MINIMUM_APP_VERSION)


def connect_to_table(storage_endpoint: str, table_name: str) -> TableClient:
    """Create an authenticated Azure Table Storage client."""
    return TableClient(
        endpoint=storage_endpoint,
        table_name=table_name,
        credential=DefaultAzureCredential(),
    )


def entity_to_metadata(entity: Mapping) -> dict[str, str]:
    """Convert an Azure Table Storage entity into a metadata dictionary.

    Keeps only the fields in METADATA_FIELDS and converts every value to a string
    (datetimes become ISO 8601). The entity's RowKey is stored as "guid".
    """
    metadata = {}
    for key in METADATA_FIELDS:
        if key not in entity:
            continue
        value = entity[key]
        if isinstance(value, datetime.datetime):
            metadata[key] = value.isoformat()
        else:
            metadata[key] = str(value)
    metadata["guid"] = str(entity["RowKey"])
    return metadata


def metadata_problem(
    entity: Mapping,
    metadata: Mapping[str, str],
    dataset: str,
    required_fields: Iterable[str],
) -> str | None:
    """Describe why a model run cannot be used, or return None if it is valid."""
    missing = [
        field for field in required_fields if not str(metadata.get(field, "")).strip()
    ]
    if missing:
        return f"missing values for {', '.join(missing)}"
    if entity.get("PartitionKey") != dataset or metadata["dataset"] != dataset:
        return "dataset does not match the table partition"
    if not is_supported_app_version(metadata["app_version"]):
        return f"unsupported app_version {metadata['app_version']!r}"
    created = pd.to_datetime(
        metadata["create_datetime"], format="ISO8601", utc=True, errors="coerce"
    )
    if pd.isna(created):
        return f"invalid create_datetime {metadata['create_datetime']!r}"
    return None


def load_metadata_from_ats(
    dataset: str,
    guid: str,
    storage_endpoint: str,
    table_name: str,
) -> dict:
    """Loads metadata for scenario converted to functional area aggregations
    from Azure Table Storage

    Args:
        dataset (str): Dataset, used as the PartitionKey in the table
        guid (str): GUID for functional area aggregation
        storage_endpoint (str): Azure Table Storage endpoint, in format "https://{storage_account_name}.table.core.windows.net"
        table_name (str): Table name containing metadata for Functional Area Aggregations

    Returns:
        dict: Dictionary with metadata for given Functional Area aggregation

    Raises:
        ValueError: If the run is incomplete, belongs to a different dataset, or was
            produced by an unsupported app version (see MINIMUM_APP_VERSION)
    """
    table_client = connect_to_table(storage_endpoint, table_name)
    entity = table_client.get_entity(partition_key=dataset, row_key=guid)
    metadata = entity_to_metadata(entity)
    problem = metadata_problem(entity, metadata, dataset, REQUIRED_METADATA_FIELDS)
    if problem:
        raise ValueError(
            f"Model run {guid} in dataset {dataset} is not usable: {problem}"
        )
    return metadata


def load_functional_aggregations_from_ats(
    dataset: str,
    storage_endpoint: str,
    table_name: str,
) -> list[dict]:
    """Load the available functional aggregations for one dataset.

    Only the fields needed to select a model run are fetched. Runs that are
    incomplete or produced by an unsupported app version are left out.

    Args:
        dataset (str): Dataset, used as the PartitionKey in the table
        storage_endpoint (str): Azure Table Storage endpoint
        table_name (str): Table name containing metadata for Functional Area Aggregations

    Returns:
        list[dict]: Metadata (as strings, including "guid") for each usable run
    """
    table_client = connect_to_table(storage_endpoint, table_name)
    entities = table_client.query_entities(
        query_filter="PartitionKey eq @dataset",
        parameters={"dataset": dataset},
        select=["PartitionKey", "RowKey", *CATALOGUE_FIELDS],
    )
    runs = []
    ignored = 0
    for entity in entities:
        metadata = entity_to_metadata(entity)
        if metadata_problem(entity, metadata, dataset, CATALOGUE_FIELDS + ("guid",)):
            ignored += 1
        else:
            runs.append(metadata)
    if ignored:
        logger.info(
            "Ignored %d model runs in dataset %s that are incomplete or use an "
            "unsupported app version.",
            ignored,
            dataset,
        )
    return runs


def load_datasets_from_ats(storage_endpoint: str, table_name: str) -> list[str]:
    """List datasets that have at least one model run from a supported app version.

    Scans the table, but fetches only the partition key and app version columns.
    """
    table_client = connect_to_table(storage_endpoint, table_name)
    entities = table_client.list_entities(select=["PartitionKey", "app_version"])
    return sorted(
        {
            str(entity["PartitionKey"])
            for entity in entities
            if is_supported_app_version(entity.get("app_version"))
        }
    )


def create_aggregations_path(metadata: Mapping[str, str]) -> str:
    """Return the path to the functional area aggregations file for a model run."""
    return f"{metadata['aggregated_results_path'].rstrip('/')}/{RESULTS_FILE_NAME}"


def add_run_details(
    metadata: Mapping[str, str],
    capacity_model_version: str,
    **extra_details: str,
) -> dict[str, str]:
    """Return a copy of the metadata stamped with details of this conversion run."""
    run_metadata = dict(metadata)
    run_metadata["capacity_conversion_runtime"] = datetime.datetime.now(
        tz=datetime.UTC
    ).strftime(RUNTIME_FORMAT)
    run_metadata["capacity_model_version"] = capacity_model_version
    run_metadata.update(extra_details)
    return run_metadata


def validate_required_env_vars() -> dict:
    """
    Loads environment variables and ensures required variables are present.
    Raises EnvironmentError if any are missing or empty.
    Returns a dictionary of the validated variables.
    """

    load_dotenv()

    required_vars = [
        "AZ_STORAGE_EP",
        "AZ_STORAGE_RESULTS",
        "TABLE_NAME",
        "AZ_TABLE_ENDPOINT",
        "CAPACITY_MODEL_VERSION",
    ]

    values = {}
    missing = []

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        else:
            values[var] = value

    if missing:
        raise OSError(
            f"Missing required environment variables in .env: {', '.join(missing)}"
        )

    return values


def load_aggregations(
    account_url: str,
    results_container: str,
    aggregations_path: str,
) -> pd.DataFrame:
    """Loads aggregated data from Azure

    Args:
        account_url (str): Azure Storage account URL
        results_container (str): Azure Storage container name with results
        aggregations_path (str): Path to "folder" with data to load

    Returns:
        pd.DataFrame: Loads aggregated data
    """
    logger.info(f"Loading data from {aggregations_path}...")
    results_connection = connect_to_container(account_url, results_container)
    aggregations = load_parquet_file(results_connection, aggregations_path)
    return aggregations


def process_activity_type(
    name: str,
    aggregations: pd.DataFrame,
    calculate_fn: Callable,
    assumptions: pd.DataFrame,
    data_to_save: dict[str, pd.DataFrame | pd.Series],
    preprocess: Callable | None = None,
    include_baseline: bool = True,
) -> None:
    """Summarise functional areas, optionally extract baseline, and calculate capacity."""
    if preprocess is not None:
        if name in ["ip_wards", "ip_procedures_and_theatres"]:
            aggregations = preprocess(aggregations, assumptions)
        else:
            aggregations = preprocess(aggregations)
    # We exclude baseline (model run 0) from conversion to capacity
    functional_areas = aggregations.loc[
        aggregations.index.get_level_values("model_run") != 0, :
    ]
    data_to_save[f"{name}_fun_area_groupings"] = functional_areas
    if include_baseline:
        data_to_save[f"{name}_baseline"] = get_baseline_activity(aggregations)
    capacity_df = calculate_fn(functional_areas, assumptions)
    data_to_save[f"{name}_capacity"] = capacity_df


def run_single_activity_type(
    activity_type: str,
    calculate_fn: Callable,
    preprocess: Callable | None = None,
    include_baseline: bool = False,
) -> int:
    """CLI entry point for a single activity type.

    Handles argument parsing, metadata/assumptions loading, aggregation loading,
    optional preprocessing, capacity calculation, and Excel saving.
    """
    configure_logging(logging.INFO)

    parser = argparse.ArgumentParser(
        description=f"Generate {activity_type.upper()} capacity outputs given functional area aggregations of {activity_type.upper()} activity"
    )
    parser.add_argument(
        "dataset",
        help="Dataset of functional area aggregation to convert into capacity",
    )
    parser.add_argument(
        "guid",
        help="GUID of functional area aggregation to convert into capacity",
    )
    parser.add_argument(
        "--path_to_assumptions_file",
        help=f"Path to assumptions file (default: '{ASSUMPTIONS_URL}')",
        default=ASSUMPTIONS_URL,
    )
    parser.add_argument(
        "--sites",
        help="Sites to filter to (default: ALL). Sites should be supplied in the format SITE_A,SITE_B,SITE_C",
        default="ALL",
    )
    args = parser.parse_args()

    config = validate_required_env_vars()
    data_to_save = {}

    metadata = load_metadata_from_ats(
        args.dataset,
        args.guid,
        config["AZ_TABLE_ENDPOINT"],
        config["TABLE_NAME"],
    )
    run_metadata = add_run_details(
        metadata, config["CAPACITY_MODEL_VERSION"], sites=args.sites
    )
    data_to_save["metadata"] = pd.Series(run_metadata)

    assumptions = load_assumptions(args.path_to_assumptions_file)
    data_to_save["assumptions"] = assumptions
    aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        create_aggregations_path(metadata),
    )
    aggregations = filter_aggregations(aggregations, args.sites, activity_type)

    process_activity_type(
        name=activity_type,
        aggregations=aggregations,
        calculate_fn=calculate_fn,
        assumptions=assumptions,
        data_to_save=data_to_save,
        preprocess=preprocess,
        include_baseline=include_baseline,
    )

    process_and_save_results_to_excel(data_to_save)
    return 0


def validate_sites(aggregations: pd.DataFrame, sites: list[str]) -> None:
    """Validates that all supplied sites exist in the aggregations sitetret column

    Args:
        aggregations (pd.DataFrame): Aggregations by functional area, with sitetret column
        sites (list[str]): List of sites to validate

    Raises:
        ValueError: If any of the supplied sites are not present in the sitetret column
    """
    valid_sites = set(aggregations["sitetret"])
    invalid_sites = [site for site in sites if site not in valid_sites]

    if invalid_sites:
        raise ValueError(
            f"The following sites are not valid: {', '.join(invalid_sites)}"
        )


def filter_aggregations(
    aggregations: pd.DataFrame, sites: str, subset: str
) -> pd.DataFrame:
    """Filters aggregations by selected sites

    Args:
        aggregations (pd.DataFrame): Aggregations by functional area, with sitetret column
        sites (str): Sites to filter to.
        subset (str): Aggregation subset to filter to.

    Returns:
        pd.DataFrame: Filtered aggregations, with sitetret column removed
    """
    logger.info(f"Filtering to {subset}")
    functional_areas = AGGREGATION_SUBSETS[subset]
    available_functional_areas = set(aggregations["functional_area"])
    missing_functional_areas = set(functional_areas) - available_functional_areas
    if missing_functional_areas:
        logger.warning(
            f"Functional areas not found in aggregations: {sorted(missing_functional_areas)}"
        )
    aggregations = aggregations[aggregations["functional_area"].isin(functional_areas)]
    logger.info(f"Filtering by sites: {sites}")
    if sites != "ALL":
        sites_split = sites.upper().split(",")
        validate_sites(aggregations, sites_split)
        aggregations = aggregations[aggregations["sitetret"].isin(sites_split)]
    # collapse all remaining sites
    groupby_cols = [
        col
        for col in aggregations.select_dtypes(exclude=["number"]).columns.tolist()
        if col != "sitetret"
    ]
    aggregations = aggregations.groupby(["model_run"] + groupby_cols).sum(
        numeric_only=True
    )
    return aggregations
