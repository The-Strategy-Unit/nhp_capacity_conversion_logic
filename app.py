import os
from io import BytesIO
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import pandas as pd
from htmltools import HTMLDependency
from shiny import App, Inputs, Outputs, Session, req, ui

from app_modules import (
    app_header_ui,
    capacity_results_server,
    capacity_results_ui,
    feedback_server,
    feedback_ui,
    model_run_server,
    model_run_ui,
    page_heading_ui,
)
from nhp.capacity_conversion.aae import calculate_aae_capacity
from nhp.capacity_conversion.config import ACTIVITY_TYPES, ASSUMPTIONS_URL
from nhp.capacity_conversion.ip_daycase import calculate_daycase_capacity
from nhp.capacity_conversion.ip_maternity import (
    calculate_maternity_capacity,
    preprocess_ip_maternity_data,
)
from nhp.capacity_conversion.ip_theatres import (
    calculate_ip_theatres_capacity,
    preprocess_ip_theatres_data,
)
from nhp.capacity_conversion.ip_wards import (
    calculate_ip_wards_capacity,
    preprocess_ip_wards_data,
)
from nhp.capacity_conversion.op import calculate_op_capacity
from nhp.capacity_conversion.results import (
    process_and_save_results_to_excel,
    summarise_model_runs,
)
from nhp.capacity_conversion.utils import (
    add_run_details,
    create_aggregations_path,
    filter_aggregations,
    load_aggregations,
    load_assumptions,
    load_datasets_from_ats,
    load_functional_aggregations_from_ats,
    load_metadata_from_ats,
    process_activity_type,
)


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _required_https_environment_variable(name: str) -> str:
    value = _required_environment_variable(name)
    try:
        parsed_url = urlparse(value)
        valid_url = (
            parsed_url.scheme.casefold() == "https"
            and parsed_url.hostname is not None
            and parsed_url.username is None
            and parsed_url.password is None
        )
    except ValueError:
        valid_url = False
    if not valid_url:
        raise RuntimeError(f"{name} must be a valid HTTPS URL")
    return value


APP_TITLE = "OpenPlan Capacity Model"
CAPACITY_MODEL_VERSION = _required_environment_variable("CAPACITY_MODEL_VERSION")
DOCUMENTATION_URL = _required_https_environment_variable("DOCUMENTATION_URL")
ALL_SITES = "ALL"
SITES = {activity_type: ALL_SITES for activity_type in ACTIVITY_TYPES}
PRIVILEGED_GROUPS = frozenset({"nhp_devs", "nhp_power_users"})
PROVIDER_GROUP_PREFIX = "nhp_provider_"
CATALOGUE_COLUMNS = (
    "guid",
    "dataset",
    "scenario",
    "create_datetime",
)
FEEDBACK_MODULE_ID = "feedback"
MODEL_RUN_MODULE_ID = "model_run"
CAPACITY_RESULTS_MODULE_ID = "capacity_results"

FEEDBACK_FORM_URL = os.getenv("FEEDBACK_FORM_URL")
STATIC_ASSETS_DIR = Path(__file__).parent / "www"
FAVICON_DEPENDENCY = HTMLDependency(
    name="strategy-unit-favicon",
    version="1.0.0",
    head=ui.tags.link(rel="icon", type="image/x-icon", href="favicon.ico"),
)

CAPACITY_CALCULATIONS = {
    "aae": calculate_aae_capacity,
    "ip_daycase": calculate_daycase_capacity,
    "ip_maternity": calculate_maternity_capacity,
    "ip_procedures_and_theatres": calculate_ip_theatres_capacity,
    "ip_wards": calculate_ip_wards_capacity,
    "op": calculate_op_capacity,
}

CAPACITY_PREPROCESSORS = {
    "ip_maternity": preprocess_ip_maternity_data,
    "ip_procedures_and_theatres": preprocess_ip_theatres_data,
    "ip_wards": preprocess_ip_wards_data,
}


def _catalogue_frame(model_runs: list[dict]) -> pd.DataFrame:
    """Return model-run metadata in a selection-ready frame.

    The package has already validated each run (required fields, app version and
    dataset), so this only shapes the data and parses the ISO 8601 timestamps.

    Args:
        model_runs (list[dict]): List of model runs, from load_functional_aggregations_from_ats

    Returns:
        pd.DataFrame: Formatted dataframe with data for selection dropdowns.
    """
    if not model_runs:
        return pd.DataFrame(columns=CATALOGUE_COLUMNS)

    catalogue = pd.DataFrame(model_runs)
    missing_columns = set(CATALOGUE_COLUMNS).difference(catalogue.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Catalogue is missing required columns: {missing}")

    catalogue = catalogue.loc[:, list(CATALOGUE_COLUMNS)].copy()
    catalogue["create_datetime"] = pd.to_datetime(
        catalogue["create_datetime"],
        format="ISO8601",
        utc=True,
    )
    return catalogue


def _is_local_development() -> bool:
    """Return whether the app is running outside Posit Connect."""
    return os.getenv("POSIT_PRODUCT") != "CONNECT"


def _is_privileged(groups: list[str] | None, *, is_local: bool) -> bool:
    """Return whether the user may access every dataset."""
    return is_local or bool(set(groups or []).intersection(PRIVILEGED_GROUPS))


def _provider_datasets(groups: list[str] | None) -> set[str]:
    """Return the datasets a provider user is entitled to through their groups."""
    return {
        group.removeprefix(PROVIDER_GROUP_PREFIX)
        for group in groups or []
        if group.startswith(PROVIDER_GROUP_PREFIX) and group != PROVIDER_GROUP_PREFIX
    }


def _may_access_dataset(
    dataset: str,
    groups: list[str] | None,
    *,
    is_local: bool,
) -> bool:
    """Apply dataset-entitlement rules for a single dataset."""
    return _is_privileged(groups, is_local=is_local) or dataset in _provider_datasets(
        groups
    )


def _available_datasets(groups: list[str] | None, *, is_local: bool) -> list[str]:
    """List the datasets the user may choose from.

    Privileged users see every dataset with a supported model run. Provider users
    see only the datasets named by their groups, so other partitions are never
    queried on their behalf.
    """
    if _is_privileged(groups, is_local=is_local):
        return load_datasets_from_ats(
            _required_environment_variable("AZ_TABLE_ENDPOINT"),
            _required_environment_variable("TABLE_NAME"),
        )
    return sorted(_provider_datasets(groups))


def _load_catalogue(
    dataset: str,
    groups: list[str] | None,
    *,
    is_local: bool,
) -> pd.DataFrame:
    """Load the model-run catalogue after checking dataset entitlement."""
    if not _may_access_dataset(dataset, groups, is_local=is_local):
        raise PermissionError("The selected dataset is not available.")
    model_runs = load_functional_aggregations_from_ats(
        dataset,
        _required_environment_variable("AZ_TABLE_ENDPOINT"),
        _required_environment_variable("TABLE_NAME"),
    )
    return _catalogue_frame(model_runs)


def _functional_aggregation_choices(
    functional_aggregations: pd.DataFrame,
) -> dict[str, str]:
    """Create newest-first GUID-to-label choices for a model-run dropdown."""
    choices: dict[str, str] = {}
    ordered_aggregations = functional_aggregations.sort_values(
        "create_datetime",
        ascending=False,
    )
    for _, functional_aggregation in ordered_aggregations.iterrows():
        create_datetime = cast(
            pd.Timestamp,
            functional_aggregation["create_datetime"],
        )
        guid = cast(str, functional_aggregation["guid"])
        run_time = create_datetime.strftime("%d %b %Y, %H:%M UTC")
        choices[guid] = run_time
    return choices


def _load_authorised_metadata(
    *,
    dataset: str,
    scenario: str,
    guid: str,
    groups: list[str] | None,
    is_local: bool,
) -> dict:
    """Check the user may load the selected run, then fetch its metadata.

    Entitlement is checked before the table is queried. The metadata is then
    re-fetched and validated (including the app-version rule), and must match the
    dropdown selection, so a tampered or stale selection is rejected.
    """
    if not _may_access_dataset(dataset, groups, is_local=is_local):
        raise PermissionError("The selected model run is not available.")

    metadata = load_metadata_from_ats(
        dataset,
        guid,
        _required_environment_variable("AZ_TABLE_ENDPOINT"),
        _required_environment_variable("TABLE_NAME"),
    )
    if (
        metadata["dataset"] != dataset
        or metadata["scenario"] != scenario
        or metadata["guid"] != guid
    ):
        raise PermissionError("The selected model run is not available.")
    return metadata


def _load_capacity_results(
    metadata: dict,
) -> dict[str, pd.DataFrame | pd.Series]:
    storage_endpoint = _required_environment_variable("AZ_STORAGE_EP")
    results_container = _required_environment_variable("AZ_STORAGE_RESULTS")
    run_metadata = add_run_details(
        metadata,
        CAPACITY_MODEL_VERSION,
        ip_sites=ALL_SITES,
        op_sites=ALL_SITES,
        aae_sites=ALL_SITES,
    )

    assumptions = load_assumptions(ASSUMPTIONS_URL)
    data_to_save: dict[str, pd.DataFrame | pd.Series] = {
        "metadata": pd.Series(run_metadata),
        "assumptions": assumptions,
    }

    # The aggregations file is the same for every activity type, so download it
    # once and filter it per activity type.
    all_aggregations = load_aggregations(
        storage_endpoint,
        results_container,
        create_aggregations_path(metadata),
    )
    for activity_type in ACTIVITY_TYPES:
        aggregations = filter_aggregations(
            all_aggregations,
            SITES[activity_type],
            activity_type,
        )
        process_activity_type(
            activity_type,
            aggregations,
            CAPACITY_CALCULATIONS[activity_type],
            assumptions,
            data_to_save,
            preprocess=CAPACITY_PREPROCESSORS.get(activity_type),
        )

    return data_to_save


def _create_workbook(data_to_save: dict[str, pd.DataFrame | pd.Series]) -> bytes:
    workbook = BytesIO()
    process_and_save_results_to_excel(data_to_save, destination=workbook)
    return workbook.getvalue()


def _require_capacity_results(
    data_to_save: dict[str, pd.DataFrame | pd.Series] | None,
) -> dict[str, pd.DataFrame | pd.Series]:
    """Silently suspend an output until capacity results are available."""
    if data_to_save is None:
        req(False)
        raise RuntimeError("Capacity results are not available.")
    return data_to_save


app_ui = ui.page_fluid(
    FAVICON_DEPENDENCY,
    ui.head_content(ui.include_css(STATIC_ASSETS_DIR / "app.css")),
    app_header_ui(APP_TITLE),
    ui.div(
        page_heading_ui(
            DOCUMENTATION_URL,
            feedback_ui(FEEDBACK_MODULE_ID),
        ),
        model_run_ui(MODEL_RUN_MODULE_ID),
        capacity_results_ui(CAPACITY_RESULTS_MODULE_ID),
        class_="container-fluid py-4",
    ),
    title=APP_TITLE,
    theme=ui.Theme.from_brand(__file__),
)


def server(input: Inputs, output: Outputs, session: Session) -> None:
    feedback_server(
        FEEDBACK_MODULE_ID,
        feedback_form_url=FEEDBACK_FORM_URL,
    )
    capacity_results = model_run_server(
        MODEL_RUN_MODULE_ID,
        empty_catalogue=_catalogue_frame([]),
        functional_aggregation_choices=_functional_aggregation_choices,
        groups=session.groups,
        is_local_development=_is_local_development,
        load_available_datasets=_available_datasets,
        load_authorised_metadata=_load_authorised_metadata,
        load_catalogue=_load_catalogue,
        load_capacity_results=_load_capacity_results,
    )
    capacity_results_server(
        CAPACITY_RESULTS_MODULE_ID,
        capacity_results,
        activity_types=ACTIVITY_TYPES,
        create_workbook=_create_workbook,
        require_capacity_results=_require_capacity_results,
        summarise_model_runs=summarise_model_runs,
    )


app = App(
    app_ui,
    server,
    static_assets=STATIC_ASSETS_DIR,
)
