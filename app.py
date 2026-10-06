import logging
import os
from io import BytesIO
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import pandas as pd
from htmltools import HTMLDependency
from shiny import App, Inputs, Outputs, Session, reactive, render, req, ui

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

logger = logging.getLogger(__name__)


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


APP_TITLE = "OpenPlan Capacity Model"
CAPACITY_MODEL_VERSION = _required_environment_variable("CAPACITY_MODEL_VERSION")
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
    ui.tags.header(
        ui.div(
            ui.span(APP_TITLE, class_="fs-4 fw-semibold"),
            ui.div(
                ui.img(
                    src="strategy-unit-nhs-logo.png",
                    alt="The Strategy Unit and NHS",
                    class_="brand-logo-image",
                ),
                class_="brand-logo-frame",
            ),
            class_=(
                "container d-flex flex-column flex-sm-row align-items-start "
                "align-items-sm-center justify-content-sm-between gap-2 py-3"
            ),
        ),
        class_="border-bottom bg-white",
    ),
    ui.div(
        ui.div(
            ui.h1("Capacity estimates", class_="mb-0"),
            ui.input_action_button(
                "feedback",
                "Feedback",
                class_="btn-primary btn-sm",
            ),
            class_=(
                "d-flex flex-column flex-sm-row align-items-sm-center "
                "justify-content-between gap-3 mb-3"
            ),
        ),
        ui.card(
            ui.card_header("Select model run"),
            ui.layout_columns(
                ui.input_select(
                    "dataset",
                    "Dataset",
                    {"": "Select a dataset"},
                ),
                ui.input_select(
                    "scenario",
                    "Scenario",
                    {"": "Select a scenario"},
                ),
                ui.input_select(
                    "model_run",
                    "Model run time",
                    {"": "Select a model run"},
                ),
                col_widths=(4, 4, 4),
            ),
            ui.div(
                ui.input_action_button(
                    "generate",
                    "Generate capacity estimates",
                    class_="btn-primary btn-sm",
                ),
                class_="d-flex justify-content-end",
            ),
            class_="mb-3",
        ),
        ui.card(
            ui.card_header("Capacity estimates"),
            ui.output_ui("results_status"),
            ui.output_data_frame("estimates"),
            ui.div(
                ui.download_button(
                    "download_estimates",
                    "Download Estimates",
                    class_="btn-primary btn-sm",
                ),
                class_="d-flex justify-content-end mt-3",
            ),
        ),
        class_="container-fluid py-4",
    ),
    title=APP_TITLE,
    theme=ui.Theme.from_brand(__file__),
)


def server(input: Inputs, output: Outputs, session: Session) -> None:
    datasets: reactive.Value[list[str]] = reactive.value([])
    catalogue = reactive.value(_catalogue_frame([]))
    capacity_results: reactive.Value[dict[str, pd.DataFrame | pd.Series] | None] = (
        reactive.value(None)
    )

    @reactive.effect
    def load_datasets() -> None:
        try:
            datasets.set(
                _available_datasets(
                    session.groups,
                    is_local=_is_local_development(),
                )
            )
        except Exception:
            logger.exception("Unable to load the list of datasets.")
            datasets.set([])
            ui.notification_show(
                "Datasets are temporarily unavailable. Please try again later.",
                type="error",
                duration=None,
            )

    @reactive.effect
    def load_catalogue() -> None:
        selected_dataset = input.dataset()
        if not selected_dataset:
            catalogue.set(_catalogue_frame([]))
            return

        try:
            if not _may_access_dataset(
                selected_dataset,
                session.groups,
                is_local=_is_local_development(),
            ):
                raise PermissionError("The selected dataset is not available.")
            model_runs = load_functional_aggregations_from_ats(
                selected_dataset,
                _required_environment_variable("AZ_TABLE_ENDPOINT"),
                _required_environment_variable("TABLE_NAME"),
            )
            catalogue.set(_catalogue_frame(model_runs))
        except Exception:
            logger.exception("Unable to load the model-run catalogue.")
            catalogue.set(_catalogue_frame([]))
            ui.notification_show(
                "Model runs are temporarily unavailable. Please try again later.",
                type="error",
                duration=None,
            )

    @reactive.effect
    def update_datasets() -> None:
        choices = {"": "Select a dataset"} | {
            dataset: dataset for dataset in datasets.get()
        }
        ui.update_select("dataset", choices=choices, selected="")

    @reactive.effect
    def update_scenarios() -> None:
        selected_dataset = input.dataset()
        functional_aggregations = catalogue.get()
        scenarios = sorted(
            functional_aggregations.loc[
                functional_aggregations["dataset"].eq(selected_dataset),
                "scenario",
            ].unique()
        )
        choices = {"": "Select a scenario"} | {
            scenario: scenario for scenario in scenarios
        }
        ui.update_select("scenario", choices=choices, selected="")

    @reactive.effect
    def update_model_runs() -> None:
        selected_dataset = input.dataset()
        selected_scenario = input.scenario()
        functional_aggregations = catalogue.get()
        matching_aggregations = functional_aggregations.loc[
            functional_aggregations["dataset"].eq(selected_dataset)
            & functional_aggregations["scenario"].eq(selected_scenario)
        ]
        choices = {"": "Select a model run"} | _functional_aggregation_choices(
            matching_aggregations
        )
        ui.update_select("model_run", choices=choices, selected="")

    @reactive.effect
    @reactive.event(input.feedback)
    def show_feedback_form() -> None:
        feedback_url = urlparse(FEEDBACK_FORM_URL or "")
        if feedback_url.scheme != "https" or not feedback_url.netloc:
            ui.modal_show(
                ui.modal(
                    ui.p("The feedback form is not currently available."),
                    title="Feedback",
                    easy_close=True,
                    footer=ui.modal_button(
                        "Close",
                        class_="btn-primary btn-sm",
                    ),
                )
            )
            return

        ui.modal_show(
            ui.modal(
                ui.tags.iframe(
                    src=FEEDBACK_FORM_URL,
                    title="Feedback form",
                    style="width: 100%; height: 70vh; border: 0;",
                ),
                title="Feedback",
                size="l",
                easy_close=True,
                footer=ui.modal_button(
                    "Close",
                    class_="btn-primary btn-sm",
                ),
            )
        )

    @reactive.effect
    @reactive.event(input.generate)
    def generate_capacity_results() -> None:
        selected_dataset = input.dataset()
        selected_scenario = input.scenario()
        selected_guid = input.model_run()
        if not selected_dataset or not selected_scenario or not selected_guid:
            ui.notification_show(
                "Select a dataset, scenario and model run before generating results.",
                type="warning",
            )
            return

        capacity_results.set(None)
        try:
            with ui.Progress(min=0, max=2) as progress:
                progress.set(0, message="Checking model-run access")
                metadata = _load_authorised_metadata(
                    dataset=selected_dataset,
                    scenario=selected_scenario,
                    guid=selected_guid,
                    groups=session.groups,
                    is_local=_is_local_development(),
                )
                progress.set(1, message="Generating capacity estimates")
                capacity_results.set(_load_capacity_results(metadata))
                progress.set(2)
        except Exception:
            logger.exception("Unable to generate capacity estimates.")
            ui.notification_show(
                "Capacity estimates could not be generated. Please try again later.",
                type="error",
                duration=None,
            )

    @render.ui
    def results_status():
        if capacity_results.get() is None:
            return ui.p(
                "Select a model run and generate estimates to view the results.",
                class_="text-muted",
            )
        return None

    @render.data_frame
    def estimates():
        data_to_save = _require_capacity_results(capacity_results.get())
        estimates_to_display = []

        for activity_type in ACTIVITY_TYPES:
            capacity_data = data_to_save[f"{activity_type}_capacity"]
            if not isinstance(capacity_data, pd.DataFrame):
                raise TypeError("Capacity results must be a DataFrame.")
            capacity_summary = summarise_model_runs(capacity_data).reset_index()
            capacity_summary.insert(0, "activity_type", activity_type)
            estimates_to_display.append(capacity_summary)

        return render.DataTable(
            pd.concat(estimates_to_display, ignore_index=True),
            width="100%",
            summary=False,
        )

    @render.download_button(
        filename="capacity_conversion_results.xlsx",
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    def download_estimates():
        yield _create_workbook(_require_capacity_results(capacity_results.get()))


app = App(
    app_ui,
    server,
    static_assets=STATIC_ASSETS_DIR,
)
