"""Model-run selection and capacity generation Shiny module."""

import logging
from collections.abc import Callable
from typing import Protocol

import pandas as pd
from shiny import Inputs, Outputs, Session, module, reactive, ui

logger = logging.getLogger(__name__)

type CapacityResults = dict[str, pd.DataFrame | pd.Series]
type UserGroups = list[str] | None


class DatasetLoader(Protocol):
    """Load the datasets available to the current user."""

    def __call__(
        self,
        groups: UserGroups,
        *,
        is_local: bool,
    ) -> list[str]: ...


class CatalogueLoader(Protocol):
    """Load a dataset's model-run catalogue for the current user."""

    def __call__(
        self,
        dataset: str,
        groups: UserGroups,
        *,
        is_local: bool,
    ) -> pd.DataFrame: ...


class AuthorisedMetadataLoader(Protocol):
    """Load metadata for an authorised model-run selection."""

    def __call__(
        self,
        *,
        dataset: str,
        scenario: str,
        guid: str,
        groups: UserGroups,
        is_local: bool,
    ) -> dict: ...


@module.ui
def model_run_ui():
    """Build the model-run selectors and generation action."""
    return ui.card(
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
    )


@module.server
def model_run_server(
    input: Inputs,
    output: Outputs,
    session: Session,
    *,
    empty_catalogue: pd.DataFrame,
    functional_aggregation_choices: Callable[[pd.DataFrame], dict[str, str]],
    groups: UserGroups,
    is_local_development: Callable[[], bool],
    load_available_datasets: DatasetLoader,
    load_authorised_metadata: AuthorisedMetadataLoader,
    load_catalogue: CatalogueLoader,
    load_capacity_results: Callable[[dict], CapacityResults],
) -> reactive.Value[CapacityResults | None]:
    """Coordinate selection and return generated capacity results."""
    datasets: reactive.Value[list[str]] = reactive.value([])
    catalogue: reactive.Value[pd.DataFrame] = reactive.value(empty_catalogue)
    capacity_results: reactive.Value[CapacityResults | None] = reactive.value(None)

    @reactive.effect
    def load_datasets() -> None:
        try:
            datasets.set(
                load_available_datasets(
                    groups,
                    is_local=is_local_development(),
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
    def load_model_run_catalogue() -> None:
        selected_dataset = input.dataset()
        if not selected_dataset:
            catalogue.set(empty_catalogue)
            return

        try:
            catalogue.set(
                load_catalogue(
                    selected_dataset,
                    groups,
                    is_local=is_local_development(),
                )
            )
        except Exception:
            logger.exception("Unable to load the model-run catalogue.")
            catalogue.set(empty_catalogue)
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
        choices = {"": "Select a model run"} | functional_aggregation_choices(
            matching_aggregations
        )
        ui.update_select("model_run", choices=choices, selected="")

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
                metadata = load_authorised_metadata(
                    dataset=selected_dataset,
                    scenario=selected_scenario,
                    guid=selected_guid,
                    groups=groups,
                    is_local=is_local_development(),
                )
                progress.set(1, message="Generating capacity estimates")
                capacity_results.set(load_capacity_results(metadata))
                progress.set(2)
        except Exception:
            logger.exception("Unable to generate capacity estimates.")
            ui.notification_show(
                "Capacity estimates could not be generated. Please try again later.",
                type="error",
                duration=None,
            )

    return capacity_results
