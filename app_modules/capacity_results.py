"""Capacity results display and download Shiny module."""

from collections.abc import Callable

import pandas as pd
from shiny import Inputs, Outputs, Session, module, reactive, render, ui

type CapacityResults = dict[str, pd.DataFrame | pd.Series]


@module.ui
def capacity_results_ui():
    """Build the capacity estimates card."""
    return ui.card(
        ui.card_header("Capacity estimates"),
        ui.output_ui("status"),
        ui.output_data_frame("estimates"),
        ui.div(
            ui.download_button(
                "download",
                "Download Estimates",
                class_="btn-primary btn-sm",
            ),
            class_="d-flex justify-content-end mt-3",
        ),
    )


@module.server
def capacity_results_server(
    input: Inputs,
    output: Outputs,
    session: Session,
    capacity_results: reactive.Value[CapacityResults | None],
    *,
    activity_types: tuple[str, ...],
    create_workbook: Callable[[CapacityResults], bytes],
    require_capacity_results: Callable[[CapacityResults | None], CapacityResults],
    summarise_model_runs: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    """Render and export generated capacity estimates."""

    @render.ui
    def status():
        if capacity_results.get() is None:
            return ui.p(
                "Select a model run and generate estimates to view the results.",
                class_="text-muted",
            )
        return None

    @render.data_frame
    def estimates():
        data_to_save = require_capacity_results(capacity_results.get())
        estimates_to_display = []

        for activity_type in activity_types:
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
    def download():
        yield create_workbook(require_capacity_results(capacity_results.get()))
