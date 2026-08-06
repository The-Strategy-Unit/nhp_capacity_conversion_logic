import os
from datetime import datetime
from io import BytesIO
from pathlib import PurePosixPath

import pandas as pd
from nhpy.az import connect_to_container, load_parquet_file
from shiny import App, Inputs, Outputs, Session, reactive, render, ui

from nhp.capacity_conversion.aae import calculate_aae_capacity
from nhp.capacity_conversion.config import ASSUMPTIONS_URL
from nhp.capacity_conversion.ip_daycase import calculate_daycase_capacity
from nhp.capacity_conversion.op import calculate_op_capacity
from nhp.capacity_conversion.utils import (
    load_assumptions,
    process_activity_type,
    summarise_model_runs,
)

CAPACITY_CALCULATIONS = {
    "aae": calculate_aae_capacity,
    "ip_daycase": calculate_daycase_capacity,
    "op": calculate_op_capacity,
}


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_capacity_results() -> tuple[str, dict[str, pd.DataFrame | pd.Series]]:
    blob_name = _required_environment_variable("AZ_FUNC_AGG_BLOB_PATH")
    blob_path = PurePosixPath(blob_name)
    path_parts = blob_path.parts

    if len(path_parts) < 4 or path_parts[-4] != "functional-aggregations":
        raise ValueError(
            "AZ_FUNC_AGG_BLOB_PATH must have the form "
            "functional-aggregations/<version>/<guid>/<activity_type>.parquet"
        )

    activity_type = blob_path.stem
    try:
        calculate_capacity = CAPACITY_CALCULATIONS[activity_type]
    except KeyError as error:
        supported_types = ", ".join(sorted(CAPACITY_CALCULATIONS))
        raise ValueError(
            f"Unsupported aggregation type '{activity_type}'. "
            f"Expected one of: {supported_types}."
        ) from error

    results_connection = connect_to_container(
        _required_environment_variable("AZ_STORAGE_EP"),
        _required_environment_variable("AZ_STORAGE_RESULTS"),
    )
    aggregations = load_parquet_file(results_connection, blob_name)
    assumptions = load_assumptions(ASSUMPTIONS_URL)

    data_to_save: dict[str, pd.DataFrame | pd.Series] = {
        "metadata": pd.Series(
            {
                "guid": path_parts[-2],
                "capacity_model_version": path_parts[-3],
                "capacity_conversion_runtime": datetime.now().strftime("%Y%m%d_%H%M%S"),
            }
        ),
        "assumptions": assumptions,
    }
    process_activity_type(
        activity_type,
        aggregations,
        calculate_capacity,
        assumptions,
        data_to_save,
    )
    return activity_type, data_to_save


def _create_workbook(data_to_save: dict[str, pd.DataFrame | pd.Series]) -> bytes:
    workbook = BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        for sheet_name, data in data_to_save.items():
            if isinstance(data, pd.DataFrame) and "model_run" in data.index.names:
                data = summarise_model_runs(data)
            pd.DataFrame(data).reset_index().to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
            )
    return workbook.getvalue()


app_ui = ui.page_fluid(
    ui.div(
        ui.tags.a(
            " Feedback",
            href=os.getenv("FEEDBACK_FORM_URL", "#"),
            target="_blank",
            class_="btn btn-primary",
        ),
        class_="d-flex justify-content-end border-bottom bg-light py-2",
    ),
    ui.div(
        ui.h1("Capacity Conversion Estimates", class_="mb-3"),
        ui.card(
            ui.card_header("Capacity estimates"),
            ui.output_data_frame("estimates"),
            ui.div(
                ui.download_button(
                    "download_estimates",
                    "Download Estimates",
                    class_="btn-primary",
                ),
                class_="d-flex justify-content-end mt-3",
            ),
        ),
        ui.card(
            ui.card_header("Feedback"),
            ui.p(
                "Please use the feedback button to share your comments about this app."
            ),
            class_="mt-3",
        ),
        class_="py-4",
        style="max-width: 920px;",
    ),
    title="NHP Capacity Conversion",
    theme=ui.Theme.from_brand(__file__),
)


def server(input: Inputs, output: Outputs, session: Session) -> None:
    @reactive.calc
    def capacity_results() -> tuple[str, dict[str, pd.DataFrame | pd.Series]]:
        return _load_capacity_results()

    @render.data_frame
    def estimates():
        activity_type, data_to_save = capacity_results()
        capacity_data = data_to_save[f"{activity_type}_capacity"]
        if not isinstance(capacity_data, pd.DataFrame):
            raise TypeError("Capacity results must be a DataFrame.")
        estimates_to_display = summarise_model_runs(capacity_data).reset_index()
        return render.DataTable(
            estimates_to_display,
            width="100%",
            summary=False,
        )

    @render.download(
        filename="capacity_conversion_results.xlsx",
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    def download_estimates():
        _, data_to_save = capacity_results()
        yield _create_workbook(data_to_save)


app = App(app_ui, server)
