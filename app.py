import os
from datetime import UTC, datetime
from io import BytesIO

import pandas as pd
from shiny import App, Inputs, Outputs, Session, reactive, render, ui

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
    summarise_model_runs,
)

CAPACITY_MODEL_VERSION = "dev"
ACTIVITY_TYPES = ("op", "aae", "ip_daycase", "ip_maternity")

CAPACITY_CALCULATIONS = {
    "aae": calculate_aae_capacity,
    "ip_daycase": calculate_daycase_capacity,
    "ip_maternity": calculate_maternity_capacity,
    "op": calculate_op_capacity,
}

CAPACITY_PREPROCESSORS = {
    "ip_maternity": preprocess_ip_maternity_data,
}


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_capacity_results() -> dict[str, pd.DataFrame | pd.Series]:
    guid = _required_environment_variable("AZ_FUNC_AGG_GUID")
    storage_endpoint = _required_environment_variable("AZ_STORAGE_EP")
    results_container = _required_environment_variable("AZ_STORAGE_RESULTS")
    metadata = load_metadata_from_ats(
        guid,
        _required_environment_variable("AZ_TABLE_ENDPOINT"),
        _required_environment_variable("TABLE_NAME"),
        CAPACITY_MODEL_VERSION,
    )
    metadata["capacity_conversion_runtime"] = datetime.now(tz=UTC).strftime(
        "%Y%m%d_%H%M%S"
    )

    assumptions = load_assumptions(ASSUMPTIONS_URL)
    data_to_save: dict[str, pd.DataFrame | pd.Series] = {
        "metadata": pd.Series(metadata).drop(
            ["PartitionKey", "RowKey"], errors="ignore"
        ),
        "assumptions": assumptions,
    }
    aggregations_path = create_aggregations_path(metadata)

    for activity_type in ACTIVITY_TYPES:
        aggregations = load_aggregations(
            storage_endpoint,
            results_container,
            aggregations_path,
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
        class_="py-4",
        style="max-width: 920px;",
    ),
    title="NHP Capacity Conversion",
    theme=ui.Theme.from_brand(__file__),
)


def server(input: Inputs, output: Outputs, session: Session) -> None:
    @reactive.calc
    def capacity_results() -> dict[str, pd.DataFrame | pd.Series]:
        return _load_capacity_results()

    @render.data_frame
    def estimates():
        data_to_save = capacity_results()
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
        yield _create_workbook(capacity_results())


app = App(app_ui, server)
