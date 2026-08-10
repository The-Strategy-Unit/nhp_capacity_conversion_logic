import os
from datetime import UTC, datetime
from io import BytesIO
from pathlib import PurePosixPath

import pandas as pd
from nhpy.az import connect_to_container, load_parquet_file
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
    load_assumptions,
    process_activity_type,
    summarise_model_runs,
)

FUNCTIONAL_AGGREGATION_ENV_VARS = {
    "op": "AZ_FUNC_AGG_OP_PATH",
    "aae": "AZ_FUNC_AGG_AAE_PATH",
    "ip_daycase": "AZ_FUNC_AGG_IP_DAYCASE_PATH",
    "ip_maternity": "AZ_FUNC_AGG_IP_MAT_PATH",
}

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


def _functional_aggregation_paths() -> tuple[dict[str, str], str, str]:
    blob_names = {}
    model_results = set()

    for activity_type, environment_variable in FUNCTIONAL_AGGREGATION_ENV_VARS.items():
        blob_name = _required_environment_variable(environment_variable)
        blob_path = PurePosixPath(blob_name)
        path_parts = blob_path.parts
        expected_filename = f"{activity_type}.parquet"

        if (
            blob_path.is_absolute()
            or len(path_parts) != 4
            or path_parts[0] != "functional-aggregations"
            or blob_path.name != expected_filename
        ):
            raise ValueError(
                f"{environment_variable} must have the form "
                "functional-aggregations/<version>/<guid>/"
                f"{expected_filename}"
            )

        blob_names[activity_type] = blob_name
        model_results.add((path_parts[1], path_parts[2]))

    if len(model_results) != 1:
        raise ValueError(
            "Functional aggregation paths must reference the same model version and GUID."
        )

    capacity_model_version, guid = model_results.pop()
    return blob_names, capacity_model_version, guid


def _load_capacity_results() -> dict[str, pd.DataFrame | pd.Series]:
    blob_names, capacity_model_version, guid = _functional_aggregation_paths()

    results_connection = connect_to_container(
        _required_environment_variable("AZ_STORAGE_EP"),
        _required_environment_variable("AZ_STORAGE_RESULTS"),
    )
    assumptions = load_assumptions(ASSUMPTIONS_URL)

    data_to_save: dict[str, pd.DataFrame | pd.Series] = {
        "metadata": pd.Series(
            {
                "guid": guid,
                "capacity_model_version": capacity_model_version,
                "capacity_conversion_runtime": datetime.now(tz=UTC).strftime(
                    "%Y%m%d_%H%M%S"
                ),
            }
        ),
        "assumptions": assumptions,
    }

    for activity_type, blob_name in blob_names.items():
        aggregations = load_parquet_file(results_connection, blob_name)
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

        for activity_type in FUNCTIONAL_AGGREGATION_ENV_VARS:
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

    @render.download(
        filename="capacity_conversion_results.xlsx",
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    def download_estimates():
        yield _create_workbook(capacity_results())


app = App(app_ui, server)
