import logging
import os
from collections import OrderedDict
from datetime import datetime
from typing import BinaryIO

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles.fonts import Font
from openpyxl.utils.dataframe import dataframe_to_rows

logger = logging.getLogger(__name__)


def add_coversheet(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
) -> OrderedDict[str, pd.DataFrame | pd.Series]:
    """Adds coversheet to the output Excel file

    Args:
        data_to_save (dict): Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included. At minimum should include "metadata" key and dataframe.

    Returns:
        OrderedDict: the data_to_save with an additional coversheet as the first key-value pair in the OrderedDict.
    """
    runtime = str(data_to_save["metadata"].get("capacity_conversion_runtime"))
    creation_date = datetime.fromisoformat(runtime).strftime("%d/%m/%Y")
    coversheet = {
        "OpenPlan Capacity Conversion Results Workbook": "",
        "Date Created": creation_date,
        "Introduction": "This workbook contains the results of the OpenPlan capacity conversion model (https://the-strategy-unit.github.io/open-plan-docs/) on results of the OpenPlan demand model (https://connect.strategyunitwm.nhs.uk/nhp/project_information/).",
        "Contents": "",
        "metadata": "Information about the OpenPlan demand model scenario that has been converted to capacity estimates, and the version of the OpenPlan capacity conversion model that was used.",
        "assumptions": "Descriptions and values of all default assumptions used in this conversion. Explanations can be found in this file: https://github.com/The-Strategy-Unit/open-plan-docs/blob/main/docs/data/assumptions_register.csv",
        "baseline_year_activity_counts": "Small-count suppressed counts of activity in the baseline modelling year (for details on suppression documentation see https://the-strategy-unit.github.io/open-plan-docs/). Note: these will differ from baseline counts of activity in the OpenPlan demand model.",
        "predicted_activity_volumes": "Mean and 80% confidence intervals (p10 and p90) of the distribution of predicted activity across functional areas.",
        "estimated_capacity_needs": "Mean and 80% confidence intervals (p10 and p90) of estimated capacity needed to meet predicted activity demand.",
    }
    data_to_save_with_coversheet = OrderedDict({"coversheet": pd.Series(coversheet)})
    data_to_save_with_coversheet.update(data_to_save)
    return data_to_save_with_coversheet


def tidy_metadata(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
) -> dict[str, pd.DataFrame | pd.Series]:
    """Tidies the 'metadata' worksheet to match agreed formatting and naming

    Args:
        data_to_save (dict[str, pd.DataFrame  |  pd.Series]): Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included. At minimum should include "metadata" key and dataframe.

    Returns:
        dict[str, pd.DataFrame | pd.Series]: Dictionary of data to save, with the "metadata" values renamed and reordered.
    """
    if "metadata" in data_to_save:
        rename = {
            "app_version": "demand_model_version",
            "scenario_name": "demand_model_scenario_name",
            "scenario_runtime": "demand_model_scenario_runtime",
        }

        keep = [
            "dataset",
            "capacity_model_version",
            "ip_sites",
            "op_sites",
            "aae_sites",
            "capacity_conversion_runtime",
            *rename,
        ]

        metadata = data_to_save["metadata"]

        data_to_save["metadata"] = metadata.reindex(
            [key for key in keep if key in metadata.index]
        ).rename(index=rename)
    return data_to_save


def summarise_model_runs(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate p10, p90 and mean across all model runs

    Args:
        df (pd.DataFrame): MultiIndex DataFrame with one index called "model_run"

    Raises:
        ValueError: If more than one value column in DataFrame

    Returns:
        pd.DataFrame: Summarised DataFrame
    """
    group_col_names = [name for name in df.index.names if name != "model_run"]
    if len(group_col_names) != 1:
        raise ValueError("Expected exactly one index column.")
    value_cols = [c for c in df.columns if c != "model_run"]
    if len(value_cols) > 1:
        df_list = []
        for col in value_cols:
            summary_df = pd.DataFrame(
                df.groupby(level=group_col_names)[col].agg(
                    p10=lambda s: s.quantile(0.10),
                    mean="mean",
                    p90=lambda s: s.quantile(0.90),
                )
            )
            summary_df["measure"] = col
            df_list.append(summary_df.reset_index())
        return pd.concat(df_list).set_index(group_col_names + ["measure"]).sort_index()
    return pd.DataFrame(
        df.groupby(level=group_col_names)[value_cols[0]].agg(
            p10=lambda s: s.quantile(0.10),
            mean="mean",
            p90=lambda s: s.quantile(0.90),
        )
    )


def apply_styling_to_coversheet(workbook: Workbook):
    """Applies styling to the coversheet worksheet in the results Excel file, turning some cells bold

    Args:
        workbook (Workbook): Results Excel Workbook
    """
    bold_values = [
        "OpenPlan Capacity Conversion Results Workbook",
        "Date Created",
        "Introduction",
        "Contents",
    ]
    ws = workbook["coversheet"]
    for row in range(1, ws.max_row + 1):
        cell = ws[f"{'A'}{row}"]
        if cell.value in bold_values:
            cell.font = Font(bold=True)


def add_care_setting_and_summarise(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
) -> dict[str, pd.DataFrame | pd.Series]:
    """Adds care setting to the dataframes and summarises model runs

    Args:
        data_to_save (dict[str, pd.DataFrame  |  pd.Series]): Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included.

    Returns:
        dict[str, pd.DataFrame | pd.Series]: Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included.
    """
    for key, df in data_to_save.items():
        if isinstance(df, pd.DataFrame) and key.startswith(("ip_", "op_", "aae_")):
            if "model_run" in df.index.names:
                df = summarise_model_runs(df)
            df["care_setting"] = key.split("_")[0]
        data_to_save[key] = df
    return data_to_save


def create_and_format_baseline_df(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines and formats dataframes for baseline_year_activity_counts worksheet

    Args:
        dfs (list[pd.DataFrame]): list of dataframes with baseline_year_activity_counts

    Returns:
        pd.DataFrame: Formatted dataframe of baseline_year_activity_counts
    """
    df = pd.concat(dfs)
    df.index.name = "activity_group"
    df = (
        df.reset_index()
        .melt(
            id_vars=["activity_group", "care_setting"],
            value_vars=[
                "total",
                "spells",
                "beddays",
                "total_theatre_time",
            ],
            var_name="measure",
            value_name="value",
        )
        .dropna(subset=["value"])
    )
    mask = df["measure"].eq("total")

    df.loc[mask, "measure"] = df.loc[mask, "activity_group"].str.split("_").str[-1]
    return df.set_index(["activity_group", "measure"])


def create_and_format_predicted_vols_df(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines and formats dataframes for predicted_activity_volumes worksheet

    Args:
        dfs (list[pd.DataFrame]): list of dataframes with predicted activity volumes

    Returns:
        pd.DataFrame: Formatted dataframe of predicted_activity_volumes
    """

    def add_measure_index(df: pd.DataFrame) -> pd.DataFrame:
        if "measure" not in df.index.names:
            df = df.copy()
            index_names = [str(name) for name in df.index.names if name != "model_run"]
            df["measure"] = [
                label.split("_")[-1]
                for label in df.index.get_level_values(index_names[0])
            ]
            df = df.set_index("measure", append=True)

            # Ensure consistent ordering
            df.index = df.index.reorder_levels(index_names + ["measure"])  # ty: ignore
        return df

    dfs = [add_measure_index(df) for df in dfs]
    df = pd.concat(dfs)
    df.index = df.index.set_names(["activity_group", "measure"])
    return df


def create_and_format_capacity_needs_df(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines and formats dataframes for estimated_capacity_needs worksheet

    Args:
        dfs (list[pd.DataFrame]): list of dataframes with estimated_capacity_needs

    Returns:
        pd.DataFrame: Formatted dataframe of estimated_capacity_needs
    """
    df = pd.concat(dfs)
    df.index.name = "resource"
    return df


def combine_and_format_dataframes(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
) -> dict[str, pd.DataFrame | pd.Series]:
    """Combines dataframes together for the different care settings, for presentation in the Excel file

    Args:
        data_to_save (dict[str, pd.DataFrame  |  pd.Series]): Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included.

    Returns:
        dict[str, pd.DataFrame | pd.Series]: Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included.
    """
    groups = {
        "baseline_year_activity_counts": [
            key for key in data_to_save if key.endswith("_baseline")
        ],
        "predicted_activity_volumes": [
            key for key in data_to_save if key.endswith("_fun_area_groupings")
        ],
        "estimated_capacity_needs": [
            key for key in data_to_save if key.endswith("_capacity")
        ],
    }

    combined_data = {}
    for sheet_name, keys in groups.items():
        if not keys:
            continue

        dfs = [pd.DataFrame(data_to_save[key]) for key in keys]

        if sheet_name == "baseline_year_activity_counts":
            combined_data[sheet_name] = create_and_format_baseline_df(dfs)
        if sheet_name == "predicted_activity_volumes":
            combined_data[sheet_name] = create_and_format_predicted_vols_df(dfs).round(
                2
            )
        if sheet_name == "estimated_capacity_needs":
            combined_data[sheet_name] = create_and_format_capacity_needs_df(dfs).round(
                2
            )

    keys_to_remove = [key for keys in groups.values() for key in keys]

    for key in keys_to_remove:
        data_to_save.pop(key, None)
    data_to_save.update(combined_data)
    return data_to_save


def process_data_to_save(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
) -> OrderedDict[str, pd.DataFrame | pd.Series]:
    """Chains together all the functions for processing the data_to_save into the required format for the Excel output

    Args:
        data_to_save (dict[str, pd.DataFrame  |  pd.Series]): Raw data for saving

    Returns:
        dict[str, pd.DataFrame | pd.Series]: Dict with data processed into the right format for saving into Excel. The keys are the titles of the
        worksheets and the values are the dataframes to be included.
    """
    data_to_save = add_care_setting_and_summarise(data_to_save)
    data_to_save = combine_and_format_dataframes(data_to_save)
    data_to_save = tidy_metadata(data_to_save)
    data_to_save = add_coversheet(data_to_save)
    return data_to_save


def process_and_save_results_to_excel(
    data_to_save: dict[str, pd.DataFrame | pd.Series],
    *,
    destination: str | os.PathLike[str] | BinaryIO | None = None,
) -> None:
    """Save capacity conversion results to an Excel file or binary stream.

    Args:
        data_to_save (dict[str, pd.DataFrame  |  pd.Series]): Dictionary of data to save, where the keys are the titles of the
        worksheets and the values are the dataframes to be included. At minimum should include "metadata" key and dataframe.
        destination (str | os.PathLike[str] | BinaryIO | None): Optional path or
        binary stream to write to. When omitted, the CLI results path is used.
    """
    filepath = None
    if destination is None:
        directory = os.path.join(
            "results",
            str(data_to_save["metadata"].loc["guid"]),
            str(data_to_save["metadata"].loc["capacity_conversion_runtime"]),
        )
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, "capacity_conversion_results.xlsx")
        destination = filepath

    data_to_process = {
        sheet_name: data.copy(deep=True) for sheet_name, data in data_to_save.items()
    }
    wb = Workbook()
    default_sheet = wb.active
    assert default_sheet is not None
    wb.remove(default_sheet)
    processed_data_to_save = process_data_to_save(data_to_process)
    for sheet_name, df in processed_data_to_save.items():
        ws = wb.create_sheet(title=sheet_name[:31])
        if isinstance(df, pd.Series):
            rows = dataframe_to_rows(
                df.to_frame().reset_index(),
                index=False,
                header=False,
            )
        else:
            rows = dataframe_to_rows(
                df.reset_index(),
                index=False,
                header=True,
            )

        for r_idx, row in enumerate(
            rows,
            start=1,
        ):
            for c_idx, value in enumerate(row, start=1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = max_len + 2
    apply_styling_to_coversheet(wb)
    wb.save(destination)
    if filepath is not None:
        logger.info(f"💾 Results saved to {filepath}")
