import logging
import sys
from typing import cast

import pandas as pd

from nhp.capacity_conversion.ip_formulas import (
    calculate_time_util_capacity,
    derive_treatment_hours,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = logging.getLogger(__name__)

THEATRES_WORKLOAD_ASSUMPTIONS_DICT = {
    "adult_elective_surgical_procedures_unknown_time": {
        "procedure_time": "INPATIENT_THEATRE_ADULT_ELECTIVE_SURGICAL_PROC_TIME"
    },
    "adult_nonelective_surgical_procedures_unknown_time": {
        "procedure_time": "INPATIENT_THEATRE_ADULT_NON_ELECTIVE_SURGICAL_PROC_TIME"
    },
    "paediatric_elective_procedures_unknown_time": {
        "procedure_time": "INPATIENT_THEATRE_PAEDIATRIC_ELECTIVE_SURGICAL_PROC_TIME"
    },
    "paediatric_nonelective_procedures_unknown_time": {
        "procedure_time": "INPATIENT_THEATRE_PAEDIATRIC_NON_ELECTIVE_SURGICAL_PROC_TIME"
    },
    "adult_surgical_daycase_procedures_unknown_time": {
        "procedure_time": "DAYCASE_THEATRE_ADULT_SURGICAL_PROC_TIME"
    },
    "paediatric_daycase_procedures_unknown_time": {
        "procedure_time": "DAYCASE_THEATRE_PAEDIATRIC_PROC_TIME"
    },
    "cardiac_catheter_procedure": {"procedure_time": "LABS_CARDIAC_CATH_PROC_TIME"},
    "interventional_radiology_procedure": {"procedure_time": "INT_RADIOLOGY_PROC_TIME"},
}

THEATRES_CAPACITY_ASSUMPTIONS_DICT = {
    "adult_elective_surgical_procedures": {
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "ADULT_ELECTIVE_SURGICAL_INPATIENT_PROC_THEATRES",
    },
    "adult_nonelective_surgical_procedures": {
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "ADULT_NON_ELECTIVE_SURGICAL_INPATIENT_PROC_THEATRES",
    },
    "adult_surgical_daycase_procedures": {
        "annual_operational_hours": "DAYCASE_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "DAYCASE_THEATRE_UTIL",
        "output": "ADULT_SURGICAL_DAYCASE_PROC_THEATRES",
    },
    "cardiac_catheter_procedure": {
        "annual_operational_hours": "LABS_CARDIAC_CATH_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "LABS_CARDIAC_CATH_UTIL",
        "output": "CARDIAC_CATH_PROC_LABS",
    },
    "interventional_radiology_procedure": {
        "annual_operational_hours": "INT_RADIOLOGY_PROC_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INT_RADIOLOGY_PROC_UTIL",
        "output": "INT_RADIOLOGY_PROC_ROOMS",
    },
    "paediatric_daycase_procedures": {
        "annual_operational_hours": "DAYCASE_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "DAYCASE_THEATRE_UTIL",
        "output": "PAEDIATRIC_DAYCASE_PROC_THEATRES",
    },
    "paediatric_elective_procedures": {
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "PAEDIATRIC_ELECTIVE_INPATIENT_PROC_THEATRES",
    },
    "paediatric_nonelective_procedures": {
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "PAEDIATRIC_NON_ELECTIVE_INPATIENT_PROC_THEATRES",
    },
}


def calculate_procedure_time(
    functional_areas: pd.DataFrame, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Calculates procedure time for spells with an unknown procedure time

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP procedures and theatres
        assumptions_df (pd.DataFrame): DataFrame with required assumptions

    Returns:
        pd.DataFrame: Functional areas with added procedure time for activity with unknown time
    """
    for grouping, assumptions_dict in THEATRES_WORKLOAD_ASSUMPTIONS_DICT.items():
        procedure_time = cast(
            float,
            assumptions_df.at[
                assumptions_dict["procedure_time"],
                "Value",
            ],
        )

        mask = functional_areas.index.get_level_values("procedure_grouping") == grouping
        functional_areas.loc[mask, "total_theatre_time"] = derive_treatment_hours(
            procedure_time, functional_areas.loc[mask, "spells"]
        )
    return functional_areas


def convert_procedure_time_to_hours(functional_areas: pd.DataFrame) -> pd.DataFrame:
    """Converts procedure times for procedures with a known time from minutes to hours

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP procedures and theatres

    Returns:
        pd.DataFrame: Functional areas from Azure for IP procedures and theatres, with total_theatre_time converted from minutes to hours
    """
    for grouping in functional_areas.index.get_level_values("procedure_grouping"):
        if grouping not in THEATRES_WORKLOAD_ASSUMPTIONS_DICT:
            functional_areas.loc[(slice(None), grouping), "total_theatre_time"] = (
                functional_areas.loc[(slice(None), grouping), "total_theatre_time"] / 60
            )
    return functional_areas


def combine_procedure_groupings(functional_areas: pd.DataFrame) -> pd.DataFrame:
    """Groups together activity_unknown_time with activity, because following calculate_procedure_time
    all activity including activity_unknown_time should have values in the "total_theatre_time" column,
    expressed in treatment hours

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP procedures and theatres which have
        been processed with convert_procedure_time_to_hours and calculate_procedure_time

    Returns:
        pd.DataFrame: Functional areas with activity_unknown_time and activity combined
    """
    functional_areas = functional_areas.reset_index(level="procedure_grouping")
    functional_areas.loc[:, "procedure_grouping"] = functional_areas[
        "procedure_grouping"
    ].str.removesuffix("_unknown_time")
    functional_areas_grouped = (
        functional_areas.groupby(["model_run", "procedure_grouping"])
        .sum(numeric_only=True)
        .sort_index()
    )
    return functional_areas_grouped


def preprocess_ip_theatres_data(
    functional_areas: pd.DataFrame, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Preprocesses IP theatres data for conversion to capacity, standardising from treatment minutes to hours

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP procedures and theatres
        assumptions_df (pd.DataFrame): DataFrame with required assumptions

    Returns:
        pd.DataFrame: Preprocessed IP procedures and theatres data for conversion to capacity
    """
    # Drop unknown procedures
    functional_areas = functional_areas.loc[
        functional_areas.index.get_level_values("procedure_grouping")
        != "unknown_procedure",
        :,
    ]
    functional_areas = convert_procedure_time_to_hours(functional_areas)
    functional_areas = calculate_procedure_time(functional_areas, assumptions_df)
    functional_areas_grouped = combine_procedure_groupings(functional_areas)
    return functional_areas_grouped


def calculate_ip_theatres_capacity(
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Converts functional areas into capacity requirements using supplied assumptions

        Args:
            functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
            Functional areas should first be processed with preprocess_ip_theatres_data
            assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

        Returns:
    pd.DataFrame: DataFrame of calculated theatres capacity requirements
    """
    logger.info("Calculating IP theatres capacity")
    results_list = []
    for grouping, assumptions_dict in THEATRES_CAPACITY_ASSUMPTIONS_DICT.items():
        treatment_hours = functional_areas_processed.xs(
            key=grouping, level="procedure_grouping"
        )["total_theatre_time"]
        annual_operational_hours = cast(
            float,
            assumptions_df.at[assumptions_dict["annual_operational_hours"], "Value"],
        )
        utilisation = cast(
            float, assumptions_df.at[assumptions_dict["utilisation"], "Value"]
        )
        capacity_df = pd.DataFrame(
            calculate_time_util_capacity(
                treatment_hours, annual_operational_hours, utilisation
            )
        )
        capacity_df.loc[:, "output"] = assumptions_dict["output"]
        capacity_df = (
            capacity_df.reset_index()
            .set_index(["output", "model_run"])
            .rename(columns={"total_theatre_time": "total"})
        )
        results_list.append(capacity_df)
    return pd.concat(results_list)


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type(
        "ip_theatres",
        calculate_ip_theatres_capacity,
        preprocess=preprocess_ip_theatres_data,
    )


if __name__ == "__main__":
    sys.exit(main())
