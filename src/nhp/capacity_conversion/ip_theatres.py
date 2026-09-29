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

THEATRES_ASSUMPTIONS_DICT = {
    "adult_elective_surgical_procedures": {
        "procedure_time": "INPATIENT_THEATRE_ADULT_ELECTIVE_SURGICAL_PROC_TIME",
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "ADULT_ELECTIVE_SURGICAL_INPATIENT_PROC_THEATRES",
    },
    "adult_nonelective_surgical_procedures": {
        "procedure_time": "INPATIENT_THEATRE_ADULT_NON_ELECTIVE_SURGICAL_PROC_TIME",
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "ADULT_NON_ELECTIVE_SURGICAL_INPATIENT_PROC_THEATRES",
    },
    "adult_surgical_daycase_procedures": {
        "procedure_time": "DAYCASE_THEATRE_ADULT_SURGICAL_PROC_TIME",
        "annual_operational_hours": "DAYCASE_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "DAYCASE_THEATRE_UTIL",
        "output": "ADULT_SURGICAL_DAYCASE_PROC_THEATRES",
    },
    "cardiac_catheter_procedure": {
        "procedure_time": "LABS_CARDIAC_CATH_PROC_TIME",
        "annual_operational_hours": "LABS_CARDIAC_CATH_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "LABS_CARDIAC_CATH_UTIL",
        "output": "CARDIAC_CATH_PROC_LABS",
    },
    "interventional_radiology_procedure": {
        "procedure_time": "INT_RADIOLOGY_PROC_TIME",
        "annual_operational_hours": "INT_RADIOLOGY_PROC_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INT_RADIOLOGY_PROC_UTIL",
        "output": "INT_RADIOLOGY_PROC_ROOMS",
    },
    "paediatric_daycase_procedures": {
        "procedure_time": "DAYCASE_THEATRE_PAEDIATRIC_PROC_TIME",
        "annual_operational_hours": "DAYCASE_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "DAYCASE_THEATRE_UTIL",
        "output": "PAEDIATRIC_DAYCASE_PROC_THEATRES",
    },
    "paediatric_elective_procedures": {
        "procedure_time": "INPATIENT_THEATRE_PAEDIATRIC_ELECTIVE_SURGICAL_PROC_TIME",
        "annual_operational_hours": "INPATIENT_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        "utilisation": "INPATIENT_THEATRE_UTIL",
        "output": "PAEDIATRIC_ELECTIVE_INPATIENT_PROC_THEATRES",
    },
    "paediatric_nonelective_procedures": {
        "procedure_time": "INPATIENT_THEATRE_PAEDIATRIC_NON_ELECTIVE_SURGICAL_PROC_TIME",
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
    for grouping, assumptions_dict in THEATRES_ASSUMPTIONS_DICT.items():
        if grouping in functional_areas.index.get_level_values("functional_area"):
            procedure_time = cast(
                float,
                assumptions_df.at[
                    assumptions_dict["procedure_time"],
                    "Value",
                ],
            )
            treatment_hours = pd.DataFrame(
                derive_treatment_hours(
                    procedure_time,
                    functional_areas.xs(
                        key=(grouping, "procedures"),
                        level=["functional_area", "measure"],
                    )["value"],
                )
            )
            treatment_hours["functional_area"] = grouping
            treatment_hours["measure"] = "total_time_hours"
            treatment_hours = treatment_hours.set_index(
                ["functional_area", "measure"], append=True
            ).reorder_levels(["model_run", "measure", "functional_area"])
            functional_areas = pd.concat([functional_areas, treatment_hours])
    return functional_areas


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
    functional_areas = calculate_procedure_time(functional_areas, assumptions_df)
    return functional_areas


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
    for grouping in functional_areas_processed.index.get_level_values(
        "functional_area"
    ).unique():
        assumptions_dict = THEATRES_ASSUMPTIONS_DICT[grouping]
        treatment_hours = functional_areas_processed.xs(
            key=(grouping, "total_time_hours"), level=["functional_area", "measure"]
        )["value"]
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
        capacity_df = capacity_df.set_index("output", append=True)
        results_list.append(capacity_df)
    return pd.concat(results_list)


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type(
        "ip_procedures_and_theatres",
        calculate_ip_theatres_capacity,
        preprocess=preprocess_ip_theatres_data,
    )


if __name__ == "__main__":
    sys.exit(main())
