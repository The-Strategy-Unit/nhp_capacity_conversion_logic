import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pandas as pd
from nhpy.utils import get_logger

from nhp.capacity_conversion.ip_formulas import (
    calculate_recovery_capacity,
    calculate_time_util_capacity,
    derive_recovery_occupancy_hours,
    derive_treatment_hours,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = get_logger()


@dataclass(frozen=True)
class MaternityConfig:
    subgroup: str
    col_to_use: str
    formula: Callable
    assumptions: dict[str, str]


def process_theatres_obstetric_proc_data(
    functional_areas: pd.DataFrame,
) -> pd.DataFrame:
    """Combine activity for elective and nonelective csections to create obstetric_theatre_procedures
    functional area grouping for calculation of theatres for obstetric procedures

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP maternity

    Returns:
        pd.DataFrame: IP maternity functional areas with new grouping obstetric_theatre_procedures
    """
    obstetric_theatre_procedures = (
        functional_areas[
            functional_areas["grouping"].isin(
                [
                    "maternity_elective_csection_nonzerolos",
                    "maternity_nonelective_csection_nonzerolos",
                    "maternity_elective_csection_zerolos",
                    "maternity_nonelective_csection_zerolos",
                ]
            )
        ]
        .groupby(level=0)
        .sum()
        .assign(grouping="obstetric_theatre_procedures")
    )
    result = pd.concat([functional_areas, obstetric_theatre_procedures]).sort_index()
    return result


def preprocess_ip_maternity_data(functional_areas: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses IP maternity data for conversion to capacity.

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP maternity

    Returns:
        pd.DataFrame: Preprocessed IP maternity data for conversion to capacity
    """
    functional_areas_processed = process_theatres_obstetric_proc_data(functional_areas)
    return functional_areas_processed


def calculate_theatres_obstetric_proc(
    subgroup: str,
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for obstetric theatres using the FRM_TIME_UTIL conversion archetype

    Args:
        subgroup (str): Name of functional area subgroup
        assumptions (dict[str, str]): Mapping of assumption name to use for the specific subgroup
        functional_area_subgroup (pd.Series): Functional area groupings in a Pandas Series, with the index name model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: Calculated capacity requirements for the specific subgroup
    """

    time = cast(float, assumptions_df.at[assumptions["treatment_time"], "Value"])
    utilisation = cast(
        float,
        assumptions_df.at[assumptions["treatment_utilisation"], "Value"],
    )
    annual_operational_hours = cast(
        float,
        assumptions_df.at[
            assumptions["treatment_annual_operational_hours"],
            "Value",
        ],
    )
    output = "OBSTETRIC_PROC_THEATRES"
    treatment_hours = derive_treatment_hours(
        time,
        functional_area_subgroup,
    )
    results = pd.DataFrame(
        calculate_time_util_capacity(
            treatment_hours,
            annual_operational_hours,
            utilisation,
        )
    )
    results.loc[:, "output"] = output
    results = results.reset_index().set_index(["output", "model_run"])
    return results


def calculate_maternity_assessment_beds(
    subgroup: str,
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for maternity assessment beds using the FRM_RECOVERY_OCCUPANCY conversion archetype

    Args:
        subgroup (str): Name of functional area subgroup
        assumptions (dict[str, str]): Mapping of assumption name to use for the specific subgroup
        functional_area_subgroup (pd.Series): Functional area groupings in a Pandas Series, with the index name model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: Calculated capacity requirements for the specific subgroup
    """

    time = cast(
        float,
        assumptions_df.at[assumptions["recovery_time"], "Value"],
    )
    occupancy = cast(
        float,
        assumptions_df.at[assumptions["recovery_occupancy"], "Value"],
    )
    annual_operational_hours = cast(
        float,
        assumptions_df.at[assumptions["recovery_annual_operational_hours"], "Value"],
    )
    occupancy_hours = derive_recovery_occupancy_hours(functional_area_subgroup, time)
    results = pd.DataFrame(
        calculate_recovery_capacity(
            occupancy_hours, annual_operational_hours, occupancy
        )
    )
    results.loc[:, "output"] = "MATERNITY_ASSESSMENT_BEDS"
    results = results.reset_index().set_index(["output", "model_run"])
    return results


MATERNITY_CONFIG = {
    "MATERNITY_ASSESSMENT_BEDS": MaternityConfig(
        subgroup="maternity_assessment",
        col_to_use="spells",
        formula=calculate_maternity_assessment_beds,
        assumptions={
            "recovery_time": "MATERNITY_ASSESSMENT_ZERO_DAY_LOS",
            "recovery_occupancy": "MATERNITY_ASSESSMENT_OCC",
            "recovery_annual_operational_hours": "MATERNITY_ASSESSMENT_ANNUAL_OPERATIONAL_HOURS",
        },
    ),
    "OBSTETRIC_PROC_THEATRES": MaternityConfig(
        subgroup="obstetric_theatre_procedures",
        col_to_use="spells",
        formula=calculate_theatres_obstetric_proc,
        assumptions={
            "treatment_time": "OBSTETRIC_THEATRE_PROC_TIME",
            "treatment_utilisation": "OBSTETRIC_THEATRE_UTIL",
            "treatment_annual_operational_hours": "OBSTETRIC_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        },
    ),
}


def calculate_maternity_capacity(
    functional_areas: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    config=MATERNITY_CONFIG,
) -> pd.DataFrame:
    """Converts functional areas into capacity requirements using supplied assumptions

        Args:
            functional_areas (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run
            assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

        Returns:
    pd.DataFrame: DataFrame of calculated maternity capacity requirements
    """
    logger.info("Calculating IP maternity capacity")
    results_list = []
    for output, subgroup_config in config.items():
        functional_area_subgroup = functional_areas.xs(
            key=subgroup_config.subgroup, level="grouping"
        )[subgroup_config.col_to_use]
        results_list.append(
            subgroup_config.formula(
                subgroup=subgroup_config.subgroup,
                assumptions=subgroup_config.assumptions,
                functional_area_subgroup=functional_area_subgroup,
                assumptions_df=assumptions_df,
            )
        )
    return pd.concat(results_list)


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type(
        "ip_maternity",
        calculate_maternity_capacity,
        preprocess=preprocess_ip_maternity_data,
    )


if __name__ == "__main__":
    sys.exit(main())
