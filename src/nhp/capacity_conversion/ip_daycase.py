import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pandas as pd

from nhp.capacity_conversion.ip_formulas import (
    calculate_beds_from_session_capacity,
    calculate_recovery_capacity,
    calculate_time_util_capacity,
    derive_recovery_occupancy_hours,
    derive_treatment_hours,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DaycaseConfig:
    formula: Callable
    assumptions: dict[str, str]


def calculate_daycase_frm_time_util(
    subgroup: str,
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for subgroups using the FRM_TIME_UTIL conversion archetype

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
    output = assumptions["output_frm_time_util"]
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


def calculate_daycase_frm_recovery_occupancy(
    subgroup: str,
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for subgroups using the FRM_RECOVERY_OCCUPANCY conversion archetype

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
    output = assumptions["output_frm_recovery_occupancy"]
    occupancy_hours = derive_recovery_occupancy_hours(functional_area_subgroup, time)
    results = pd.DataFrame(
        calculate_recovery_capacity(
            occupancy_hours, annual_operational_hours, occupancy
        )
    )
    results.loc[:, "output"] = output
    results = results.reset_index().set_index(["output", "model_run"])
    return results


def calculate_daycase_frm_session_capacity(
    subgroup: str,
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for subgroups using the FRM_SESSION_CAPACITY conversion archetype

    Args:
        subgroup (str): Name of functional area subgroup
        assumptions (dict[str, str]): Mapping of assumption name to use for the specific subgroup
        functional_area_subgroup (pd.Series): Functional area groupings in a Pandas Series, with the index name model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: Calculated capacity requirements for the specific subgroup
    """
    results = {}
    annual_session_capacity = cast(
        float,
        assumptions_df.at[assumptions["annual_session_capacity"], "Value"],
    )
    output = assumptions["output_frm_session_capacity"]
    results = pd.DataFrame(
        calculate_beds_from_session_capacity(
            functional_area_subgroup, annual_session_capacity
        )
    )
    results.loc[:, "output"] = output
    results = results.reset_index().set_index(["output", "model_run"])
    return results


DAYCASE_CONFIG = {
    "daycase_haem_onc_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_time_util,
            assumptions={
                "treatment_time": "HAEM_ONC_TREATMENT_TIME",
                "treatment_utilisation": "HAEM_ONC_TREATMENT_UTIL",
                "treatment_annual_operational_hours": "HAEM_ONC_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_time_util": "HAEM_ONC_TRT_SPACES",
            },
        )
    ],
    "daycase_endoscopy_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_time_util,
            assumptions={
                "treatment_time": "ENDOSCOPY_PROC_TIME",
                "treatment_utilisation": "ENDOSCOPY_PROC_UTIL",
                "treatment_annual_operational_hours": "ENDOSCOPY_PROC_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_time_util": "ENDOSCOPY_PROC_ROOMS",
            },
        ),
        DaycaseConfig(
            formula=calculate_daycase_frm_recovery_occupancy,
            assumptions={
                "recovery_time": "DAYCASE_ENDOSCOPY_RECOVERY_LOS",
                "recovery_occupancy": "DAYCASE_ENDOSCOPY_RECOVERY_OCC",
                "recovery_annual_operational_hours": "DAYCASE_ENDOSCOPY_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_recovery_occupancy": "DAYCASE_ENDOSCOPY_RECOVERY_BEDS",
            },
        ),
    ],
    "daycase_renal_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_session_capacity,
            assumptions={
                "annual_session_capacity": "DAYCASE_RENAL_ANNUAL_SESSION_CAPACITY_PER_BED",
                "output_frm_session_capacity": "DAYCASE_RENAL_BEDS",
            },
        )
    ],
    "daycase_adult_medical_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_recovery_occupancy,
            assumptions={
                "recovery_time": "DAYCASE_RECOVERY_ADULT_MEDICAL_LOS",
                "recovery_occupancy": "DAYCASE_RECOVERY_ADULT_MEDICAL_OCC",
                "recovery_annual_operational_hours": "DAYCASE_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_recovery_occupancy": "ADULT_MEDICAL_DAYCASE_RECOVERY_BEDS",
            },
        )
    ],
    "daycase_adult_surgical_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_recovery_occupancy,
            assumptions={
                "recovery_time": "DAYCASE_RECOVERY_ADULT_SURGICAL_LOS",
                "recovery_occupancy": "DAYCASE_RECOVERY_ADULT_SURGICAL_OCC",
                "recovery_annual_operational_hours": "DAYCASE_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_recovery_occupancy": "ADULT_SURGICAL_DAYCASE_RECOVERY_BEDS",
            },
        )
    ],
    "daycase_child_medical_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_recovery_occupancy,
            assumptions={
                "recovery_time": "DAYCASE_RECOVERY_PAEDIATRIC_MEDICAL_LOS",
                "recovery_occupancy": "DAYCASE_RECOVERY_PAEDIATRIC_MEDICAL_OCC",
                "recovery_annual_operational_hours": "DAYCASE_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_recovery_occupancy": "PAEDIATRIC_MEDICAL_DAYCASE_RECOVERY_BEDS",
            },
        )
    ],
    "daycase_child_surgical_spells": [
        DaycaseConfig(
            formula=calculate_daycase_frm_recovery_occupancy,
            assumptions={
                "recovery_time": "DAYCASE_RECOVERY_PAEDIATRIC_SURGICAL_LOS",
                "recovery_occupancy": "DAYCASE_RECOVERY_PAEDIATRIC_SURGICAL_OCC",
                "recovery_annual_operational_hours": "DAYCASE_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
                "output_frm_recovery_occupancy": "PAEDIATRIC_SURGICAL_DAYCASE_RECOVERY_BEDS",
            },
        )
    ],
}


def calculate_daycase_capacity(
    functional_areas: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    config=DAYCASE_CONFIG,
) -> pd.DataFrame:
    """Converts functional areas into capacity requirements using supplied assumptions

        Args:
            functional_areas (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run
            assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

        Returns:
    pd.DataFrame: DataFrame of calculated Daycase capacity requirements
    """
    logger.info("Calculating IP daycase capacity")
    results_list = []
    for subgroup, calculations in config.items():
        functional_area_subgroup = functional_areas.xs(key=subgroup, level="grouping")[
            "total"
        ]
        for calculation in calculations:
            results_list.append(
                calculation.formula(
                    subgroup=subgroup,
                    assumptions=calculation.assumptions,
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
    return run_single_activity_type("ip_daycase", calculate_daycase_capacity)


if __name__ == "__main__":
    sys.exit(main())
