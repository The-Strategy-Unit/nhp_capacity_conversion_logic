import sys
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

ASSUMPTIONS_MAPPING = {
    "daycase_haem_onc_spells": {
        "treatment_time": "HAEM_ONC_TREATMENT_TIME",
        "treatment_utilisation": "HAEM_ONC_TREATMENT_UTIL",
        "treatment_annual_operational_hours": "HAEM_ONC_ANNUAL_OPERATIONAL_HOURS",
        "output_frm_time_util": "HAEM_ONC_TRT_SPACES",
    },
    "daycase_endoscopy_spells": {
        "recovery_time": "DAYCASE_ENDOSCOPY_RECOVERY_LOS",
        "recovery_occupancy": "DAYCASE_ENDOSCOPY_RECOVERY_OCC",
        "recovery_annual_operational_hours": "DAYCASE_ENDOSCOPY_RECOVERY_ANNUAL_OPERATIONAL_HOURS",
        "treatment_time": "ENDOSCOPY_PROC_TIME",
        "treatment_utilisation": "ENDOSCOPY_PROC_UTIL",
        "treatment_annual_operational_hours": "ENDOSCOPY_PROC_ANNUAL_OPERATIONAL_HOURS",
        "output_frm_recovery_occupancy": "DAYCASE_ENDOSCOPY_RECOVERY_BEDS",
        "output_frm_time_util": "ENDOSCOPY_PROC_ROOMS",
    },
    # "daycase_adult_medical_spells": {},
    # "daycase_adult_surgical_spells": {},
    # "daycase_child_medical_spells": {},
    # "daycase_child_surgical_spells": {},
    # "daycase_renal_spells": {},
}


def calculate_daycase_frm_time_util(
    subgroup: str, functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> dict:
    results = {}
    time = cast(
        float,
        assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["treatment_time"], "Value"],
    )
    utilisation = cast(
        float,
        assumptions_df.at[
            ASSUMPTIONS_MAPPING[subgroup]["treatment_utilisation"], "Value"
        ],
    )
    annual_operational_hours = cast(
        float,
        assumptions_df.at[
            ASSUMPTIONS_MAPPING[subgroup]["treatment_annual_operational_hours"], "Value"
        ],
    )
    output = ASSUMPTIONS_MAPPING[subgroup]["output_frm_time_util"]
    for value in ["p10", "mean", "p90"]:
        treatment_hours = derive_treatment_hours(
            time,
            functional_areas_summarised[subgroup][value],
        )
        results[value] = calculate_time_util_capacity(
            treatment_hours, annual_operational_hours, utilisation
        )
    return {output: results}


def calculate_daycase_frm_recovery_occupancy(
    subgroup: str, functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> dict:
    results = {}
    time = cast(
        float,
        assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["recovery_time"], "Value"],
    )
    occupancy = cast(
        float,
        assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["recovery_occupancy"], "Value"],
    )
    annual_operational_hours = cast(
        float,
        assumptions_df.at[
            ASSUMPTIONS_MAPPING[subgroup]["recovery_annual_operational_hours"], "Value"
        ],
    )
    output = ASSUMPTIONS_MAPPING[subgroup]["output_frm_recovery_occupancy"]
    for value in ["p10", "mean", "p90"]:
        treatment_hours = derive_recovery_occupancy_hours(
            time,
            functional_areas_summarised[subgroup][value],
        )
        results[value] = calculate_recovery_capacity(
            treatment_hours, annual_operational_hours, occupancy
        )
    return {output: results}


FORMULA_MAPPING = {
    calculate_daycase_frm_time_util: [
        "daycase_haem_onc_spells",
        "daycase_endoscopy_spells",
    ],
    calculate_daycase_frm_recovery_occupancy: ["daycase_endoscopy_spells"],
}


def calculate_daycase_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated OP capacity requirements
    """
    logger.info("Calculating IP daycase capacity")
    results_dict = {}
    # for subgroup in functional_areas_summarised.keys():
    for formula in FORMULA_MAPPING.keys():
        for subgroup in FORMULA_MAPPING[formula]:
            subgroup_results = formula(
                subgroup, functional_areas_summarised, assumptions_df
            )
            results_dict.update(subgroup_results)

    return pd.DataFrame.from_dict(results_dict, orient="index")


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type("ip_daycase", calculate_daycase_capacity)


if __name__ == "__main__":
    sys.exit(main())
