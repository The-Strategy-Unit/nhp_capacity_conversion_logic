import sys
from typing import cast

import pandas as pd
from nhpy.utils import get_logger

from nhp.capacity_conversion.ip_formulas import (
    calculate_time_util_capacity,
    derive_treatment_hours,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = get_logger()

ASSUMPTIONS_MAPPING = {
    "daycase_haem_onc_spells": {
        "treatment_time": "HAEM_ONC_TREATMENT_TIME",
        "utilisation": "HAEM_ONC_TREATMENT_UTIL",
        "annual_operational_hours": "HAEM_ONC_ANNUAL_OPERATIONAL_HOURS",
        "output": "HAEM_ONC_TRT_SPACES",
    },
    "daycase_endoscopy_spells": {},
    "daycase_adult_medical_spells": {},
    "daycase_adult_surgical_spells": {},
    "daycase_child_medical_spells": {},
    "daycase_child_surgical_spells": {},
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
    for subgroup in ["daycase_haem_onc_spells"]:
        # TODO: change conversion archetype used depending on the subgroup
        # TODO will also need to change the values though....
        results = {}

        treatment_time = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["treatment_time"], "Value"],
        )
        utilisation = cast(
            float,
            assumptions_df.at[ASSUMPTIONS_MAPPING[subgroup]["utilisation"], "Value"],
        )
        annual_operational_hours = cast(
            float,
            assumptions_df.at[
                ASSUMPTIONS_MAPPING[subgroup]["annual_operational_hours"], "Value"
            ],
        )
        output = ASSUMPTIONS_MAPPING[subgroup]["output"]
        for value in ["p10", "mean", "p90"]:
            treatment_hours = derive_treatment_hours(
                treatment_time,
                functional_areas_summarised[subgroup][value],
            )
            results[value] = calculate_time_util_capacity(
                treatment_hours, annual_operational_hours, utilisation
            )
        results_dict[output] = results
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
