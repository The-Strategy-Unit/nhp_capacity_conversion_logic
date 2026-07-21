import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pandas as pd
from nhpy.utils import get_logger

from nhp.capacity_conversion.ip_formulas import (
    calculate_recovery_capacity,
    derive_recovery_occupancy_hours,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = get_logger()


@dataclass(frozen=True)
class MaternityConfig:
    col_to_use: str
    formula: Callable
    assumptions: dict[str, str]


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
    "MATERNITY_ASSESSMENT_BEDS": {
        "maternity_assessment": MaternityConfig(
            col_to_use="spells",
            formula=calculate_maternity_assessment_beds,
            assumptions={
                "recovery_time": "MATERNITY_ASSESSMENT_ZERO_DAY_LOS",
                "recovery_occupancy": "MATERNITY_ASSESSMENT_OCC",
                "recovery_annual_operational_hours": "MATERNITY_ASSESSMENT_ANNUAL_OPERATIONAL_HOURS",
            },
        )
    }
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
    for output, output_config in config.items():
        for subgroup, subgroup_config in output_config.items():
            functional_area_subgroup = functional_areas.xs(
                key=subgroup, level="grouping"
            )[subgroup_config.col_to_use]
            results_list.append(
                subgroup_config.formula(
                    subgroup=subgroup,
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
    return run_single_activity_type("ip_maternity", calculate_maternity_capacity)


if __name__ == "__main__":
    sys.exit(main())
