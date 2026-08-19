import logging
import sys
from typing import cast

import pandas as pd

from nhp.capacity_conversion.ip_formulas import (
    calculate_beds,
    derive_beddays_from_spells,
)
from nhp.capacity_conversion.utils import run_single_activity_type

logger = logging.getLogger(__name__)


WARD_WORKLOAD_ASSUMPTIONS_DICT = {
    "adult_nonelective_medical": {
        "zero_day_los": "WARD_ADULT_NON_ELECTIVE_MEDICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_ADULT_PERCENT_OVERNIGHT_BED_DAYS",
        "assessment_los": "INPATIENT_ASSESSMENT_ADULT_NON_ELECTIVE_MEDICAL_LOS",
    },
    "adult_elective_medical": {
        "zero_day_los": "WARD_ADULT_ELECTIVE_MEDICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_ADULT_PERCENT_OVERNIGHT_BED_DAYS",
    },
    "adult_nonelective_surgical": {
        "zero_day_los": "WARD_ADULT_NON_ELECTIVE_SURGICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_ADULT_PERCENT_OVERNIGHT_BED_DAYS",
        "assessment_los": "INPATIENT_ASSESSMENT_ADULT_NON_ELECTIVE_SURGICAL_LOS",
    },
    "adult_elective_surgical": {
        "zero_day_los": "WARD_ADULT_ELECTIVE_SURGICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_ADULT_PERCENT_OVERNIGHT_BED_DAYS",
    },
    "paediatric_nonelective_medical": {
        "zero_day_los": "WARD_PAEDIATRIC_NON_ELECTIVE_MEDICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_PAEDIATRIC_PERCENT_OVERNIGHT_BED_DAYS",
        "assessment_los": "INPATIENT_ASSESSMENT_PAEDIATRIC_NON_ELECTIVE_MEDICAL_LOS",
    },
    "paediatric_elective_medical": {
        "zero_day_los": "WARD_PAEDIATRIC_ELECTIVE_MEDICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_PAEDIATRIC_PERCENT_OVERNIGHT_BED_DAYS",
    },
    "paediatric_nonelective_surgical": {
        "zero_day_los": "WARD_PAEDIATRIC_NON_ELECTIVE_SURGICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_PAEDIATRIC_PERCENT_OVERNIGHT_BED_DAYS",
        "assessment_los": "INPATIENT_ASSESSMENT_PAEDIATRIC_NON_ELECTIVE_SURGICAL_LOS",
    },
    "paediatric_elective_surgical": {
        "zero_day_los": "WARD_PAEDIATRIC_ELECTIVE_SURGICAL_ZERO_DAY_LOS",
        "critical_care_percentage": "CRITICAL_CARE_PAEDIATRIC_PERCENT_OVERNIGHT_BED_DAYS",
    },
}


WARD_GROUP_DEFINITIONS = {
    "adult_assessment_beddays": (
        [
            "adult_nonelective_medical",
            "adult_elective_medical",
            "adult_nonelective_surgical",
            "adult_elective_surgical",
        ],
        "assessment_beddays",
    ),
    "adult_critical_care_beddays": (
        [
            "adult_nonelective_medical",
            "adult_elective_medical",
            "adult_nonelective_surgical",
            "adult_elective_surgical",
        ],
        "critical_care_beddays",
    ),
    "adult_elective_wards_beddays": (
        ["adult_elective_medical", "adult_elective_surgical"],
        "ward_beddays",
    ),
    "adult_nonelective_wards_beddays": (
        ["adult_nonelective_medical", "adult_nonelective_surgical"],
        "ward_beddays",
    ),
    "paediatric_assessment_beddays": (
        [
            "paediatric_nonelective_medical",
            "paediatric_elective_medical",
            "paediatric_nonelective_surgical",
            "paediatric_elective_surgical",
        ],
        "assessment_beddays",
    ),
    "paediatric_critical_care_beddays": (
        [
            "paediatric_nonelective_medical",
            "paediatric_elective_medical",
            "paediatric_nonelective_surgical",
            "paediatric_elective_surgical",
        ],
        "critical_care_beddays",
    ),
    "paediatric_wards_beddays": (
        [
            "paediatric_elective_medical",
            "paediatric_elective_surgical",
            "paediatric_nonelective_medical",
            "paediatric_nonelective_surgical",
        ],
        "ward_beddays",
    ),
}

WARD_CAPACITY_ASSUMPTIONS_DICT = {
    "adult_assessment_beddays": {
        "operational_days": "INPATIENT_ASSESSMENT_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "INPATIENT_ASSESSMENT_ADULT_OCC",
        "output": "ADULT_INPATIENT_ASSESSMENT_BEDS",
    },
    "adult_critical_care_beddays": {
        "operational_days": "CRITICAL_CARE_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "CRITICAL_CARE_ADULT_OCC",
        "output": "ADULT_CRITICAL_CARE_BEDS",
    },
    "adult_elective_wards_beddays": {
        "operational_days": "WARD_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "WARD_ADULT_ELECTIVE_OCC",
        "output": "ADULT_ELECTIVE_INPATIENT_WARD_BEDS",
    },
    "adult_nonelective_wards_beddays": {
        "operational_days": "WARD_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "WARD_ADULT_NON_ELECTIVE_OCC",
        "output": "ADULT_NON_ELECTIVE_INPATIENT_WARD_BEDS",
    },
    "paediatric_assessment_beddays": {
        "operational_days": "INPATIENT_ASSESSMENT_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "INPATIENT_ASSESSMENT_PAEDIATRIC_OCC",
        "output": "PAEDIATRIC_INPATIENT_ASSESSMENT_BEDS",
    },
    "paediatric_critical_care_beddays": {
        "operational_days": "CRITICAL_CARE_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "CRITICAL_CARE_PAEDIATRIC_OCC",
        "output": "PAEDIATRIC_CRITICAL_CARE_BEDS",
    },
    "paediatric_wards_beddays": {
        "operational_days": "WARD_ANNUAL_OPERATIONAL_DAYS",
        "occupancy": "WARD_PAEDIATRIC_OCC",
        "output": "PAEDIATRIC_INPATIENT_WARD_BEDS",
    },
}


def derive_ward_beddays(
    grouping: str,
    functional_areas: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    assumptions_dict: dict[str, str],
) -> dict[str, pd.Series]:
    """Calculate ward beddays

    Args:
        grouping (str): Name of functional area grouping
        functional_areas (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating beddays


    Returns:
        dict[str, pd.Series]: Dictionary with calculated ward, critical critical care, and assessment beddays
    """
    _age_group, admission_type, _specialty_type = grouping.split("_")
    zero_day_los = cast(
        float,
        assumptions_df.at[
            assumptions_dict["zero_day_los"],
            "Value",
        ],
    )
    zero_day_beddays = derive_beddays_from_spells(
        functional_areas.xs(key=grouping + "_zerolos", level="grouping")["spells"],
        zero_day_los,
    )

    critical_care_percentage = cast(
        float,
        assumptions_df.at[assumptions_dict["critical_care_percentage"], "Value"],
    )
    critical_care_beddays = (
        critical_care_percentage
        * functional_areas.xs(key=grouping + "_nonzerolos", level="grouping")["beddays"]
    )
    # Assessment beddays are always 0 for elective activity
    assessment_beddays = pd.Series(0, index=critical_care_beddays.index.copy())
    if admission_type == "nonelective":
        assessment_los = cast(
            float,
            assumptions_df.at[
                assumptions_dict["assessment_los"],
                "Value",
            ],
        )
        assessment_spells = (
            functional_areas.xs(key=grouping + "_nonzerolos", level="grouping")[
                "spells"
            ]
            + functional_areas.xs(key=grouping + "_zerolos", level="grouping")["spells"]
        )
        assessment_beddays = derive_beddays_from_spells(
            assessment_spells, assessment_los
        )
    ward_beddays = (
        functional_areas.xs(key=grouping + "_nonzerolos", level="grouping")["beddays"]
        + zero_day_beddays
        - assessment_beddays
        - critical_care_beddays
    )
    bedday_pools = {
        "ward_beddays": ward_beddays,
        "critical_care_beddays": critical_care_beddays,
        "assessment_beddays": assessment_beddays,
    }
    return bedday_pools


def group_ip_wards_beddays(ip_wards_bedday_pools: pd.DataFrame) -> pd.DataFrame:
    """Combines groupings together for conversion to capacity

    Args:
        ip_wards_bedday_pools (pd.DataFrame): Calculated bedday pools, ungrouped

    Returns:
        pd.DataFrame: Grouped bedday pools
    """
    grouped = pd.concat(
        [
            ip_wards_bedday_pools.loc[
                ip_wards_bedday_pools.index.get_level_values("grouping").isin(groups),
                column,
            ]
            .groupby(level="model_run")
            .sum()
            .rename(new_group)
            for new_group, (groups, column) in WARD_GROUP_DEFINITIONS.items()
        ],
        axis=1,
    )
    grouped = pd.Series(grouped.stack()).rename("total").to_frame()
    grouped.index.names = ["model_run", "grouping"]
    return grouped


def preprocess_ip_wards_data(
    functional_areas: pd.DataFrame, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Preprocesses IP wards data for conversion to capacity.

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP wards
        assumptions_df (pd.DataFrame): DataFrame with required assumptions

    Returns:
        pd.DataFrame: Preprocessed IP maternity data for conversion to capacity
    """

    logger.info("Calculating IP wards bedday pools...")
    bedday_pools_list: list[pd.DataFrame] = []
    for grouping, assumptions_dict in WARD_WORKLOAD_ASSUMPTIONS_DICT.items():
        bedday_pools = derive_ward_beddays(
            grouping,
            functional_areas,
            assumptions_df,
            assumptions_dict,
        )
        bedday_pools_list.append(
            pd.DataFrame(bedday_pools)
            .assign(grouping=grouping)
            .set_index("grouping", append=True)
        )
    ip_wards_bedday_pools = pd.concat(bedday_pools_list)
    grouped_bedday_pools = group_ip_wards_beddays(ip_wards_bedday_pools)
    return grouped_bedday_pools


def calculate_ip_wards_capacity(
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
):
    """Converts functional areas into capacity requirements using supplied assumptions

        Args:
            functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
            Functional areas should first be processed with preprocess_ip_wards_data
            assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

        Returns:
    pd.DataFrame: DataFrame of calculated wards capacity requirements
    """
    logger.info("Calculating IP wards capacity")
    results_list = []
    for grouping, assumptions_dict in WARD_CAPACITY_ASSUMPTIONS_DICT.items():
        functional_area_subgroup = functional_areas_processed.xs(
            key=grouping, level="grouping"
        )["total"]
        operational_days = cast(
            float, assumptions_df.at[assumptions_dict["operational_days"], "Value"]
        )
        occupancy = cast(
            float, assumptions_df.at[assumptions_dict["occupancy"], "Value"]
        )
        capacity_df = pd.DataFrame(
            calculate_beds(functional_area_subgroup, operational_days, occupancy)
        )
        capacity_df.loc[:, "output"] = assumptions_dict["output"]
        capacity_df = capacity_df.reset_index().set_index(["output", "model_run"])
        results_list.append(capacity_df)
    return pd.concat(results_list)


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    return run_single_activity_type(
        "ip_wards",
        calculate_ip_wards_capacity,
        preprocess=preprocess_ip_wards_data,
    )


if __name__ == "__main__":
    sys.exit(main())
