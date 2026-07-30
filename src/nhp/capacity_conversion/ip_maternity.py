import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pandas as pd
from nhpy.utils import get_logger
from numpy import float64

from nhp.capacity_conversion.ip_formulas import (
    calculate_beds,
    calculate_recovery_capacity,
    calculate_time_util_capacity,
    derive_beddays_from_spells,
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


def derive_birth_related_ward_beddays(
    grouping: str,
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    assumptions: dict[str, str],
) -> pd.Series:
    """Calculate birth related maternity ward beddays

    Args:
        grouping (str): Name of functional area grouping
        functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
        Functional areas should first be processed with preprocess_ip_maternity_data
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity
        assumptions (dict[str, str]): Assumptions dictionary for the specific grouping

    Returns:
        pd.Series: Calculated birth related ward beddays
    """
    zero_day_los = cast(
        float,
        assumptions_df.at[assumptions["zero_day_los"], "Value"],
    )
    zero_day_beddays = derive_beddays_from_spells(
        functional_areas_processed.xs(key=grouping + "_zerolos", level="grouping")[
            "spells"
        ],
        zero_day_los,
    )
    if grouping != "maternity_elective_csection":
        birthroom_los = cast(
            float,
            assumptions_df.at[assumptions["birthroom_los"], "Value"],
        )
        birth_room_beddays = derive_beddays_from_spells(
            functional_areas_processed.xs(key=grouping, level="grouping")["spells"],
            birthroom_los,
        )
    else:
        # elective csections do not spend any time in the birth room
        birth_room_beddays = 0
    birth_spell_overnight_beddays = functional_areas_processed.xs(
        key=grouping + "_nonzerolos", level="grouping"
    )["beddays"]
    return birth_spell_overnight_beddays + zero_day_beddays - birth_room_beddays


def derive_total_maternity_ward_beddays(
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    assumptions_dict: dict[str, dict[str, str]],
) -> pd.Series:
    """Calculate total maternity ward beddays

    Args:
        functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
        Functional areas should first be processed with preprocess_ip_maternity_data
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity
        assumptions_dict (dict[str, dict[str, str]]): Maternity wards assumptions dictionary

    Returns:
        pd.Series: Calculated total maternity ward beddays
    """
    birth_related_ward_beddays = pd.Series(dtype=float64)
    for grouping in [
        "maternity_normal_delivery",
        "maternity_assisted_delivery",
        "maternity_elective_csection",
        "maternity_nonelective_csection",
    ]:
        birth_related_ward_beddays = birth_related_ward_beddays.add(
            derive_birth_related_ward_beddays(
                grouping,
                functional_areas_processed,
                assumptions_df,
                assumptions_dict[grouping],
            ),
            fill_value=0,
        )
    no_birth_ward_beddays = functional_areas_processed.xs(
        key="maternity_overnight_no_birth", level="grouping"
    )["beddays"]
    return birth_related_ward_beddays + no_birth_ward_beddays


def calculate_maternity_ward_beds(
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    assumptions_dict: dict[str, dict[str, str]],
):
    """Calculate maternity ward beds

    Args:
        functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
        Functional areas should first be processed with preprocess_ip_maternity_data
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity
        assumptions_dict (dict[str, dict[str, str]]): Maternity wards assumptions dictionary

    Returns:
        pd.Series: Calculated total maternity ward beddays
    """
    total_ward_beddays = derive_total_maternity_ward_beddays(
        functional_areas_processed, assumptions_df, assumptions_dict
    )
    maternity_ward_occupancy = cast(
        float,
        assumptions_df.at["MATERNITY_WARD_OCC", "Value"],
    )
    maternity_ward_operational_days = cast(
        float,
        assumptions_df.at["MATERNITY_WARD_ANNUAL_OPERATIONAL_DAYS", "Value"],
    )
    maternity_ward_beds = calculate_beds(
        total_ward_beddays, maternity_ward_operational_days, maternity_ward_occupancy
    ).to_frame(name="total")
    maternity_ward_beds.loc[:, "output"] = "MATERNITY_WARD_BEDS"
    results = maternity_ward_beds.reset_index().set_index(["output", "model_run"])
    return results


ward_assumptions_dict = {
    "maternity_normal_delivery": {
        "zero_day_los": "MATERNITY_WARD_NORMAL_DELIVERY_ZERO_DAY_LOS",
        "birthroom_los": "MATERNITY_NORMAL_DELIVERY_BIRTH_ROOM_LOS",
    },
    "maternity_assisted_delivery": {
        "zero_day_los": "MATERNITY_WARD_ASSISTED_DELIVERY_ZERO_DAY_LOS",
        "birthroom_los": "MATERNITY_ASSISTED_DELIVERY_BIRTH_ROOM_LOS",
    },
    "maternity_elective_csection": {
        "zero_day_los": "MATERNITY_WARD_ELECTIVE_C_SECTION_ZERO_DAY_LOS",
    },
    "maternity_nonelective_csection": {
        "zero_day_los": "MATERNITY_WARD_NON_ELECTIVE_C_SECTION_ZERO_DAY_LOS",
        "birthroom_los": "MATERNITY_NON_ELECTIVE_C_SECTION_BIRTH_ROOM_LOS",
    },
}


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


def process_maternity_birth_data(
    functional_areas: pd.DataFrame,
) -> pd.DataFrame:
    """Combine activity for zero los and nonzero los normal, assisted, and nonelective csection
    functional area groupings for calculation of birth rooms.

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP maternity

    Returns:
        pd.DataFrame: IP maternity functional areas with new groupings
    """
    df_list = []
    groups_list = [
        ["maternity_normal_delivery_zerolos", "maternity_normal_delivery_nonzerolos"],
        [
            "maternity_assisted_delivery_zerolos",
            "maternity_assisted_delivery_nonzerolos",
        ],
        [
            "maternity_nonelective_csection_zerolos",
            "maternity_nonelective_csection_nonzerolos",
        ],
    ]
    for groups in groups_list:
        df_list.append(
            functional_areas[functional_areas["grouping"].isin(groups)]
            .groupby(level=0)
            .sum()
            .assign(grouping="_".join(groups[0].split("_")[:-1]))
        )
    result = pd.concat([functional_areas] + df_list).sort_index()
    return result


def preprocess_ip_maternity_data(functional_areas: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses IP maternity data for conversion to capacity.

    Args:
        functional_areas (pd.DataFrame): Functional areas from Azure for IP maternity

    Returns:
        pd.DataFrame: Preprocessed IP maternity data for conversion to capacity
    """
    functional_areas = process_theatres_obstetric_proc_data(functional_areas)
    functional_areas = process_maternity_birth_data(functional_areas)
    return functional_areas


def calculate_maternity_birth_rooms(
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
"""Calculates capacity requirements for maternity birth rooms using the FRM_BED_OCCUPANCY conversion archetype

    Args:
        assumptions (dict[str, str]): Mapping of assumption name to use for the specific subgroup
        functional_area_subgroup (pd.Series): Functional area groupings in a Pandas Series, with the index name model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: Calculated capacity requirements for the specific subgroup
    """

    los = cast(float, assumptions_df.at[assumptions["birthroom_los"], "Value"])
    occupancy = cast(
        float,
        assumptions_df.at[assumptions["birthroom_occupancy"], "Value"],
    )
    operational_days = cast(
        float,
        assumptions_df.at[
            assumptions["birthroom_operational_days"],
            "Value",
        ],
    )
    output = assumptions["output"]
    birthroom_beddays = derive_beddays_from_spells(functional_area_subgroup, los)
    results = pd.DataFrame(
        calculate_beds(birthroom_beddays, operational_days, occupancy)
    )
    results.loc[:, "output"] = output
    results = results.reset_index().set_index(["output", "model_run"])
    return results


def calculate_theatres_obstetric_proc(
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for obstetric theatres using the FRM_TIME_UTIL conversion archetype

    Args:
        assumptions (dict[str, str]): Mapping of assumption name to use for the specific subgroup
        functional_area_subgroup (pd.Series): Functional area groupings in a Pandas Series, with the index name model_run
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: Calculated capacity requirements for the specific subgroup
    """

    time = cast(float, assumptions_df.at[assumptions["procedure_time"], "Value"])
    utilisation = cast(
        float,
        assumptions_df.at[assumptions["theatre_utilisation"], "Value"],
    )
    annual_operational_hours = cast(
        float,
        assumptions_df.at[
            assumptions["theatre_annual_operational_hours"],
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
    assumptions: dict[str, str],
    functional_area_subgroup: pd.Series,
    assumptions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates capacity requirements for maternity assessment beds using the FRM_RECOVERY_OCCUPANCY conversion archetype

    Args:
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
            "procedure_time": "OBSTETRIC_THEATRE_PROC_TIME",
            "theatre_utilisation": "OBSTETRIC_THEATRE_UTIL",
            "theatre_annual_operational_hours": "OBSTETRIC_THEATRE_ANNUAL_OPERATIONAL_HOURS",
        },
    ),
    "NORMAL_DELIVERY_MATERNITY_BIRTH_ROOMS": MaternityConfig(
        subgroup="maternity_normal_delivery",
        col_to_use="spells",
        formula=calculate_maternity_birth_rooms,
        assumptions={
            "birthroom_los": "MATERNITY_NORMAL_DELIVERY_BIRTH_ROOM_LOS",
            "birthroom_occupancy": "MATERNITY_BIRTH_ROOM_OCC",
            "birthroom_operational_days": "MATERNITY_BIRTH_ROOM_ANNUAL_OPERATIONAL_DAYS",
            "output": "NORMAL_DELIVERY_MATERNITY_BIRTH_ROOMS",
        },
    ),
    "ASSISTED_DELIVERY_MATERNITY_BIRTH_ROOMS": MaternityConfig(
        subgroup="maternity_assisted_delivery",
        col_to_use="spells",
        formula=calculate_maternity_birth_rooms,
        assumptions={
            "birthroom_los": "MATERNITY_ASSISTED_DELIVERY_BIRTH_ROOM_LOS",
            "birthroom_occupancy": "MATERNITY_BIRTH_ROOM_OCC",
            "birthroom_operational_days": "MATERNITY_BIRTH_ROOM_ANNUAL_OPERATIONAL_DAYS",
            "output": "ASSISTED_DELIVERY_MATERNITY_BIRTH_ROOMS",
        },
    ),
    "NON_ELECTIVE_C_SECTION_MATERNITY_BIRTH_ROOMS": MaternityConfig(
        subgroup="maternity_nonelective_csection",
        col_to_use="spells",
        formula=calculate_maternity_birth_rooms,
        assumptions={
            "birthroom_los": "MATERNITY_NON_ELECTIVE_C_SECTION_BIRTH_ROOM_LOS",
            "birthroom_occupancy": "MATERNITY_BIRTH_ROOM_OCC",
            "birthroom_operational_days": "MATERNITY_BIRTH_ROOM_ANNUAL_OPERATIONAL_DAYS",
            "output": "NON_ELECTIVE_C_SECTION_MATERNITY_BIRTH_ROOMS",
        },
    ),
}


def calculate_maternity_capacity(
    functional_areas_processed: pd.DataFrame,
    assumptions_df: pd.DataFrame,
    config=MATERNITY_CONFIG,
) -> pd.DataFrame:
    """Converts functional areas into capacity requirements using supplied assumptions

        Args:
            functional_areas_processed (pd.DataFrame): Functional area groupings in a MultiIndex dataframe, with the index names grouping and model_run.
            Functional areas should first be processed with preprocess_ip_maternity_data
            assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

        Returns:
    pd.DataFrame: DataFrame of calculated maternity capacity requirements
    """
    logger.info("Calculating IP maternity capacity")
    results_list = []
    for subgroup_config in config.values():
        functional_area_subgroup = functional_areas_processed.xs(
            key=subgroup_config.subgroup, level="grouping"
        )[subgroup_config.col_to_use]
        results_list.append(
            subgroup_config.formula(
                assumptions=subgroup_config.assumptions,
                functional_area_subgroup=functional_area_subgroup,
                assumptions_df=assumptions_df,
            ).rename(columns={subgroup_config.col_to_use: "total"})
        )
    results_list.append(
        calculate_maternity_ward_beds(
            functional_areas_processed, assumptions_df, ward_assumptions_dict
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
