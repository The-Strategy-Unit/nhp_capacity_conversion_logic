import argparse
import sys
from datetime import datetime
from logging import INFO
from typing import cast

import pandas as pd
from nhpy.utils import (
    configure_logging,
    get_logger,
)

from nhp.capacity_conversion.utils import (
    create_aggregations_path,
    load_aggregations,
    load_assumptions,
    load_metadata_from_ats,
    save_results_to_excel,
    summarise_functional_areas,
    validate_required_env_vars,
)

logger = get_logger()


def calculate_critical_care_beddays(
    functional_area_name: str,
    beddays_dict: dict[str, float],
    assumptions_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """Calculates critical care beddays from the IP wards functional areas

    Args:
        functional_area_name (str): Functional area to be converted to critical care beddays
        beddays_dict (dict[str, float]): Dictionary of values, usually with the keys "p10", "mean" and "p90"
        assumptions_df (pd.DataFrame): DataFrame of assumptions to use,
        including adult_cc_beddays_proportion and paediatric_cc_beddays_proportion values

    Returns:
        dict[str, dict[str, float]]: Dictionary with calculated critical care beddays for the given functional area
    """

    key = (
        "adult_cc_beddays_proportion"
        if "adult" in functional_area_name
        else "paediatric_cc_beddays_proportion"
    )
    cc_beddays_proportion = cast(
        float,
        assumptions_df.at[key, "assumption_value"],
    )
    cc_name = functional_area_name.replace("beddays", "cc_beddays")
    cc_beddays = {k: float(v * cc_beddays_proportion) for k, v in beddays_dict.items()}
    return {cc_name: cc_beddays}


def calculate_0los_beddays(
    functional_area_name: str,
    beddays_dict: dict[str, float],
    assumptions_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """Calculates 0 LOS beddays from the IP wards functional areas

    Args:
        functional_area_name (str): Functional area to be converted to 0 LOS beddays
        beddays_dict (dict[str, float]): Dictionary of values, usually with the keys "p10", "mean" and "p90"
        assumptions_df (pd.DataFrame): DataFrame of assumptions to use,
        including adult_0los_indicative_los_hours and paediatric_0los_indicative_los_hours values

    Returns:
        dict[str, dict[str, float]]: Dictionary with calculated 0 LOS beddays for the given functional area
    """
    key = (
        "adult_0los_indicative_los_hours"
        if "adult" in functional_area_name
        else "paediatric_0los_indicative_los_hours"
    )
    indicative_los_hours = cast(
        float,
        assumptions_df.at[key, "assumption_value"],
    )
    name_0los_adjusted = functional_area_name.replace("spells", "beddays")
    beddays_0los = {
        k: float(v * (indicative_los_hours / 24)) for k, v in beddays_dict.items()
    }
    return {name_0los_adjusted: beddays_0los}


def calculate_assessment_beddays(
    functional_area_name: str,
    beddays_dict: dict[str, float],
    assumptions_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """Calculates assessment beddays from the IP wards functional areas

       Args:
           functional_area_name (str): Functional area to be converted to assessment beddays
           beddays_dict (dict[str, float]): Dictionary of values, usually with the keys "p10", "mean" and "p90"
           assumptions_df (pd.DataFrame): DataFrame of assumptions to use,
           including adult_elective_medical_assessment_hours, adult_elective_surgical_assessment_hours,
           adult_nonelective_medical_assessment_hours, adult_nonelective_surgical_assessment_hours,
           paediatric_elective_medical_assessment_hours, paediatric_elective_surgical_assessment_hours,
           paediatric_nonelective_medical_assessment_hours, and paediatric_nonelective_surgical_assessment_hours


    values

       Returns:
           dict[str, dict[str, float]]: Dictionary with calculated assessment beddays for the given functional area
    """
    key = functional_area_name.replace("spells", "assessment_hours").replace(
        "_0los", ""
    )
    assessment_los_hours = cast(
        float,
        assumptions_df.at[key, "assumption_value"],
    )
    name_adjusted = functional_area_name.replace("spells", "assessment_beddays")
    assessment_beddays = {
        k: float((v * assessment_los_hours) / 24) for k, v in beddays_dict.items()
    }
    return {name_adjusted: assessment_beddays}


def calculate_ward_beddays(functional_areas_summarised, bedday_pools):
    ip_ward_beddays_dict = {}
    functional_areas = [
        "adult_elective_medical",
        "adult_nonelective_medical",
        "adult_elective_surgical",
        "adult_nonelective_surgical",
        "paediatric_elective_medical",
        "paediatric_nonelective_medical",
        "paediatric_elective_surgical",
        "paediatric_nonelective_surgical",
    ]

    for f_a in functional_areas:
        results = {}
        for value in ["p10", "mean", "p90"]:
            total_adjusted_beddays = (
                functional_areas_summarised[f_a + "_beddays"][value]
                + bedday_pools[f_a + "_0los_beddays"][value]
            )
            assessment_beddays = bedday_pools.get(
                f_a + "_assessment_beddays", {"p10": 0, "mean": 0, "p90": 0}
            )[value]  # elective activity will not have any assesssment beddays
            cc_beddays = bedday_pools[f_a + "_cc_beddays"][value]
            ward_beddays = total_adjusted_beddays - assessment_beddays - cc_beddays
            results[value] = ward_beddays
        ip_ward_beddays_dict[f_a + "_ward_beddays"] = results
    return ip_ward_beddays_dict


def calculate_separate_bedday_pools(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
):

    bedday_pools = {}
    for functional_area, functional_area_dict in functional_areas_summarised.items():
        # critical care
        if "0los" not in functional_area and functional_area.endswith("beddays"):
            bedday_pools.update(
                calculate_critical_care_beddays(
                    functional_area, functional_area_dict, assumptions_df
                )
            )
        # 0 los
        if "0los" in functional_area and functional_area.endswith("spells"):
            bedday_pools.update(
                calculate_0los_beddays(
                    functional_area, functional_area_dict, assumptions_df
                )
            )
        # assessment
        if "nonelective" in functional_area and functional_area.endswith("spells"):
            bedday_pools.update(
                calculate_assessment_beddays(
                    functional_area, functional_area_dict, assumptions_df
                )
            )
    # ward beds
    bedday_pools.update(
        calculate_ward_beddays(functional_areas_summarised, bedday_pools)
    )
    return pd.DataFrame.from_dict(bedday_pools, orient="index")


def group_bedday_pools(bedday_pools):
    # We have separate rows for 0los_assessment_beddays and _assessment_beddays which need to be combined
    assessment_only = bedday_pools[bedday_pools.index.str.contains("assessment")]
    all_other_bedday_pools = bedday_pools[
        ~bedday_pools.index.str.contains("assessment")
    ]

    new_rows = {}

    for idx in assessment_only.index:
        if idx.endswith("_0los_assessment_beddays"):
            prefix = idx.replace("_0los_assessment_beddays", "")
            base_idx = f"{prefix}_assessment_beddays"
            total_idx = f"{prefix}_total_assessment_beddays"

            if base_idx in assessment_only.index:
                new_rows[total_idx] = (
                    assessment_only.loc[idx] + assessment_only.loc[base_idx]
                )

    # Append new rows
    new_bedday_pools = pd.concat(
        [all_other_bedday_pools, pd.DataFrame.from_dict(new_rows, orient="index")]
    )
    # We don't need 0los_beddays anymore as they should already be in the ward_beddays
    new_bedday_pools = new_bedday_pools[~new_bedday_pools.index.str.contains("0los")]
    return new_bedday_pools


def convert_ip_beddays_to_beds(
    total_beddays: float, operational_days_per_year: float, occupancy_rate: float
) -> float:
    """Formula used for converting IP wards functional area activity to capacity requirements

    Args:
        total_beddays (float): Number of beddays (speldur, NOT beddays + 1)
        operational_days_per_year (float): Operational days per year
        occupancy_rate (float): Occupancy rate

    Returns:
        float: Calculated beds required
    """
    return total_beddays / (operational_days_per_year * occupancy_rate)


def calculate_ip_wards_capacity(
    bedday_pools: pd.DataFrame, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    logger.info("Calculating IP critical care, assessment, and wards capacity")
    grouped_bedday_pools = group_bedday_pools(bedday_pools)
    bedday_pools_dict = grouped_bedday_pools.to_dict(orient="index")
    results_dict = {}
    for bedday_pool, values_dict in bedday_pools_dict.items():
        results = {}
        bedday_pool = str(bedday_pool)
        capacity_name = bedday_pool.replace("_beddays", "_beds").replace("_total", "")
        # which assumptions to use?
        if bedday_pool.startswith("adult"):
            lookup = "adult_"
        else:
            lookup = "paediatric_"
        if "_cc_" in bedday_pool:
            lookup += "cc_"
        elif "assessment" in bedday_pool:
            lookup += "assessment_"
        elif "ward" in bedday_pool:
            lookup += "ward_"
        operational_days = cast(
            float,
            assumptions_df.at[lookup + "operational_days", "assumption_value"],
        )
        occupancy_rate = cast(
            float,
            assumptions_df.at[lookup + "occupancy_rate", "assumption_value"],
        )
        for value in ["p10", "mean", "p90"]:
            results[value] = convert_ip_beddays_to_beds(
                bedday_pools_dict[bedday_pool][value],
                operational_days,
                occupancy_rate,
            )
        results_dict[capacity_name] = results
    ip_wards_capacity = pd.DataFrame.from_dict(results_dict, orient="index")
    return ip_wards_capacity


def main():
    """
    CLI entry point when module is run directly.

    Returns:
        int: Exit code (0 for success, 2 for errors)
    """
    configure_logging(INFO)
    capacity_conversion_runtime = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Generate IP capacity outputs given functional area aggregations of IP activity"
    )
    parser.add_argument(
        "guid",
        help="GUID of functional area aggregation to convert into capacity",
    )
    parser.add_argument(
        "--capacity_model_version",
        help="Capacity model version",
        default="dev",
    )
    parser.add_argument(
        "--path_to_assumptions_file",
        help="Path to assumptions file (default: 'data/reference/default_assumptions.csv')",
        default="data/reference/default_assumptions.csv",
    )
    args = parser.parse_args()
    config = validate_required_env_vars()
    data_to_save = {}
    metadata = load_metadata_from_ats(
        args.guid,
        config["AZ_TABLE_ENDPOINT"],
        config["TABLE_NAME"],
        args.capacity_model_version,
    )
    metadata["capacity_conversion_runtime"] = capacity_conversion_runtime
    data_to_save["metadata"] = pd.Series(metadata).drop(["PartitionKey", "RowKey"])
    assumptions = load_assumptions(args.path_to_assumptions_file)
    data_to_save["assumptions"] = assumptions
    aggregations_path = create_aggregations_path(metadata)
    ip_wards_aggregations = load_aggregations(
        config["AZ_STORAGE_EP"],
        config["AZ_STORAGE_RESULTS"],
        aggregations_path,
        "ip_wards",
    )
    functional_areas_summarised = summarise_functional_areas(ip_wards_aggregations)
    data_to_save["ip_wards_functional_areas"] = pd.DataFrame.from_dict(
        functional_areas_summarised, orient="index"
    )
    bedday_pools = calculate_separate_bedday_pools(
        functional_areas_summarised, assumptions
    )
    data_to_save["calculated_bedday_pools"] = bedday_pools
    ip_wards_capacity_df = calculate_ip_wards_capacity(bedday_pools, assumptions)
    data_to_save["ip_wards_capacity"] = ip_wards_capacity_df
    save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
