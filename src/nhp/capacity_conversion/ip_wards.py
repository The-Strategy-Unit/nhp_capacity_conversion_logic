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
    summarise_functional_areas,
    validate_required_env_vars,
)

logger = get_logger()


def convert_ip_wards_capacity(
    daycase_spells: float,
    assumed_los_hours: float,
    operational_hours: float,
    operational_days: float,
    occupancy_rate: float,
) -> float:
    """Formula used for converting IP daycase functional area activity to capacity requirements

    Args:
        daycase_spells (float): Number of daycase spells
        assumed_los_hours (float): Indicative stay in hours
        operational_hours (float): Operational hours per day
        operational_days (float): Operational days per year
        occupancy_rate (float): Occupancy rate

    Returns:
        float: Calculated capacity requirement
    """
    return (daycase_spells * (assumed_los_hours)) / (
        operational_hours * operational_days * occupancy_rate
    )


def calculate_critical_care_beddays(
    functional_area_name: str,
    beddays_dict: dict[str, float],
    assumptions_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:

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
    beddays_0los = {k: float(v * indicative_los_hours) for k, v in beddays_dict.items()}
    return {name_0los_adjusted: beddays_0los}


def calculate_assessment_beddays(
    functional_area_name: str,
    beddays_dict: dict[str, float],
    assumptions_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    key = (
        "adult_assessment_bed_hours"
        if "adult" in functional_area_name
        else "paediatric_assessment_bed_hours"
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
    return bedday_pools


def calculate_critical_care_beds():
    pass


def calculate_ip_wards_capacity(
    functional_areas_summarised: dict, assumptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts p10, p90 and mean for functional areas into capacity requirements using supplied assumptions

    Args:
        functional_areas_summarised (dict): Dict with p10, p90 and mean for each of the functional areas
        assumptions_df (pd.DataFrame): DataFrame with required assumptions for calculating capacity

    Returns:
        pd.DataFrame: DataFrame of calculated IP daycase capacity requirements
    """
    logger.info("Calculating IP daycase capacity")
    results_dict = {}

    return calculate_separate_bedday_pools(
        functional_areas_summarised, assumptions_df
    )  # return dict with all the different types of beddays for critical care, assessment, ward
    # Calculate critical care beddays first
    # Calculate assessment beddays
    # Calculate ward beddays

    # convert_ip_cc_capacity()
    # convert_ip_assessment_capacity()
    # convert_ip_wards_capacity()
    # # Calculate critical care beds
    # # Calculate assessment beds
    # # Calculate ward beds

    # for capacity_requirement in [
    #     "adult_surgical_daycase",
    #     "adult_medical_daycase",
    #     "paediatric_surgical_daycase",
    #     "paediatric_medical_daycase",
    # ]:
    #     results = {}
    #     assumed_los_hours = cast(
    #         float,
    #         assumptions_df.at[capacity_requirement + "_stay_hours", "assumption_value"],
    #     )

    #     operational_hours = cast(
    #         float,
    #         assumptions_df.at[
    #             capacity_requirement.split("_")[0] + "_daycase_operational_hours",
    #             "assumption_value",
    #         ],
    #     )
    #     operational_days = cast(
    #         float,
    #         assumptions_df.at[
    #             capacity_requirement.split("_")[0] + "_daycase_operational_days",
    #             "assumption_value",
    #         ],
    #     )
    #     occupancy_rate = cast(
    #         float,
    #         assumptions_df.at[
    #             capacity_requirement.split("_")[0] + "_daycase_recovery_occupancy_rate",
    #             "assumption_value",
    #         ],
    #     )
    #     for value in ["p10", "mean", "p90"]:
    #         results[value] = convert_ip_wards_capacity(
    #             functional_areas_summarised[capacity_requirement][value],
    #             assumed_los_hours,
    #             operational_hours,
    #             operational_days,
    #             occupancy_rate,
    #         )
    #     results_dict[capacity_requirement] = results
    # ip_daycase_capacity = pd.DataFrame.from_dict(results_dict, orient="index")
    # ip_daycase_capacity.index = [i + "_beds" for i in ip_daycase_capacity.index]
    # return ip_daycase_capacity


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
    print(calculate_separate_bedday_pools(functional_areas_summarised, assumptions))
    # ip_wards_capacity_df = calculate_ip_wards_capacity(
    #     functional_areas_summarised, assumptions
    # )
    # data_to_save["ip_wards_capacity"] = ip_wards_capacity_df
    # save_results_to_excel(data_to_save)


if __name__ == "__main__":
    sys.exit(main())
