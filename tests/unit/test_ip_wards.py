import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_wards import (
    calculate_0los_beddays,
    calculate_assessment_beddays,
    calculate_critical_care_beddays,
    calculate_ip_wards_capacity,
    calculate_separate_bedday_pools,
    calculate_ward_beddays,
    convert_ip_beddays_to_beds,
    group_bedday_pools,
    main,
)


@pytest.mark.parametrize(
    argnames="functional_area_name, expected_new_name, expected_result",
    argvalues=[
        (
            "paediatric_nonelective_surgical_beddays",
            "paediatric_nonelective_surgical_cc_beddays",
            0.2,
        ),
        ("adult_elective_medical_beddays", "adult_elective_medical_cc_beddays", 0.1),
    ],
)
def test_calculate_critical_care_beddays(
    functional_area_name, expected_new_name, expected_result
):
    # arrange
    beddays_dict = {"value": 1.0}
    assumptions_df = pd.DataFrame(
        {"assumption_value": [0.1, 0.2]},
        index=["adult_cc_beddays_proportion", "paediatric_cc_beddays_proportion"],
    )
    # act
    results_dict = calculate_critical_care_beddays(
        functional_area_name, beddays_dict, assumptions_df
    )
    expected = {expected_new_name: {"value": expected_result}}
    # assert
    assert results_dict == expected


@pytest.mark.parametrize(
    argnames="functional_area_name, expected_new_name, expected_result",
    argvalues=[
        (
            "paediatric_elective_medical_spells",
            "paediatric_elective_medical_assessment_beddays",
            3 / 24,
        ),
        (
            "adult_nonelective_surgical_0los_spells",
            "adult_nonelective_surgical_0los_assessment_beddays",
            2 / 24,
        ),
    ],
)
def test_calculate_assessment_beddays(
    functional_area_name, expected_new_name, expected_result
):

    # arrange
    beddays_dict = {"value": 1.0}
    assumptions_df = pd.DataFrame(
        {"assumption_value": [2, 3]},
        index=[
            "adult_nonelective_surgical_assessment_hours",
            "paediatric_elective_medical_assessment_hours",
        ],
    )
    # act
    results_dict = calculate_assessment_beddays(
        functional_area_name, beddays_dict, assumptions_df
    )
    expected = {expected_new_name: {"value": expected_result}}
    # assert
    assert results_dict == expected


@pytest.mark.parametrize(
    argnames="functional_area_name, expected_new_name, expected_result",
    argvalues=[
        (
            "paediatric_elective_medical_0los_spells",
            "paediatric_elective_medical_0los_beddays",
            3 / 24,
        ),
        (
            "adult_nonelective_surgical_0los_spells",
            "adult_nonelective_surgical_0los_beddays",
            2 / 24,
        ),
    ],
)
def test_calculate_0los_beddays(
    functional_area_name, expected_new_name, expected_result
):

    # arrange
    beddays_dict = {"value": 1.0}
    assumptions_df = pd.DataFrame(
        {"assumption_value": [2, 3]},
        index=[
            "adult_0los_indicative_los_hours",
            "paediatric_0los_indicative_los_hours",
        ],
    )
    # act
    results_dict = calculate_0los_beddays(
        functional_area_name, beddays_dict, assumptions_df
    )
    expected = {expected_new_name: {"value": expected_result}}
    # assert
    assert results_dict == expected


def test_convert_ip_beddays_to_beds():
    # arrange
    total_beddays = 10000
    operational_days = 365
    occupancy_rate = 0.8

    expected = 10000 / (365 * 0.8)

    # act
    actual = convert_ip_beddays_to_beds(
        total_beddays,
        operational_days,
        occupancy_rate,
    )
    # assert
    assert actual == expected


def test_calculate_ward_beddays():
    # arrange
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
    values_dict = {"p10": 100, "mean": 100, "p90": 100}
    functional_areas_summarised = {
        f_a + "_beddays": values_dict for f_a in functional_areas
    }
    # 0los first
    bedday_pools = {f_a + "_0los_beddays": values_dict for f_a in functional_areas}
    # assessment first
    assessment = {
        f_a + "_assessment_beddays": values_dict
        for f_a in functional_areas
        if "nonelective" in f_a
    }
    bedday_pools.update(assessment)
    # add critical care
    critical_care = {f_a + "_cc_beddays": values_dict for f_a in functional_areas}
    bedday_pools.update(critical_care)
    results_dict_nonelective = {"p10": 0, "mean": 0, "p90": 0}
    results_dict_elective = {"p10": 100, "mean": 100, "p90": 100}
    expected = {
        f_a + "_ward_beddays": (
            results_dict_nonelective if "nonelective" in f_a else results_dict_elective
        )
        for f_a in functional_areas
    }
    # act
    actual = calculate_ward_beddays(functional_areas_summarised, bedday_pools)
    # assert
    assert actual == expected


def test_calculate_separate_bedday_pools(mocker):
    # arrange
    mock_dict = {"value": 100}
    functional_areas = [
        "adult_elective_medical_0los_beddays",
        "adult_elective_medical_0los_spells",
        "adult_nonelective_medical_beddays",
        "adult_nonelective_medical_spells",
    ]
    assumptions_df = pd.DataFrame()
    functional_areas_summarised = {f_a: mock_dict for f_a in functional_areas}
    mock_cc = mocker.patch(
        "nhp.capacity_conversion.ip_wards.calculate_critical_care_beddays",
        return_value={"cc": mock_dict},
    )
    mock_0los = mocker.patch(
        "nhp.capacity_conversion.ip_wards.calculate_0los_beddays",
        return_value={"0los": mock_dict},
    )
    mock_assessment = mocker.patch(
        "nhp.capacity_conversion.ip_wards.calculate_assessment_beddays",
        return_value={"assessment": mock_dict},
    )
    mock_ward = mocker.patch(
        "nhp.capacity_conversion.ip_wards.calculate_ward_beddays",
        return_value={"ward": mock_dict},
    )
    bedday_pools_dict = {k: mock_dict for k in ["cc", "0los", "assessment", "ward"]}
    # act
    actual = calculate_separate_bedday_pools(
        functional_areas_summarised, assumptions_df
    )
    # assert
    mock_cc.assert_called_once_with(
        "adult_nonelective_medical_beddays", mock_dict, assumptions_df
    )
    mock_0los.assert_called_once_with(
        "adult_elective_medical_0los_spells", mock_dict, assumptions_df
    )
    mock_assessment.assert_called_once_with(
        "adult_nonelective_medical_spells", mock_dict, assumptions_df
    )
    mock_ward.assert_called_once_with(functional_areas_summarised, bedday_pools_dict)
    expected = pd.DataFrame(
        {"value": [100] * 4}, index=["0los", "cc", "assessment", "ward"]
    )
    assert_frame_equal(actual, expected)


def test_group_bedday_pools():
    # arrange
    bedday_pools = pd.DataFrame(
        {"value": [1, 2, 3, 4]},
        index=[
            "a_0los_assessment_beddays",
            "a_assessment_beddays",
            "a_cc_beddays",
            "b_0los_beddays",
        ],
    )
    expected = pd.DataFrame(
        {"value": [3, 3]},
        index=[
            "a_total_assessment_beddays",
            "a_cc_beddays",
        ],
    )
    # act
    actual = group_bedday_pools(bedday_pools)

    # assert
    assert_frame_equal(actual.sort_index(), expected.sort_index())


def test_calculate_ip_wards_capacity(mocker):
    # arrange
    bedday_pools = pd.DataFrame(
        {k: [1, 1, 1] for k in ["p10", "mean", "p90"]},
        index=[
            "adult_elective_medical_ward_beddays",
            "paediatric_nonelective_surgical_cc_beddays",
            "adult_nonelective_medical_assessment_beddays",
        ],
    )
    assumptions_df = pd.DataFrame(
        {"assumption_value": [1, 2, 3, 4, 5, 6]},
        index=[
            "adult_ward_operational_days",
            "adult_ward_occupancy_rate",
            "paediatric_cc_operational_days",
            "paediatric_cc_occupancy_rate",
            "adult_assessment_operational_days",
            "adult_assessment_occupancy_rate",
        ],
    )

    mock_group = mocker.patch(
        "nhp.capacity_conversion.ip_wards.group_bedday_pools",
        return_value=bedday_pools,
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_wards.convert_ip_beddays_to_beds",
        return_value=1,
    )
    expected = pd.DataFrame(
        {"p10": [1] * 3, "mean": [1] * 3, "p90": [1] * 3},
        index=[
            "adult_elective_medical_ward_beds",
            "paediatric_nonelective_surgical_cc_beds",
            "adult_nonelective_medical_assessment_beds",
        ],
    )
    # act
    actual = calculate_ip_wards_capacity(bedday_pools, assumptions_df)
    # assert
    mock_group.assert_called_once_with(bedday_pools)
    assert mock_convert.call_count == 9
    assert_frame_equal(actual, expected)


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.ip_wards"

    mock_now = mocker.Mock()
    mock_now.strftime.return_value = "20250101_120000"
    mocker.patch(f"{module_path}.datetime").now.return_value = mock_now

    mock_parser = mocker.Mock()
    mock_args = mocker.Mock()
    mock_args.guid = "GUID123"
    mock_args.path_to_assumptions_file = "assumptions.csv"
    mock_args.capacity_model_version = "dev"

    mock_parser.parse_args.return_value = mock_args
    mocker.patch(f"{module_path}.argparse.ArgumentParser", return_value=mock_parser)
    env_vars_dict = {
        "AZ_STORAGE_EP": "AZ_STORAGE_EP",
        "AZ_STORAGE_RESULTS": "AZ_STORAGE_RESULTS",
        "TABLE_NAME": "TABLE_NAME",
        "AZ_TABLE_ENDPOINT": "AZ_TABLE_ENDPOINT",
    }
    mocker.patch(
        f"{module_path}.validate_required_env_vars", return_value=env_vars_dict
    )
    metadata_dict = {
        "PartitionKey": "PartitionKey",
        "RowKey": "RowKey",
        "guid": "GUID123",
        "capacity_model_version": "dev",
    }
    mocker.patch(
        f"{module_path}.load_metadata_from_ats",
        return_value=metadata_dict,
    )
    mocker.patch(
        f"{module_path}.create_aggregations_path", return_value="aggregations_path"
    )

    mock_assumptions = pd.DataFrame()
    mocker.patch(f"{module_path}.load_assumptions", return_value=mock_assumptions)

    mock_aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "total": [3] * 3 + [4] * 3 + [5] * 3,
        }
    )
    mocker.patch(f"{module_path}.load_aggregations", return_value=mock_aggregations)

    mock_functional_summary = {"area": {"mean": 1}}
    mocker.patch(
        f"{module_path}.summarise_functional_areas",
        return_value=mock_functional_summary,
    )

    mocker.patch(
        f"{module_path}.calculate_separate_bedday_pools",
        return_value=mock_functional_summary,
    )

    mock_capacity_df = pd.DataFrame({"mean": [1]})
    mocker.patch(
        f"{module_path}.calculate_ip_wards_capacity",
        return_value=mock_capacity_df,
    )

    mock_save = mocker.patch(f"{module_path}.save_results_to_excel")

    # act

    main()

    # assert

    module = __import__(module_path, fromlist=["dummy"])
    module.load_metadata_from_ats.assert_called_once_with(
        "GUID123", "AZ_TABLE_ENDPOINT", "TABLE_NAME", "dev"
    )
    module.load_assumptions.assert_called_once_with("assumptions.csv")
    module.create_aggregations_path.assert_called_once_with(metadata_dict)
    module.load_aggregations.assert_called_once_with(
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_wards"
    )
    module.summarise_functional_areas.assert_called_once_with(mock_aggregations)
    module.calculate_separate_bedday_pools.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    module.calculate_ip_wards_capacity.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    mock_save.assert_called_once()
    mock_data_to_save = mock_save.call_args_list[0].args[0]
    assert_series_equal(
        mock_data_to_save["metadata"],
        pd.Series(
            {
                "guid": "GUID123",
                "capacity_model_version": "dev",
                "capacity_conversion_runtime": "20250101_120000",
            }
        ),
    )
    assert_frame_equal(mock_data_to_save["assumptions"], pd.DataFrame())
    assert_frame_equal(
        mock_data_to_save["ip_wards_functional_areas"],
        pd.DataFrame.from_dict(mock_functional_summary, orient="index"),
    )
    assert mock_data_to_save["calculated_bedday_pools"] == mock_functional_summary
    assert_frame_equal(mock_data_to_save["ip_wards_capacity"], mock_capacity_df)
