import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_wards import (
    calculate_critical_care_beddays,
    # calculate_0los_beddays,
    # calculate_assessment_beddays,
    # calculate_ward_beddays,
    # calculate_separate_bedday_pools,
    # group_bedday_pools,
    # convert_ip_beddays_to_beds,
    # calculate_ip_wards_capacity,
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
    beddays_dict = {"value": 1}
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


def test_convert_ip_daycase_capacity():
    # arrange
    daycase_spells = 20000
    assumed_los_hours = 9.3
    operational_hours = 14
    operational_days = 288
    occupancy_rate = 0.85

    expected = 186000 / 3427.2

    actual = convert_ip_daycase_capacity(
        daycase_spells,
        assumed_los_hours,
        operational_hours,
        operational_days,
        occupancy_rate,
    )
    # assert
    assert actual == expected


def test_calculate_ip_wards_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")

    capacity_requirements = [
        "adult_surgical_daycase",
        "adult_medical_daycase",
        "paediatric_surgical_daycase",
        "paediatric_medical_daycase",
    ]
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_daycase.convert_ip_wards_capacity",
        return_value=999,
    )
    functional_areas_summarised = {
        functional_area: {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
        for functional_area in capacity_requirements
    }
    assumptions_data = {}
    for req in capacity_requirements:
        assumptions_data[f"{req}_stay_hours"] = {"assumption_value": 1}
        assumptions_data[f"{req.split('_')[0]}_daycase_operational_hours"] = {
            "assumption_value": 2
        }
        assumptions_data[f"{req.split('_')[0]}_daycase_operational_days"] = {
            "assumption_value": 3
        }
        assumptions_data[f"{req.split('_')[0]}_daycase_recovery_occupancy_rate"] = {
            "assumption_value": 4
        }

    assumptions_df = pd.DataFrame.from_dict(assumptions_data, orient="index")

    output_index = [
        "adult_surgical_daycase_beds",
        "adult_medical_daycase_beds",
        "paediatric_surgical_daycase_beds",
        "paediatric_medical_daycase_beds",
    ]

    # act
    result = calculate_ip_daycase_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # convert_ip_daycase_capacity should be called 12 times (4 × 3)
    assert mock_convert.call_count == 12

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == output_index

    # all values should be mocked return value
    assert (result == 999).all().all()

    # check arguments in calls to convert_ip_daycase_capacity
    first_call = mock_convert.call_args_list[0]
    args = first_call.args
    assert args[1] == 1  # assumed_los_hours
    assert args[2] == 2  # operational_hours
    assert args[3] == 3  # operational_days
    assert args[4] == 4  # occupancy_rate


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.ip_daycase"

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

    mock_capacity_df = pd.DataFrame({"mean": [1]})
    mocker.patch(
        f"{module_path}.calculate_ip_daycase_capacity",
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
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_daycase"
    )
    module.summarise_functional_areas.assert_called_once_with(mock_aggregations)
    module.calculate_ip_daycase_capacity.assert_called_once_with(
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
        mock_data_to_save["ip_daycase_functional_areas"],
        pd.DataFrame.from_dict(mock_functional_summary, orient="index"),
    )
    assert_frame_equal(mock_data_to_save["ip_daycase_capacity"], mock_capacity_df)
