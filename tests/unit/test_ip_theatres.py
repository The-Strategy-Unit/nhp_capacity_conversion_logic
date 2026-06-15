import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_theatres import (
    calculate_ip_theatres_capacity,
    calculate_unknown_theatres_duration,
    convert_ip_theatres_capacity,
    main,
)


def test_convert_ip_theatres_capacity():
    # arrange
    theatres_minutes = 30000
    operational_hours = 14
    operational_days = 288
    utilisation_rate = 0.85

    expected = 500 / 3427.2

    actual = convert_ip_theatres_capacity(
        theatres_minutes,
        operational_hours,
        operational_days,
        utilisation_rate,
    )
    # assert
    assert actual == expected


def test_calculate_unknown_theatres_duration():
    # arrange
    number_of_admissions = 100
    average_theatre_time_mins = 40
    expected = 4000
    # act
    # assert
    assert (
        calculate_unknown_theatres_duration(
            number_of_admissions, average_theatre_time_mins
        )
        == expected
    )


def test_calculate_ip_theatres_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")

    functional_areas = [
        "adult_elective_surgical_procedures",
        "adult_elective_surgical_procedures_unknown_time",
        "adult_nonelective_surgical_procedures",
        "adult_nonelective_surgical_procedures_unknown_time",
        "adult_surgical_daycase",
        "adult_surgical_daycase_unknown_time",
        "paediatric_elective_surgical_procedures",
        "paediatric_elective_surgical_procedures_unknown_time",
        "paediatric_nonelective_surgical_procedures",
        "paediatric_nonelective_surgical_procedures_unknown_time",
        "paediatric_surgical_daycase",
        "paediatric_surgical_daycase_unknown_time",
    ]
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.convert_ip_theatres_capacity",
        return_value=999,
    )
    mock_unknown = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.calculate_unknown_theatres_duration",
        return_value=1,
    )
    functional_areas_summarised = {
        functional_area: {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
        for functional_area in functional_areas
    }
    assumptions_data = {}
    capacity_requirements = [
        "adult_elective_theatres",
        "paediatric_elective_theatres",
        "adult_nonelective_theatres",
        "paediatric_nonelective_theatres",
        "adult_surgical_daycase_theatres",
        "paediatric_surgical_daycase_theatres",
    ]
    for req in capacity_requirements:
        assumptions_data[f"{req}_operational_hours"] = {"assumption_value": 1}
        assumptions_data[f"{req}_operational_days"] = {"assumption_value": 2}
        assumptions_data[f"{req}_utilisation_rate"] = {"assumption_value": 3}
        assumptions_data[f"{req}_average_time_mins"] = {"assumption_value": 4}

    assumptions_df = pd.DataFrame.from_dict(assumptions_data, orient="index")

    # act
    result = calculate_ip_theatres_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # convert_ip_theatres_capacity should be called 18 times (6 × 3)
    assert mock_convert.call_count == 18
    assert mock_unknown.call_count == 18

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert sorted(list(result.index)) == sorted(capacity_requirements)

    # all values should be mocked return value
    assert (result == 999).all().all()

    # check arguments in calls to convert_ip_theatres_capacity
    first_call = mock_convert.call_args_list[0]
    args = first_call.args
    assert args[1] == 1  # operational_hours
    assert args[2] == 2  # operational_days
    assert args[3] == 3  # utilisation_rate

    convert_unknown_args = mock_unknown.call_args_list[0].args
    assert convert_unknown_args[0] == 100
    assert convert_unknown_args[1] == 4


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.ip_theatres"

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
        f"{module_path}.calculate_ip_theatres_capacity",
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
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_theatres"
    )
    module.summarise_functional_areas.assert_called_once_with(mock_aggregations)
    module.calculate_ip_theatres_capacity.assert_called_once_with(
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
        mock_data_to_save["ip_theatres_functional_areas"],
        pd.DataFrame.from_dict(mock_functional_summary, orient="index"),
    )
    assert_frame_equal(mock_data_to_save["ip_theatres_capacity"], mock_capacity_df)
