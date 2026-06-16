import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.aae import (
    calculate_aae_capacity,
    convert_aae_capacity,
    main,
    map_aae_capacity_to_functional_area,
)


def test_convert_aae_capacity():
    # arrange
    attendances = 10000
    assumed_los_mins = 240
    operating_weeks_per_year = 52
    operating_hours_per_week = 168
    utilisation_rate = 0.5

    expected = 40000 / 4368

    # act
    actual = convert_aae_capacity(
        attendances,
        assumed_los_mins,
        operating_weeks_per_year,
        operating_hours_per_week,
        utilisation_rate,
    )
    # assert
    assert actual == expected


def test_map_aae_capacity_to_functional_area():
    # arrange
    capacity_requirement_string = "sdec_spaces"
    expected = "sdec_attendances"
    # act
    actual = map_aae_capacity_to_functional_area(capacity_requirement_string)
    # assert
    assert actual == expected


def test_calculate_aae_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")

    capacity_requirements = [
        "adult_major_spaces",
        "adult_minor_spaces",
        "child_major_spaces",
        "child_minor_spaces",
        "sdec_spaces",
        "resus_spaces",
    ]
    mocker.patch(
        "nhp.capacity_conversion.aae.map_aae_capacity_to_functional_area",
        return_value="mock_functional_area",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.aae.convert_aae_capacity",
        return_value=999,
    )
    functional_areas_summarised = {
        "mock_functional_area": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
    assumptions_data = {}
    for req in capacity_requirements:
        assumptions_data[f"{req}_assumed_los_mins"] = {"assumption_value": 240}
        assumptions_data[f"{req}_operating_hours"] = {"assumption_value": 168}
        assumptions_data[f"{req}_operating_weeks"] = {"assumption_value": 52}
        assumptions_data[f"{req}_utilisation_rate"] = {"assumption_value": 0.5}

    assumptions_df = pd.DataFrame.from_dict(assumptions_data, orient="index")

    # act
    result = calculate_aae_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # convert_aae_capacity should be called 18 times (6 × 3)
    assert mock_convert.call_count == 18

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == capacity_requirements

    # all values should be mocked return value
    assert (result == 999).all().all()

    # check arguments in calls to convert_aae_capacity
    first_call = mock_convert.call_args_list[0]
    args = first_call.args
    assert args[1] == 240  # assumed_los
    assert args[2] == 52  # operating_weeks
    assert args[3] == 168  # operating_hours
    assert args[4] == 0.5  # utilisation


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.aae"

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
        f"{module_path}.calculate_aae_capacity",
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
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "aae"
    )
    module.summarise_functional_areas.assert_called_once_with(mock_aggregations)
    module.calculate_aae_capacity.assert_called_once_with(
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
        mock_data_to_save["aae_functional_areas"],
        pd.DataFrame.from_dict(mock_functional_summary, orient="index"),
    )
    assert_frame_equal(mock_data_to_save["aae_capacity"], mock_capacity_df)
