from unittest.mock import call

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.aae import (
    calculate_aae_capacity,
    convert_aae_capacity,
    derive_aae_workload,
    main,
)


def test_derive_aae_workload():
    # arrange
    attendances = 10
    assumed_los_mins = 120
    expected = 20
    # act
    actual = derive_aae_workload(attendances, assumed_los_mins)
    # assert
    assert actual == expected


def test_convert_aae_capacity():
    # arrange
    occupancy_hours = 100
    annual_operational_hours = 20
    utilisation = 0.5

    expected = 10

    # act
    actual = convert_aae_capacity(
        occupancy_hours,
        annual_operational_hours,
        utilisation,
    )
    # assert
    assert actual == expected


def test_calculate_aae_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mocker.patch(
        "nhp.capacity_conversion.aae.ASSUMPTIONS_MAPPING",
        {
            "test_subgroup": {
                "los": "LOS",
                "hours": "HOURS",
                "util": "UTIL",
                "output": "output_spaces",
            }
        },
    )

    functional_areas_summarised = {
        "test_subgroup": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
    mock_workload = mocker.patch(
        "nhp.capacity_conversion.aae.derive_aae_workload",
        return_value="workload",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.aae.convert_aae_capacity",
        return_value="capacity",
    )

    assumptions_df = pd.DataFrame(
        {"assumption_value": [1, 2, 3]},
        index=["LOS", "HOURS", "UTIL"],
    )

    # act
    result = calculate_aae_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == ["output_spaces"]
    assert (result == "capacity").all().all()

    # test calls to mocked functions
    mock_workload.assert_has_calls(
        [
            call(100, 1),
            call(200, 1),
            call(300, 1),
        ],
        any_order=False,
    )
    mock_convert.assert_has_calls(
        [
            call(
                "workload",
                annual_operational_hours=2,
                utilisation=3,
            )
        ]
        * 3
    )


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
