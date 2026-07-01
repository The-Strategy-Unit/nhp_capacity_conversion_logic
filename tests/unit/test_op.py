from unittest.mock import call

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.op import (
    calculate_op_capacity,
    convert_op_capacity,
    derive_op_workload,
    main,
)


def test_derive_op_workload():
    # arrange
    time = 20
    dna_rate = 0.1
    dna_time = 20
    attendances = 60
    expected = 22
    # act
    actual = derive_op_workload(time, dna_rate, dna_time, attendances)
    # assert
    assert actual == expected


def test_convert_op_capacity():
    # arrange
    workload_hours = 100
    operational_hours = 50
    utilisation_rate = 0.1
    expected = 20

    # act
    actual = convert_op_capacity(workload_hours, operational_hours, utilisation_rate)
    # assert
    assert actual == expected


def test_calculate_op_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mocker.patch(
        "nhp.capacity_conversion.op.ASSUMPTIONS_MAPPING",
        {
            "test_subgroup": {
                "time": "TIME",
                "dna_rate": "DNA_RATE",
                "dna_time": "DNA_TIME",
                "util": "UTIL",
                "operational_hours": "OPERATIONAL_HOURS",
                "output": "OUTPUT",
            }
        },
    )

    mock_workload = mocker.patch(
        "nhp.capacity_conversion.op.derive_op_workload",
        return_value="workload",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.op.convert_op_capacity",
        return_value="capacity",
    )
    functional_areas_summarised = {
        "test_subgroup": {
            "p10": 100,
            "mean": 200,
            "p90": 300,
        }
    }
    assumptions_df = pd.DataFrame(
        {
            "Value": [
                "TIME",
                "DNA_RATE",
                "DNA_TIME",
                "UTIL",
                "OPERATIONAL_HOURS",
                "OUTPUT",
            ]
        },
        index=["TIME", "DNA_RATE", "DNA_TIME", "UTIL", "OPERATIONAL_HOURS", "OUTPUT"],
    )

    # act
    result = calculate_op_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    assert mock_convert.call_count == 3

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == ["OUTPUT"]
    assert (result == "capacity").all().all()

    # test calls to mocked functions
    mock_workload.assert_has_calls(
        [
            call("TIME", "DNA_RATE", "DNA_TIME", 100),
            call("TIME", "DNA_RATE", "DNA_TIME", 200),
            call("TIME", "DNA_RATE", "DNA_TIME", 300),
        ],
        any_order=False,
    )
    mock_convert.assert_has_calls(
        [
            call(
                "workload",
                "OPERATIONAL_HOURS",
                "UTIL",
            )
        ]
        * 3
    )


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.op"
    utils_path = "nhp.capacity_conversion.utils"

    mock_now = mocker.Mock()
    mock_now.strftime.return_value = "20250101_120000"
    mocker.patch(f"{utils_path}.datetime").now.return_value = mock_now

    mock_parser = mocker.Mock()
    mock_args = mocker.Mock()
    mock_args.guid = "GUID123"
    mock_args.path_to_assumptions_file = "assumptions.csv"
    mock_args.capacity_model_version = "dev"

    mock_parser.parse_args.return_value = mock_args
    mocker.patch(f"{utils_path}.argparse.ArgumentParser", return_value=mock_parser)
    env_vars_dict = {
        "AZ_STORAGE_EP": "AZ_STORAGE_EP",
        "AZ_STORAGE_RESULTS": "AZ_STORAGE_RESULTS",
        "TABLE_NAME": "TABLE_NAME",
        "AZ_TABLE_ENDPOINT": "AZ_TABLE_ENDPOINT",
    }
    mocker.patch(f"{utils_path}.validate_required_env_vars", return_value=env_vars_dict)
    metadata_dict = {
        "PartitionKey": "PartitionKey",
        "RowKey": "RowKey",
        "guid": "GUID123",
        "capacity_model_version": "dev",
    }
    mocker.patch(
        f"{utils_path}.load_metadata_from_ats",
        return_value=metadata_dict,
    )
    mocker.patch(
        f"{utils_path}.create_aggregations_path", return_value="aggregations_path"
    )

    mock_assumptions = pd.DataFrame()
    mocker.patch(f"{utils_path}.load_assumptions", return_value=mock_assumptions)

    mock_aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "total": [3] * 3 + [4] * 3 + [5] * 3,
        }
    )
    mocker.patch(f"{utils_path}.load_aggregations", return_value=mock_aggregations)

    mock_functional_summary = {"area": {"mean": 1}}
    mocker.patch(
        f"{utils_path}.summarise_functional_areas",
        return_value=mock_functional_summary,
    )

    mock_capacity_df = pd.DataFrame({"mean": [1]})
    mocker.patch(
        f"{module_path}.calculate_op_capacity",
        return_value=mock_capacity_df,
    )

    mock_save = mocker.patch(f"{utils_path}.save_results_to_excel")

    # act

    main()

    # assert

    utils = __import__(utils_path, fromlist=["dummy"])
    utils.load_metadata_from_ats.assert_called_once_with(
        "GUID123", "AZ_TABLE_ENDPOINT", "TABLE_NAME", "dev"
    )
    utils.load_assumptions.assert_called_once_with("assumptions.csv")
    utils.create_aggregations_path.assert_called_once_with(metadata_dict)
    utils.load_aggregations.assert_called_once_with(
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "op"
    )
    module = __import__(module_path, fromlist=["dummy"])
    module.calculate_op_capacity.assert_called_once_with(
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
        mock_data_to_save["op_functional_areas"],
        pd.DataFrame.from_dict(mock_functional_summary, orient="index"),
    )
    assert_frame_equal(mock_data_to_save["op_capacity"], mock_capacity_df)
