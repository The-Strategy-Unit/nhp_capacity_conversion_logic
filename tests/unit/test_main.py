from unittest.mock import call

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.__main__ import main


def test_main(mocker):
    # arrange
    main_path = "nhp.capacity_conversion.__main__"

    mock_now = mocker.Mock()
    mock_now.strftime.return_value = "20250101_120000"
    mocker.patch(f"{main_path}.datetime.datetime").now.return_value = mock_now

    mock_parser = mocker.Mock()
    mock_args = mocker.Mock()
    mock_args.guid = "GUID123"
    mock_args.path_to_assumptions_file = "assumptions.csv"
    mock_args.capacity_model_version = "dev"
    mock_args.ip_sites = "ip_sites"
    mock_args.op_sites = "op_sites"
    mock_args.aae_sites = "aae_sites"

    mock_parser.parse_args.return_value = mock_args
    mocker.patch(f"{main_path}.argparse.ArgumentParser", return_value=mock_parser)
    env_vars_dict = {
        "AZ_STORAGE_EP": "AZ_STORAGE_EP",
        "AZ_STORAGE_RESULTS": "AZ_STORAGE_RESULTS",
        "TABLE_NAME": "TABLE_NAME",
        "AZ_TABLE_ENDPOINT": "AZ_TABLE_ENDPOINT",
    }
    mocker.patch(f"{main_path}.validate_required_env_vars", return_value=env_vars_dict)
    metadata_dict = {
        "PartitionKey": "PartitionKey",
        "RowKey": "RowKey",
        "guid": "GUID123",
        "capacity_model_version": "dev",
    }
    mocker.patch(
        f"{main_path}.load_metadata_from_ats",
        return_value=metadata_dict,
    )
    mocker.patch(
        f"{main_path}.create_aggregations_path", return_value="aggregations_path"
    )

    mock_assumptions = pd.DataFrame()
    mocker.patch(f"{main_path}.load_assumptions", return_value=mock_assumptions)

    mock_filtered_aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "total": [3] * 3 + [4] * 3 + [5] * 3,
        }
    )
    mocker.patch(f"{main_path}.load_aggregations", return_value="aggregations")
    mocker.patch(
        f"{main_path}.filter_aggregations", return_value=mock_filtered_aggregations
    )
    mocker.patch(f"{main_path}.process_activity_type")

    mock_save = mocker.patch(f"{main_path}.process_and_save_results_to_excel")

    # act

    main()

    # assert

    main_mod = __import__(main_path, fromlist=["dummy"])
    main_mod.load_metadata_from_ats.assert_called_once_with(
        "GUID123", "AZ_TABLE_ENDPOINT", "TABLE_NAME", "dev"
    )
    main_mod.load_assumptions.assert_called_once_with("assumptions.csv")
    main_mod.create_aggregations_path.assert_called_once_with(metadata_dict)

    assert main_mod.load_aggregations.call_count == 6
    expected_calls = [
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "op"),
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "aae"),
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_daycase"),
        call(
            "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_maternity"
        ),
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_wards"),
        call(
            "AZ_STORAGE_EP",
            "AZ_STORAGE_RESULTS",
            "aggregations_path",
            "ip_procedures_and_theatres",
        ),
    ]
    main_mod.load_aggregations.assert_has_calls(expected_calls)

    assert main_mod.filter_aggregations.call_count == 6
    expected_calls = [
        call("aggregations", "op_sites"),
        call("aggregations", "aae_sites"),
        call("aggregations", "ip_sites"),
        call("aggregations", "ip_sites"),
        call("aggregations", "ip_sites"),
        call("aggregations", "ip_sites"),
    ]

    assert main_mod.process_activity_type.call_count == 6
    main_mod.process_activity_type.assert_has_calls(
        [
            call("op", mocker.ANY, mocker.ANY, mock_assumptions, mocker.ANY),
            call("aae", mocker.ANY, mocker.ANY, mock_assumptions, mocker.ANY),
            call("ip_daycase", mocker.ANY, mocker.ANY, mock_assumptions, mocker.ANY),
            call(
                "ip_maternity",
                mocker.ANY,
                mocker.ANY,
                mock_assumptions,
                mocker.ANY,
                preprocess=mocker.ANY,
            ),
            call(
                "ip_wards",
                mocker.ANY,
                mocker.ANY,
                mock_assumptions,
                mocker.ANY,
                preprocess=mocker.ANY,
            ),
        ],
        any_order=False,
    )
    mock_save.assert_called_once()
    mock_data_to_save = mock_save.call_args_list[0].args[0]
    assert_series_equal(
        mock_data_to_save["metadata"],
        pd.Series(
            {
                "guid": "GUID123",
                "capacity_model_version": "dev",
                "ip_sites": "ip_sites",
                "op_sites": "op_sites",
                "aae_sites": "aae_sites",
                "capacity_conversion_runtime": "20250101_120000",
            }
        ),
    )
    assert_frame_equal(mock_data_to_save["assumptions"], pd.DataFrame())
