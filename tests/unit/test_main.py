from nhp.capacity_conversion.main import main
from pandas.testing import assert_series_equal, assert_frame_equal

from unittest.mock import call
import pandas as pd


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.main"

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
        f"{module_path}.calculate_op_capacity",
        return_value=mock_capacity_df,
    )
    mocker.patch(
        f"{module_path}.calculate_aae_capacity",
        return_value=mock_capacity_df,
    )
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

    assert module.load_aggregations.call_count == 3
    expected_calls = [
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "op"),
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "aae"),
        call("AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "ip_daycase"),
    ]
    module.load_aggregations.assert_has_calls(expected_calls)

    assert module.summarise_functional_areas.call_count == 3
    module.calculate_op_capacity.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    module.calculate_aae_capacity.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    module.calculate_ip_daycase_capacity.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    mock_save.assert_called_once()
    mock_data_to_save = mock_save.call_args_list[0].args[0]
    assert list(mock_data_to_save.keys()) == [
        "metadata",
        "assumptions",
        "op_functional_areas",
        "op_capacity",
        "aae_functional_areas",
        "aae_capacity",
        "ip_daycase_functional_areas",
        "ip_daycase_capacity",
    ]
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
