import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal

from nhp.capacity_conversion.op import (
    convert_op_capacity,
    map_op_capacity_to_functional_area,
    calculate_op_capacity,
    main,
)


def test_convert_op_capacity():
    # arrange
    attendances = 100000
    duration_mins = 38
    dna_rate = 0.07
    dna_time_mins = 20
    operational_hours = 40
    operational_weeks = 48
    utilisation_rate = 0.85

    expected = (3940000 / 60) / 1632

    actual = convert_op_capacity(
        attendances,
        duration_mins,
        dna_rate,
        dna_time_mins,
        operational_hours,
        operational_weeks,
        utilisation_rate,
    )
    # assert
    assert actual == expected


def test_map_op_capacity_to_functional_area():
    # arrange
    capacity_requirement_strings = ["op_procedures", "op_first"]
    expected = ["outpatient_procedures", "outpatient_first_attendances"]
    # act
    actual = [
        map_op_capacity_to_functional_area(var_name)
        for var_name in capacity_requirement_strings
    ]
    # assert
    assert actual == expected


def test_calculate_op_capacity(mocker, caplog):
    # arrange
    caplog.set_level("INFO")

    capacity_requirements = [
        "op_first",
        "op_followup",
        "op_virtual",
        "op_procedures",
    ]
    mocker.patch(
        "nhp.capacity_conversion.op.map_op_capacity_to_functional_area",
        return_value="mock_functional_area",
    )
    mock_convert = mocker.patch(
        "nhp.capacity_conversion.op.convert_op_capacity",
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
        assumptions_data[f"{req}_duration_mins"] = {"assumption_value": 1}
        assumptions_data[f"{req}_dna_rate"] = {"assumption_value": 2}
        assumptions_data[f"{req}_dna_time_mins"] = {"assumption_value": 3}
        assumptions_data[f"{req}_operational_hours"] = {"assumption_value": 4}
        assumptions_data[f"{req}_operational_weeks"] = {"assumption_value": 5}
        assumptions_data[f"{req}_utilisation_rate"] = {"assumption_value": 6}

    assumptions_df = pd.DataFrame.from_dict(assumptions_data, orient="index")

    output_index = [
        "op_first",
        "op_followup",
        "op_virtual_consultation_rooms",
        "op_procedure_rooms",
        "op_consultation_rooms",
    ]

    # act
    result = calculate_op_capacity(
        functional_areas_summarised,
        assumptions_df,
    )

    # assert

    # convert_op_capacity should be called 12 times (4 × 3)
    assert mock_convert.call_count == 12

    # output structure
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["p10", "mean", "p90"]
    assert list(result.index) == output_index

    # all values should be mocked return value except op_consultation_rooms
    assert (result.loc[output_index[:-1]] == 999).all().all()
    assert (result.loc[output_index[-1]] == 999 * 2).all().all()

    # check arguments in calls to convert_op_capacity
    first_call = mock_convert.call_args_list[0]
    args = first_call.args
    assert args[1] == 1  # duration
    assert args[2] == 2  # dna_rate
    assert args[3] == 3  # dna_time
    assert args[4] == 4  # operational_hours
    assert args[5] == 5  # operational_weeks
    assert args[6] == 6  # utilisation_rate


def test_main(mocker):
    # arrange
    module_path = "nhp.capacity_conversion.op"

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
        "AZ_STORAGE_EP", "AZ_STORAGE_RESULTS", "aggregations_path", "op"
    )
    module.summarise_functional_areas.assert_called_once_with(mock_aggregations)
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
