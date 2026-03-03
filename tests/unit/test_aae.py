import pandas as pd
from pandas.testing import assert_series_equal

from nhp.capacity_conversion.aae import (
    map_unknown,
    load_aae_aggregations,
    process_aae,
    convert_aae_capacity,
    map_aae_capacity_to_functional_area,
    calculate_aae_capacity,
    main,
)


def test_load_aae_aggregations(mocker, caplog):
    # arrange
    caplog.set_level("INFO")
    mock_connection = mocker.Mock()
    mocker.patch(
        "nhp.capacity_conversion.aae.connect_to_container",
        return_value=mock_connection,
    )
    mocker.patch(
        "nhp.capacity_conversion.aae.load_parquet_file",
        return_value=pd.DataFrame({"col": [1]}),
    )

    # act
    load_aae_aggregations("url", "container", "path")

    # assert
    assert "Loading A&E data from path..." in caplog.text


def test_process_aae(mocker):
    # arrange
    aae_aggregations = pd.DataFrame(
        {
            "grouping": ["a", "b", "c"] * 3,
            "model_run": [0] * 3 + [1] * 3 + [2] * 3,
            "arrivals": [3] * 3 + [4] * 3 + [5] * 3,
        }
    ).set_index("model_run")
    mocker.patch(
        "nhp.capacity_conversion.aae.map_unknown",
        return_value=aae_aggregations["grouping"],
    )
    mocker.patch(
        "nhp.capacity_conversion.aae.calculate_prediction_intervals_and_mean",
        return_value={"mean": 4.5, "p10": 4.1, "p90": 4.9},
    )

    expected = {
        "a": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
        "b": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
        "c": {"mean": 4.5, "p10": 4.1, "p90": 4.9},
    }

    # act
    actual = process_aae(aae_aggregations)

    # assert
    assert actual == expected


def test_map_unknown():
    # arrange
    col = pd.Series(
        ["adult_unknown", "adult_minor_attendances", "sdec", "child_unknown"]
    )
    # act
    col_mapped = map_unknown(col)
    # assert
    assert_series_equal(
        col_mapped,
        pd.Series(
            [
                "adult_minor_attendances",
                "adult_minor_attendances",
                "sdec",
                "child_minor_attendances",
            ]
        ),
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
        assumptions_data[f"{req}_assumed_los"] = {"assumption_value": 240}
        assumptions_data[f"{req}_operating_hours"] = {"assumption_value": 168}
        assumptions_data[f"{req}_operating_weeks"] = {"assumption_value": 52}
        assumptions_data[f"{req}_utilisation"] = {"assumption_value": 0.5}

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
    mock_args.aggregations_path = "functional-aggregations/V1/GUID123/"
    mock_args.path_to_assumptions_file = "assumptions.csv"

    mock_parser.parse_args.return_value = mock_args
    mocker.patch(f"{module_path}.argparse.ArgumentParser", return_value=mock_parser)

    mocker.patch(f"{module_path}.load_dotenv")
    mocker.patch(f"{module_path}.os.getenv", side_effect=["account_url", "container"])

    mock_assumptions = pd.DataFrame()
    mocker.patch(f"{module_path}.load_assumptions", return_value=mock_assumptions)

    mock_aggregations = pd.DataFrame()
    mocker.patch(f"{module_path}.load_aae_aggregations", return_value=mock_aggregations)

    mock_functional_summary = {"area": {"mean": 1}}
    mocker.patch(
        f"{module_path}.process_aae",
        return_value=mock_functional_summary,
    )

    mock_capacity_df = pd.DataFrame({"mean": [1]})
    mocker.patch(
        f"{module_path}.calculate_aae_capacity",
        return_value=mock_capacity_df,
    )

    mock_save = mocker.patch(f"{module_path}.save_results_to_csv")

    # act

    main()

    # assert

    module = __import__(module_path, fromlist=["dummy"])
    module.load_assumptions.assert_called_once_with("assumptions.csv")
    module.load_aae_aggregations.assert_called_once_with(
        "account_url",
        "container",
        "functional-aggregations/V1/GUID123/",
    )
    module.process_aae.assert_called_once_with(mock_aggregations)
    module.calculate_aae_capacity.assert_called_once_with(
        mock_functional_summary,
        mock_assumptions,
    )
    mock_save.assert_called_once_with(
        mock_capacity_df,
        "GUID123",
        "20250101_120000",
        "aae",
    )
