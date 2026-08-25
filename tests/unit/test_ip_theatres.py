import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_theatres import (
    THEATRES_WORKLOAD_ASSUMPTIONS_DICT,
    calculate_ip_theatres_capacity,
    calculate_procedure_time,
    combine_procedure_groupings,
    convert_procedure_time_to_hours,
    main,
    preprocess_ip_theatres_data,
)


def test_calculate_procedure_time(mocker):
    functional_areas = pd.DataFrame(
        {
            "spells": [2.0] * len(THEATRES_WORKLOAD_ASSUMPTIONS_DICT),
            "total_theatre_time": [0.0] * len(THEATRES_WORKLOAD_ASSUMPTIONS_DICT),
        },
        index=pd.MultiIndex.from_tuples(
            [(0, group) for group in THEATRES_WORKLOAD_ASSUMPTIONS_DICT],
            names=["model_run", "procedure_grouping"],
        ),
    )
    functional_areas.loc[("unchanged_grouping", 0), :] = 1.0
    assumptions_df = pd.DataFrame(
        {"Value": "Value" * len(THEATRES_WORKLOAD_ASSUMPTIONS_DICT)},
        index=[
            assumption_dict["procedure_time"]
            for assumption_dict in THEATRES_WORKLOAD_ASSUMPTIONS_DICT.values()
        ],
    )
    mock_derive_treatment_hours = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.derive_treatment_hours", return_value=1.0
    )
    expected = pd.DataFrame(
        {
            "spells": [2.0] * len(THEATRES_WORKLOAD_ASSUMPTIONS_DICT) + [1.0],
            "total_theatre_time": [1.0] * len(THEATRES_WORKLOAD_ASSUMPTIONS_DICT)
            + [1.0],
        },
        index=functional_areas.index,
    )
    actual = calculate_procedure_time(functional_areas, assumptions_df)
    assert_frame_equal(actual, expected)
    assert mock_derive_treatment_hours.call_count == len(
        THEATRES_WORKLOAD_ASSUMPTIONS_DICT
    )


def test_convert_procedure_time_to_hours(mocker):
    index = pd.MultiIndex.from_tuples(
        [
            (0, "procedure_a"),
            (0, "adult_elective_surgical_procedures_unknown_time"),
            (1, "procedure_a"),
            (1, "adult_elective_surgical_procedures_unknown_time"),
        ],
        names=["model_run", "procedure_grouping"],
    )
    functional_areas = pd.DataFrame(
        {
            "total_theatre_time": [120.0, 60.0] * 2,
        },
        index=index,
    )
    expected = pd.DataFrame(
        {
            "total_theatre_time": [2.0, 60.0] * 2,
        },
        index=index,
    )
    actual = convert_procedure_time_to_hours(functional_areas)
    assert_frame_equal(actual, expected)


def test_combine_procedure_groupings():
    index = pd.MultiIndex.from_tuples(
        [
            (0, "procedure_a"),
            (0, "procedure_a_unknown_time"),
            (0, "procedure_b"),
        ],
        names=["model_run", "procedure_grouping"],
    )
    functional_areas = pd.DataFrame(
        {
            "spells": [2.0, 1.0, 3.0],
            "total_theatre_time": [2.0, 1.0, 3.0],
        },
        index=index,
    )
    expected_index = pd.MultiIndex.from_tuples(
        [
            (0, "procedure_a"),
            (0, "procedure_b"),
        ],
        names=["model_run", "procedure_grouping"],
    )
    expected = pd.DataFrame(
        {
            "spells": [3.0, 3.0],
            "total_theatre_time": [3.0, 3.0],
        },
        index=expected_index,
    )
    actual = combine_procedure_groupings(functional_areas)
    assert_frame_equal(actual, expected)


def test_preprocess_ip_theatres_data(mocker):
    index = pd.MultiIndex.from_tuples(
        [
            (0, "procedure_a"),
            (0, "unknown_procedure"),
        ],
        names=["model_run", "procedure_grouping"],
    )
    functional_areas = pd.DataFrame(
        {"total_theatre_time": [120.0, 30.0]},
        index=index,
    )
    assumptions_df = pd.DataFrame()
    converted_data = mocker.sentinel.converted_data
    calculated_data = mocker.sentinel.calculated_data
    grouped_data = mocker.sentinel.grouped_data

    mock_convert = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.convert_procedure_time_to_hours",
        return_value=converted_data,
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.calculate_procedure_time",
        return_value=calculated_data,
    )
    mock_combine = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.combine_procedure_groupings",
        return_value=grouped_data,
    )
    result = preprocess_ip_theatres_data(
        functional_areas,
        assumptions_df,
    )
    # unknown_procedure should be removed before the helper is called
    convert_input = mock_convert.call_args.args[0]
    assert list(convert_input.index.get_level_values("procedure_grouping")) == [
        "procedure_a"
    ]
    mock_calculate.assert_called_once_with(
        converted_data,
        assumptions_df,
    )
    mock_combine.assert_called_once_with(calculated_data)
    assert result is grouped_data


def test_calculate_ip_theatres_capacity(mocker):
    functional_areas_processed = pd.DataFrame(
        {
            "total_theatre_time": [100.0, 200.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("procedure_a", 0),
                ("procedure_a", 1),
            ],
            names=["procedure_grouping", "model_run"],
        ),
    )

    assumptions_df = pd.DataFrame(
        {
            "Value": {
                "annual_operational_hours": 1000.0,
                "utilisation": 0.8,
            }
        }
    )

    mocker.patch.dict(
        "nhp.capacity_conversion.ip_theatres.THEATRES_CAPACITY_ASSUMPTIONS_DICT",
        {
            "procedure_a": {
                "annual_operational_hours": "annual_operational_hours",
                "utilisation": "utilisation",
                "output": "capacity_output",
            }
        },
        clear=True,
    )
    mock_calculate = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.calculate_time_util_capacity",
        return_value=pd.Series(
            [1.5, 2.5],
            index=pd.Index([0, 1], name="model_run"),
            name="total_theatre_time",
        ),
    )
    treatment_hours = pd.Series(
        [100.0, 200.0],
        index=pd.Index([0, 1], name="model_run"),
        name="total_theatre_time",
    )

    expected = pd.DataFrame(
        {"total": [1.5, 2.5]},
        index=pd.MultiIndex.from_tuples(
            [
                ("capacity_output", 0),
                ("capacity_output", 1),
            ],
            names=["output", "model_run"],
        ),
    )
    actual = calculate_ip_theatres_capacity(
        functional_areas_processed,
        assumptions_df,
    )
    assert_frame_equal(actual, expected)
    calculate_input = mock_calculate.call_args.args[0]
    assert_series_equal(treatment_hours, calculate_input)


def test_main(mocker):
    mock_run_single = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.run_single_activity_type"
    )
    main()
    mock_run_single.assert_called_with(
        "ip_procedures_and_theatres",
        calculate_ip_theatres_capacity,
        preprocess=preprocess_ip_theatres_data,
    )
