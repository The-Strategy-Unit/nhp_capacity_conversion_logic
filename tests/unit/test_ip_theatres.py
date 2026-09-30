import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from nhp.capacity_conversion.ip_theatres import (
    THEATRES_ASSUMPTIONS_DICT,
    calculate_ip_theatres_capacity,
    calculate_procedure_time,
    main,
    preprocess_ip_theatres_data,
)


def test_calculate_procedure_time(mocker):
    functional_areas = pd.DataFrame(
        {"value": [2.0] * len(THEATRES_ASSUMPTIONS_DICT)},
        index=pd.MultiIndex.from_tuples(
            [(0, "procedures", group) for group in THEATRES_ASSUMPTIONS_DICT],
            names=["model_run", "measure", "functional_area"],
        ),
    )
    assumptions_df = pd.DataFrame(
        {"Value": ["Value"] * len(THEATRES_ASSUMPTIONS_DICT)},
        index=[
            assumption_dict["procedure_time"]
            for assumption_dict in THEATRES_ASSUMPTIONS_DICT.values()
        ],
    )
    mock_derive_treatment_hours = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.derive_treatment_hours",
        return_value=pd.Series(
            [3.0], name="value", index=pd.Index([0], name="model_run")
        ),
    )
    expected = pd.DataFrame(
        {
            "value": [2.0] * len(THEATRES_ASSUMPTIONS_DICT)
            + [3.0] * len(THEATRES_ASSUMPTIONS_DICT)
        },
        index=pd.MultiIndex.from_tuples(
            [(0, "procedures", group) for group in THEATRES_ASSUMPTIONS_DICT]
            + [(0, "total_time_hours", group) for group in THEATRES_ASSUMPTIONS_DICT],
            names=["model_run", "measure", "functional_area"],
        ),
    )
    actual = calculate_procedure_time(functional_areas, assumptions_df)
    print(actual)
    assert_frame_equal(actual, expected)
    assert mock_derive_treatment_hours.call_count == len(THEATRES_ASSUMPTIONS_DICT)


def test_preprocess_ip_theatres_data(mocker):
    mock_calculate_procedure_time = mocker.patch(
        "nhp.capacity_conversion.ip_theatres.calculate_procedure_time"
    )
    assumptions_df = pd.DataFrame()
    functional_areas = pd.DataFrame()
    preprocess_ip_theatres_data(functional_areas, assumptions_df)
    mock_calculate_procedure_time.assert_called_once()


def test_calculate_ip_theatres_capacity(mocker):
    functional_areas_processed = pd.DataFrame(
        {
            "value": [100.0, 200.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                (0, "total_time_hours", "procedure_a"),
                (1, "total_time_hours", "procedure_a"),
            ],
            names=["model_run", "measure", "functional_area"],
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
        "nhp.capacity_conversion.ip_theatres.THEATRES_ASSUMPTIONS_DICT",
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
            name="total",
        ),
    )
    treatment_hours = pd.Series(
        [100.0, 200.0],
        index=pd.Index([0, 1], name="model_run"),
        name="value",
    )

    expected = pd.DataFrame(
        {"total": [1.5, 2.5]},
        index=pd.MultiIndex.from_tuples(
            [
                (0, "capacity_output"),
                (1, "capacity_output"),
            ],
            names=["model_run", "output"],
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
