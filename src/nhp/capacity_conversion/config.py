ASSUMPTIONS_URL = "https://raw.githubusercontent.com/The-Strategy-Unit/open-plan-docs/refs/heads/main/docs/data/assumptions_register.csv"

# Name of the parquet file holding functional area aggregations, stored inside
# the "aggregated_results_path" folder recorded against each model run
RESULTS_FILE_NAME = "functional_areas.parquet"

# Format used for the capacity conversion runtime stamped into result metadata
RUNTIME_FORMAT = "%Y%m%d_%H%M%S"

# Only model runs produced by demand model app versions at or above this can be
# converted (app_version values look like "v6.0"; "dev" runs are excluded)
MINIMUM_APP_VERSION = (6, 0)

# Fields needed to list and select model runs. The RowKey is the run's GUID.
CATALOGUE_FIELDS = (
    "dataset",
    "scenario",
    "create_datetime",
    "app_version",
)

# Fields kept from the Azure Table Storage entity when loading a single run
METADATA_FIELDS = (
    "app_version",
    "dataset",
    "start_year",
    "end_year",
    "scenario",
    "create_datetime",
    "model_run_id",
    "aggregated_results_path",
)

# Fields that must be present and non-empty when loading a single run
REQUIRED_METADATA_FIELDS = (*CATALOGUE_FIELDS, "guid", "aggregated_results_path")

ACTIVITY_TYPES = (
    "op",
    "aae",
    "ip_daycase",
    "ip_maternity",
    "ip_wards",
    "ip_procedures_and_theatres",
)

AGGREGATION_SUBSETS = {
    "op": [
        "op_procedures",
        "op_first_attendances",
        "op_follow_up_attendances",
        "op_virtual_attendances",
    ],
    "aae": [
        "adult_major_attendances",
        "adult_minor_attendances",
        "paediatric_major_attendances",
        "paediatric_minor_attendances",
        "resus_attendances",
        "sdec_procedures",
    ],
    "ip_daycase": [
        "adult_daycase_medical",
        "adult_daycase_surgical",
        "daycase_endoscopy",
        "daycase_haem_onc",
        "daycase_renal",
        "paediatric_daycase_medical",
        "paediatric_daycase_surgical",
    ],
    "ip_maternity": [
        "maternity_assessment",
        "maternity_assisted_delivery_nonzerolos",
        "maternity_assisted_delivery_zerolos",
        "maternity_elective_csection_nonzerolos",
        "maternity_elective_csection_zerolos",
        "maternity_nonelective_csection_nonzerolos",
        "maternity_nonelective_csection_zerolos",
        "maternity_normal_delivery_nonzerolos",
        "maternity_normal_delivery_zerolos",
        "maternity_overnight_no_birth",
    ],
    "ip_wards": [
        "adult_elective_medical_nonzerolos",
        "adult_elective_medical_zerolos",
        "adult_elective_surgical_nonzerolos",
        "adult_elective_surgical_zerolos",
        "adult_nonelective_medical_nonzerolos",
        "adult_nonelective_medical_zerolos",
        "adult_nonelective_surgical_nonzerolos",
        "adult_nonelective_surgical_zerolos",
        "paediatric_elective_medical_nonzerolos",
        "paediatric_elective_medical_zerolos",
        "paediatric_elective_surgical_nonzerolos",
        "paediatric_elective_surgical_zerolos",
        "paediatric_nonelective_medical_nonzerolos",
        "paediatric_nonelective_medical_zerolos",
        "paediatric_nonelective_surgical_nonzerolos",
        "paediatric_nonelective_surgical_zerolos",
    ],
    "ip_procedures_and_theatres": [
        "adult_elective_surgical_procedures",
        "adult_nonelective_surgical_procedures",
        "adult_surgical_daycase_procedures",
        "cardiac_catheter_procedure",
        "interventional_radiology_procedure",
        "paediatric_daycase_procedures",
        "paediatric_elective_procedures",
        "paediatric_nonelective_procedures",
    ],
}
