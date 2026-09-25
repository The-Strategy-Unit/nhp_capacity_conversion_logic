ASSUMPTIONS_URL = "https://raw.githubusercontent.com/The-Strategy-Unit/open-plan-docs/refs/heads/main/docs/data/assumptions_register.csv"

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
}
