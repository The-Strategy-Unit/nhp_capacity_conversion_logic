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
    ]
}
