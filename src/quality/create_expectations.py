import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnToExist,
    ExpectTableRowCountToBeBetween,
    ExpectColumnMaxToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnValueLengthsToBeBetween
)

context = gx.get_context(mode="file", project_root_dir=".")
suite = context.suites.get(name="covid19_quality_suite")

# ── table level ──────────────────────────────────────────
suite.add_expectation(
    ExpectTableRowCountToBeBetween(min_value=1000)
)

# ── key columns exist ────────────────────────────────────
for col in ["date", "country_code", "country_name",
            "total_new_cases", "total_new_deaths"]:
    suite.add_expectation(ExpectColumnToExist(column=col))

# ── date column ──────────────────────────────────────────
suite.add_expectation(
    ExpectColumnValuesToNotBeNull(column="date")
)

# ── country code ─────────────────────────────────────────
suite.add_expectation(
    ExpectColumnValuesToNotBeNull(column="country_code")
)
suite.add_expectation(
    ExpectColumnValueLengthsToBeBetween(column="country_code", min_value=2, max_value=2)
)
suite.add_expectation(
    ExpectColumnValuesToMatchRegex(column="country_code", regex="^[A-Z]{2}$")
)

# ── pandemic metrics ─────────────────────────────────────
suite.add_expectation(
    ExpectColumnMinToBeBetween(column="total_new_cases", min_value=0)
)
suite.add_expectation(
    ExpectColumnMinToBeBetween(column="total_new_deaths", min_value=0)
)
suite.add_expectation(
    ExpectColumnValuesToBeBetween(
        column="case_fatality_rate", 
        min_value=0, 
        max_value=1000,
        mostly=0.99
    )
)

# ── hospital metrics ─────────────────────────────────────
suite.add_expectation(
    ExpectColumnMinToBeBetween(column="current_icu_patients", min_value=0)
)
suite.add_expectation(
    ExpectColumnMinToBeBetween(column="current_ventilator_patients", min_value=0)
)

suite.save()
print("✅ All expectations saved successfully")