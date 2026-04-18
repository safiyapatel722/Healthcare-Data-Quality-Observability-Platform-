import great_expectations as gx
import pandas as pd
from google.cloud import bigquery

# fetch mart data into pandas
client = bigquery.Client(project="covid-data-observability")
query = "SELECT * FROM `covid-data-observability.healthcare_staging.mart_pandemic_impact` LIMIT 100000"
df = client.query(query).to_dataframe()
print(f"Fetched {len(df)} rows from BigQuery")

context = gx.get_context(mode="file", project_root_dir=".")

datasource = context.data_sources.add_or_update_pandas(name="bigquery_pandas")
asset = datasource.add_dataframe_asset(name="pandemic_impact")
batch_definition = asset.add_batch_definition_whole_dataframe("full_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

validation_definition = gx.ValidationDefinition(
    name="pandemic_impact_validation",
    suite=context.suites.get("covid19_quality_suite"),
    data=batch_definition
)

try:
    context.validation_definitions.add(validation_definition)
except Exception:
    context.validation_definitions.delete("pandemic_impact_validation")
    context.validation_definitions.add(validation_definition)
results = validation_definition.run(batch_parameters={"dataframe": df})

print(f"\nSuccess: {results.success}")
print(f"Total:  {results.statistics['evaluated_expectations']}")
print(f"Passed: {results.statistics['successful_expectations']}")
print(f"Failed: {results.statistics['unsuccessful_expectations']}")

for result in results.results:
    if not result.success:
        print(f"Failed: {result.expectation_config.type}")
        print(f"Details: {result.result}")
        print("---")