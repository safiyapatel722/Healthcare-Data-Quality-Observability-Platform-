import sys
from pyspark.sql import SparkSession

PROJECT_ID = sys.argv[1]
BUCKET_NAME = sys.argv[2]
BQ_TABLE = "bigquery-public-data.covid19_open_data.covid19_open_data"
OUTPUT_PATH = f"gs://{BUCKET_NAME}/raw/covid19/"

spark = SparkSession.builder.appName("CovidDataExtraction").getOrCreate()

df = (
    spark.read.format("bigquery")
    .option("table", BQ_TABLE)
    .option("project", PROJECT_ID)
    .load()
)

print("Row count:", df.count())
df.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"✅ Written to {OUTPUT_PATH}")