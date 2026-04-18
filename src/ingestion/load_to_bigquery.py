import sys
from pyspark.sql import SparkSession

PROJECT_ID = sys.argv[1]
BUCKET_NAME = sys.argv[2]
INPUT_PATH = f"gs://{BUCKET_NAME}/raw/covid19/"
BQ_DATASET = "healthcare_staging"
BQ_TABLE = "covid19_raw"

spark = SparkSession.builder.appName("LoadToBigQuery").getOrCreate()

df = spark.read.parquet(INPUT_PATH)

(
    df.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}")
    .option("temporaryGcsBucket", BUCKET_NAME)
    .mode("overwrite")
    .save()
)

print(f"✅ Loaded to BigQuery: {PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}")