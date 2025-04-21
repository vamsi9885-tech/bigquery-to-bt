
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("BQ to GCS Parquet").getOrCreate()
df = spark.read.format("bigquery").option("table", "your_dataset.your_table").load()
df.write.mode("overwrite").parquet(sys.argv[1])
