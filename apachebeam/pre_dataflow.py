from google.cloud import bigtable, bigquery, storage
import json
import logging
import argparse
from pyspark.sql import SparkSession

def parse_arguments():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--project_id", help="Project ID")
    parser.add_argument("--gear_project_id", help="Gear Project ID")
    parser.add_argument("--bq_table_name", help="Bigquery table name")
    parser.add_argument("--dataset_id", help="Bigquery Dataset name")
    parser.add_argument("--temp_path" , help = "path for CSV file")
    return parser.parse_args()

def fetch_bigquery_to_gcp(spark, gear_project_id, dataset_id, bq_table_name,temp_path):
    logging.info(f"Fetching data from BigQuery table {bq_table_name}")
    # bq_client = bigquery.Client(project=project_id)  # Intialize BigQuery Client
    # query = f'''SELECT * from `{project_id}.{dataset_id}.{bq_table_name}` WHERE hb9_date_stmt_yr=2024 limit 1000'''
    # query_job = bq_client.query(query)  # Execute the query
    # results = query_job.result()  # Get the results
    query = f"SELECT * FROM `{gear_project_id}.{dataset_id}.{bq_table_name}` "
    logging.info(f"Running this query : {query} ")
    df = spark.read.format("bigquery") \
        .option("query", query).option("project", gear_project_id).option("dataset", dataset_id)\
        .option("viewsEnabled", "true").option("materializationDataset", dataset_id).load()
    logging.info(f"Fetched {df.count()} rows from {bq_table_name} table")
    df.write.mode("overwrite").csv(f"gs://{gear_project_id}/{temp_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BigQueryToBigTable").getOrCreate()
    args = parse_arguments()
    fetch_bigquery_to_gcp(spark=spark , gear_project_id= args.gear_project_id , dataset_id=args.dataset_id , bq_table_name= args.bq_table_name , args.temp_path)
    logging.info("the data loaded from Bigtable to GCS")