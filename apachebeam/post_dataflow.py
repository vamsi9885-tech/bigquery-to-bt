from google.cloud import bigtable, bigquery, storage
import logging
import argparse
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gear_project_id", help="Project ID")
    parser.add_argument("--temp_path" , help = "path for CSV file")
    return parser.parse_args()

def delete_gcs_path(bucket_name ,gcs_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if gcs_path.startswith("gs://"):
        gcs_path = gcs_path.replace(f"gs://{bucket_name}/","")
        gcs_path = gcs_path + "/*"
    if '*' in gcs_path:
        blobs = bucket.list_blobs(prefix = gcs_path)
        for blob in blobs:
            blob.delete()
            logging.info(f"deleted : {blob.name}")
    else:
        blob = bucket.blob(gcs_path)
        if blob.exists():
              blob.delete()
              logging.info(f"deleted : {gcs_path}")
        else:
            logging.info(f"blob not found {gcs_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BigQueryToBigTable").getOrCreate()
    args = parse_arguments()
    delete_gcs_path(bucket_name= args.gear_project_id , gcs_path= args.temp_path)
    logging.info("the data deleted from Bigtable to GCS")