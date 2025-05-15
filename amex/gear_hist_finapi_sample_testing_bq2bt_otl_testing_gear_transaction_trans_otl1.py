import os
from google.cloud import bigtable, bigquery, storage
import json
import logging
import argparse
from pyspark.sql import SparkSession

def main_function():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
    logger = logging.getLogger(__name__)
    args = parse_arguments()

    project_id = args.project_id
    gear_project_id = args.gear_project_id
    bt_project_name = args.bt_project_name
    instance_id = args.instance_id
    dataset_id = args.dataset_id
    json_file = args.json_file

    logging.info(f"Used arguments : {args}")

    logger.info('Initiating Spark Session')
    spark = (
        SparkSession.builder
        .appName("BigQuery to Bigtable")
        .master("yarn")
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigtable-with-dependencies_2.12-0.4.0.jar')
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0')
        .config('spark.jars.packages', 'com.google.cloud.bigtable:bigtable-hbase-2.x-hadoop:2.26.1')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .getOrCreate()
    )
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationProject", gear_project_id)
    spark.conf.set("materializationDataset", "temp")

    logging.info("Starting BigQuery to BigTable loading process")
    dict_results = load_json(gear_project_id, json_file)
    catalog = load_catalog(dict_results)
    bt_table = initialize_bigtable(bt_project_name, instance_id, dict_results)
    create_bigtable_table(bt_table, dict_results)
    df = fetch_bigquery_data(spark, project_id, dataset_id, dict_results)
    export_data_to_bigtable(df, catalog, instance_id, bt_project_name)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", help="Project ID")
    parser.add_argument("--gear_project_id", help="Gear Project ID")
    parser.add_argument("--bt_project_name", help="bigtable project name")
    parser.add_argument("--instance_id", help="bigtable instance ID")
    parser.add_argument("--dataset_id", help="Bigquery Dataset name")
    parser.add_argument("--json_file", help="Path to JSON file")
    return parser.parse_args()


def load_json(gear_project_id, json_file):
    json_name = os.path.basename(json_file)
    logging.info(f"Loading JSON file {json_name}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(gear_project_id)
    blob = bucket.blob(json_file)
    str_json = blob.download_as_text()
    dict_results = json.loads(str_json)
    logging.info("JSON file loaded Successfully")
    return dict_results


def load_catalog(dict_results):
    bt_table_name = dict_results["table"]["bt_table_name"]         # changes
    column_mapping = dict_results["column_mapping"]
    catalog = json.dumps({
        "table": {"name": bt_table_name, "tableCoder": "PrimitiveType"},
        "rowkey": "bq_rowkey",
        "columns": column_mapping
    })
    logging.info(f'Defined Catalog for BigTable : {catalog}')
    return catalog

def fetch_bigquery_data(spark, project_id, dataset_id,dict_results):
    bq_table_name = dict_results["table"]["bq_table_name"]
    column_mapping = dict_results["column_mapping"]
    excluded_columns = {"bq_rowkey","cm13","cm15","decrypt_cm13","decrypt_cm15"}
    column_keys = [key for key in column_mapping.keys() if key not in excluded_columns]
    selected_columns = ", ".join(column_keys)
    query_template = dict_results["query"]
    decrypt_expr = {
        key: value.format(project_id=project_id, dataset_id=dataset_id, bq_table_name=bq_table_name) for key, value in dict_results["decrypt_expr"].items()
    }
    decrypt_cm13 = decrypt_expr["decrypt_cm13"]
    decrypt_cm15 = decrypt_expr["decrypt_cm15"]
    query = query_template.format(decrypt_cm13=decrypt_cm13,
                                  decrypt_cm15=decrypt_cm15,
                                  columns=selected_columns,
                                  project_id=project_id, dataset_id=dataset_id,
                                  bq_table_name=bq_table_name)
    logging.info(f"Executing this query : {query} ")
    df = spark.read.format("bigquery").load(query).cache()
    logging.info(f"Fetched {df.count()} rows from {bq_table_name} table")
    logging.info(f'Total records fetched from BigQuery : {df.count()}')
    logging.info(f'Number of Partitions : {df.rdd.getNumPartitions()}')
    logging.info(f'Table Schema : {df.describe}')
    return df


def initialize_bigtable(bt_project_name, instance_id, dict_results):
    bt_table_name = dict_results["table"]["bt_table_name"]         # changes
    logging.info(f"Initializing Bigtable {bt_table_name}")
    bt_client = bigtable.Client(project=bt_project_name, admin=True)
    instance = bt_client.instance(instance_id)
    bt_table = instance.table(bt_table_name)
    logging.info("Bigtable initialized Successfully")
    return bt_table


def create_bigtable_table(bt_table, dict_results):
    column_mapping = dict_results["column_mapping"]
    column_family_set = {value["cf"] for key, value in column_mapping.items() if key != "bq_rowkey"}
    logging.info(f"column families in the json are : {column_family_set}")
    if not bt_table.exists():
        logging.info(f"Table {bt_table.table_id} not exists")
        bt_table.create()
        logging.info(f"Table {bt_table.table_id} created")
        for cf_name in column_family_set:
            column_family = bt_table.column_family(cf_name)
            column_family.create()
            logging.info(f'Column family {cf_name} created')
    else:
        logging.info(f'Table {bt_table.table_id} already exists. Checking column Families')


def export_data_to_bigtable(df, catalog, instance_id, bt_project_name):
    logging.info("writing df to bigtable...")
    df.write.format("bigtable") \
        .option("catalog", catalog) \
        .option("spark.bigtable.project.id", bt_project_name) \
        .option("spark.bigtable.instance.id", instance_id) \
        .option("spark.bigtable.enable.batch_mutate.flow_control", "true") \
        .option("spark.bigtable.app_profile.id", "gear_load") \
        .option("spark.bigtable.create.new.table", "false") \
        .save()

    logging.info("Data Successfully exported from BQ to BT")
    return "Success"

if __name__ == "__main__":
    main_function()