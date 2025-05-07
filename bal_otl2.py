import os
from google.cloud import bigtable, bigquery, storage
import json
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.conf import SparkConf

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", help="Project ID")
    parser.add_argument("--gear_project_id", help="Gear Project ID")
    parser.add_argument("--bt_project_name", help="bigtable project name")
    parser.add_argument("--instance_id", help="bigtable instance ID")
    parser.add_argument("--bt_table_name", help="Bigtable Table name")
    parser.add_argument("--bq_table_name", help="Bigquery table name")
    parser.add_argument("--dataset_id", help="Bigquery Dataset name")
    parser.add_argument("--json_file", help="Path to JSON file")
    parser.add_argument("--row_key", help="Row key Column Name")
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


def load_catalog(dict_results,args):
    bt_table_name = args.bt_table_name
    column_mapping = dict_results["column_mapping"]
    catalog = json.dumps({
        "table": {"name": bt_table_name, "tableCoder": "PrimitiveType"},
        "rowkey": "bq_rowkey",
        "columns": column_mapping
    })
    logging.info(f'Defined Catalog for BigTable : {catalog}')
    return catalog


# def fetch_bigquery_data(spark, project_id, dataset_id, bq_table_name, dict_results):
#     logging.info(f"Fetching data from BigQuery table {bq_table_name}")
#     selected_columns = ", ".join(col for col in dict_results.keys() if col != "bq_rowkey")
#     query = f"SELECT (`{project_id}.{dataset_id}`.decrypt_sde(`{project_id}.{dataset_id}`.get_sde_tag('cm13','{bq_table_name}'),cm13) || '#' || hb9_date_stmt_yr || hb9_date_stmt_mo) as bq_rowkey, {selected_columns} FROM `{project_id}.{dataset_id}.{bq_table_name}` WHERE hb9_date_stmt_yr=2024 and hb9_date_stmt_mo='02' LIMIT 200"
#     # logging.info(f"Running this query : {query} ")
#     df = spark.read.format("bigquery").load(query).cache()
#     logging.info(f"Fetched {df.count()} rows from {bq_table_name} table")
#     logging.info(f'Total records fetched from BigQuery : {df.count()}')
#     logging.info(f'Number of Partitions : {df.rdd.getNumPartitions()}')
#     logging.info(f'Table Schema : {df.describe}')
#     return df

# fetching query in new way
def fetch_bigquery_data(spark, bq_table_name, dict_results):
    table_info = dict_results["table"]
    project = table_info["project"]
    dataset = table_info["dataset"]
    table_name = table_info["table_name"]

    column_mapping = dict_results["column_mapping"]
    excluded_columns = {"cm13","bq_rowkey"}
    column_keys = [key for key in column_mapping.keys() if key not in excluded_columns]
    selected_columns = ", ".join(column_keys)

    decrypt_expr = dict_results["decrypt_cm13"].format(project=project, dataset=dataset)
    query = dict_results["query_template"].format(decrypt_cm13=decrypt_expr,
                                                  columns=selected_columns,
                                                  project=project, dataset=dataset,
                                                  table_name=table_name)
    query = "SELECT concat(Request_ID,'-',current_date) as bq_rowkey , * FROM `prj-d-gbl-gar-epgear.temp.cars_logs` "
    logging.info(f"Executing this query : {query} ")

    df = spark.read.format("bigquery").load(query).cache()
    logging.info(f"Fetched {df.count()} rows from {bq_table_name} table")
    logging.info(f'Total records fetched from BigQuery : {df.count()}')
    logging.info(f'Number of Partitions : {df.rdd.getNumPartitions()}')
    logging.info(f'Table Schema : {df.describe}')
    #     logging.info(df.show())
    return df


def initialize_bigtable(bt_project_name, instance_id, bt_table_name):
    logging.info(f"Initializing Bigtable {bt_table_name}")
    bt_client = bigtable.Client(project=bt_project_name, admin=True)
    instance = bt_client.instance(instance_id)
    bt_table = instance.table(bt_table_name)
    logging.info("Bigtable initialized Successfully")
    return bt_table


def create_bigtable_table(bt_table, dict_results):
    column_mapping = dict_results["column_mapping"]
    column_family_set = {value["cf"] for key, value in column_mapping.items() if key != "bq_rowkey"}
    # column_family_set = set(value["cf"] for value in column_mapping.values())
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

def cast_columns_to_string(df, dict_result):
    column_mapping = dict_result["column_mapping"]
    for col_name in column_mapping.keys():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast("string"))
    return df

def export_data_to_bigtable(df, catalog, instance_id, bt_project_name):
    # Load in Big Table
    # logging.info("Dataframe schema before export :")
    # df.printSchema()
    # row_count= df.count()
    # logging.info(f"Total records {row_count}")

    logging.info("Sample bq_rowkey values :")
    df.select("bq_rowkey").show(5, truncate=False)
    # logging.info("Checking for row with null or empty rowkey :")
    # null_key_df= df.filter(col("bq_rowkey").isNull() | col("bq_rowkey") == "")
    # null_key_count= null_key_df.count()
    # logging.info(f"null bq_rowkey count is {null_key_count}")
    # df_filtered = df.filter(col("bq_rowkey").isNotNull() & col("bq_rowkey") != "")

    # df = df.fillna("null")
    # df.show(5)

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

def main_function():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
    logger = logging.getLogger(__name__)
    args = parse_arguments()

    project_id = args.project_id
    gear_project_id = args.gear_project_id
    bt_project_name = args.bt_project_name
    instance_id = args.instance_id
    bt_table_name = args.bt_table_name
    bq_table_name = args.bq_table_name
    dataset_id = args.dataset_id
    json_file = args.json_file
    row_key = args.row_key

    logging.info(f"Used arguments : {args}")

    logger.info('Initiating Spark Session')
    # .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0,com.google.cloud.bigtable:bigtable-hbase-2.x-hadoop:2.26.1')
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
    catalog = load_catalog(dict_results,args)
    bt_table = initialize_bigtable(bt_project_name, instance_id, bt_table_name)
    create_bigtable_table(bt_table, dict_results)
    df = fetch_bigquery_data(spark, bq_table_name, dict_results)
    df = cast_columns_to_string(df, dict_results)
    logging.info("schema after casting columns to string :")
    df.printSchema()
    export_data_to_bigtable(df, catalog, instance_id, bt_project_name)


if __name__ == "__main__":
    main_function()
