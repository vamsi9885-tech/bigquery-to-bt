import argparse
import logging
import json
import yaml
from pyspark.sql import SparkSession
from utils import selective_df, add_audit_columns, get_active_date, get_rec_version, order_cols

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_yaml_config(yaml_path):
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def load_dict_config(dict_path):
    with open(dict_path, 'r') as f:
        return json.load(f)


def main(config_path, dict_path, start_date, end_date):
    config = load_yaml_config(config_path)
    data_dict = load_dict_config(dict_path)

    job_key = list(config.keys())[0]
    job_cfg = config[job_key]
    source_db = job_cfg['SOURCE_DB']
    target_db = job_cfg['TARGET_DB']
    target_tbl = job_cfg['TARGET_TBL']
    source_system = job_cfg['SOURCE_SYSTEM']

    spark = SparkSession.builder.appName(job_key).enableHiveSupport().getOrCreate()

    dfs = {}
    logger.info("Creating DataFrames based on dictionary definitions.")
    for tbl_key, params in data_dict.items():
        if tbl_key == "transformations":
            continue
        full_table_name = f"{source_db}.{tbl_key}"
        df = selective_df(spark, full_table_name, params, start_date, end_date)
        logger.info(f"Schema for {tbl_key}:")
        df.printSchema()
        logger.info(f"Row count for {tbl_key}: {df.count()}")
        dfs[tbl_key] = df

    logger.info("Applying transformations and joins.")
    joins = data_dict['transformations']['joins']
    result_df = dfs[joins[0]['table']]

    for join in joins[1:]:
        join_type = join['type']
        table = join['table']
        condition = join['condition']
        result_df = result_df.join(dfs[table], expr(condition), join_type)

    field_exprs = data_dict['transformations']['fields']
    result_df = result_df.selectExpr([f"{v} AS {k}" for k, v in field_exprs.items()])

    logger.info("Adding audit columns and formatting.")
    result_df = get_active_date(result_df, "trtmt_care_advised_dt")
    result_df = add_audit_columns(result_df, source_system, ["trtmt_care_servc_type", "trtmt_care_advised_dt", "trtmt_care_reqstd_dt"])
    result_df = get_rec_version(result_df, data_dict['transformations']['primary_key'])

    tbl_cols = spark.table(f"{target_db}.{target_tbl}").columns
    result_df = result_df.select(order_cols(result_df.columns, tbl_cols))

    logger.info("Saving final DataFrame to Hive table.")
    result_df.write.mode("append").saveAsTable(f"{target_db}.{target_tbl}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", required=True, help="Path to YAML config file")
    parser.add_argument("--dict_file", required=True, help="Path to JSON dictionary file")
    parser.add_argument("--start_date", required=False, help="Start date")
    parser.add_argument("--end_date", required=False, help="End date")
    args = parser.parse_args()

    main(args.config_file, args.dict_file, args.start_date, args.end_date)
