import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import argparse
import logging
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from src.utils.df_utils import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType, DecimalType, TimestampType
from pyspark.sql.functions import broadcast
from datetime import datetime


def hub_pimsctp_treatment(spark, props, app_logger):
    env_mapping = {
        "DEV": "d",
        "QA": "q",
        "UAT": "u",
        "PROD": "p"
    }
    env = env_mapping.get(props['env'], "d")
    app_logger.info(__name__,f"Started processing for {props['TARGET_TBL']}")
    app_logger.info(__name__,f"{props}")
    dic_file_path = os.path.join(os.getcwd(), props['CONFIG_DIC_FILE_NAME'])
    app_logger.info(__name__,f"Loading dictionary from {dic_file_path}")
    data_dict = load_dict_config(dic_file_path)
    target_tbl = props['TARGET_TBL']
    source_system = props['SOURCE_TYPE'].upper()
    source_db = props['SOURCE_DB'].strip().format(env=env)
    target_db = props['TARGET_DB'].strip().format(env=env)
    dfs = {}
    app_logger.info(__name__,"Creating DataFrames based on dictionary definitions.")
    for tbl_key, params in data_dict.items():
        if tbl_key == "transformations":
            continue
        full_table_name = f"{source_db}.{tbl_key}"
        df = selective_df(spark, full_table_name, params, )
        app_logger.info(__name__,f"Schema for {tbl_key}:")
        df.printSchema()
        app_logger.info(__name__,f"Row count for {tbl_key}: {df.count()}")
        dfs[tbl_key] = df

    app_logger.info(__name__,"Applying transformations and joins.")
    joins = data_dict['transformations']['joins']
    base_table = data_dict['transformations']['source_tbl']
    result_df = dfs[base_table]

    for join in joins[0:]:
        join_type = join['type']
        table = join['table']
        condition = join['condition']
        result_df = result_df.join(dfs[table], expr(condition), join_type)
        app_logger.info(__name__,f"{join['table']} and the schema after join : {result_df.printSchema()}")
    app_logger.info(__name__,f"final_schema after all the joins : {result_df.printSchema()}")
    field_exprs = data_dict['transformations']['fields']
    result_df = result_df.selectExpr([f"{v} AS {k}" for k, v in field_exprs.items()])

    app_logger.info(__name__,"Adding audit columns and formatting.")
    # STEP 4: Apply SCD Type 2 versioning
    dt_now = current_timestamp
    target_table_cnt = spark.table(f"{target_db}.{target_tbl}").where(col("source_system_cd") == props['SOURCE_TYPE'].upper()).limit(1).count()

    # Define primary key and windows - use business keys from dictionary
    primary_key_cols = table_columns.get("transformations", {}).get("primary_key")

    w = Window.partitionBy(col(*[col(key) for key in primary_key_cols])).orderBy(col("_timestamp"))
    wk = Window.partitionBy(col(*[col(key) for key in primary_key_cols]), col("_timestamp").cast(DateType())).orderBy(col("_timestamp").desc())
    if target_table_cnt == 0:
        app_logger.info(__name__, "First load detected - creating initial versions")
        audit_info = data_dict['transformations']
        active_date_col = audit_info.get("active_date_column", "trtmt_care_advised_dt")
        exclude_cols = audit_info.get("exclude_audit_columns", [])
        record_keys = audit_info.get("primary_key")
        result_df = get_active_date(result_df, active_date_col)
        app_logger.info(__name__,f"after adding active date : {result_df.printSchema()}")
        result_df = add_audit_columns(result_df, source_system, exclude_cols)
        app_logger.info(__name__,f"after adding audit cols : {result_df.printSchema()}")
        result_df = get_rec_version(result_df, record_keys,active_date_col)
        app_logger.info(__name__,f"after adding record_version : {result_df.printSchema()}")
        tbl_cols = spark.table(f"{target_db}.{target_tbl}").columns
        result_df = result_df.select(order_cols(result_df.columns, tbl_cols))
    else:
        app_logger.info(__name__, "Incremental load - merging with history")
        history_data = spark.table(f"{target_db}.{target_tbl}").select(
                 *[col(pk) for pk in primary_key_cols], 
                col("record_version_no"),
                col("rec_sha")
            )

        hw = Window.partitionBy(*[col(pk) for pk in primary_key_cols]).orderBy(col("record_version_no").desc())
        src_history_data = history_data
        for pk in primary_key_cols:
            src_history_data = src_history_data.withColumnRenamed(pk, f"hist_{pk}")
        src_history_data = (src_history_data
            .withColumn("rnk", rank().over(hw))
            .filter("rnk=1")
            .withColumnRenamed("record_version_no", "hist_rec_version")
            .withColumnRenamed("rec_sha", "rec_sha_history")
            .drop("rnk")
        )
        result_df = result_df
        for pk in primary_key_cols:
            result_df = result_df.withColumnRenamed(pk, f"tgt_{pk}")

        result_df = (result_df
            .withColumn("rnkk", rank().over(wk))
            .filter("rnkk=1")
            .drop("rnkk")
            .withColumn("lag_recsha", lag("rec_sha", 1).over(w))
            .filter("lag_recsha is null or lag_recsha!=rec_sha")
            .drop("lag_recsha")
            .withColumn("active_dt", col("_timestamp").cast(DateType()))
            .withColumn("load_dt", lit(dt_now))
            .withColumn("tgt_rnk", rank().over(w))
            .alias('tgt')
            .join(
            src_history_data.alias("h"),
            [col(f"h.hist_{pk}") == col(f"tgt.tgt_{pk}") for pk in primary_key_cols] + [col("tgt_rnk") == 1],
            "left_outer"
            )
            .withColumn("rec_sha_temp",when((col("rec_sha_history") == col("rec_sha")) & (col("tgt_rnk") == 1),col("hist_rec_version")).when(((col("h.rec_sha_history") != col("tgt.rec_sha")) & (col("tgt_rnk") == 1)) &(col("h.rec_sha_history").isNotNull()),col("hist_rec_version")).when((col("h.rec_sha_history").isNull()) & (col("tgt_rnk") == 1), "0").otherwise(None))
            .withColumn("rm_ind",when((col("h.rec_sha_history") == col("tgt.rec_sha")) & (col("tgt_rnk") == 1),"1").otherwise("0"))
            .filter("rm_ind!=1")
            .withColumn("record_version_no", (col("rec_sha_temp") + rank().over(w)).cast(StringType()))
            .drop("tgt_rnk", "rec_sha_temp", "hist_rec_version", 
          *[f"hist_{pk}" for pk in primary_key_cols], "rec_sha_history", "rm_ind")
        )


        final_data = final_data
        for pk in primary_key_cols:
            final_data = final_data.withColumnRenamed(pk, f"final_{pk}")

        final_data = (final_data
            .withColumn('active_ym', date_format(col("active_dt"), "yyyy-MM"))
            .withColumn('source', lit(source_type))
        )

    app_logger.info(__name__,"Saving final DataFrame to Hive table.")
    save_hive(spark=spark, 
                 df=result_df, 
                 columns_obj=props, 
                 hive_db_name=target_db,
                 hive_table_name=target_tbl, 
                 app_logger=app_logger, 
                 mode="append",
                 partition_column="source,active_ym")
    app_logger.info(__name__,f"data laoding completed for {__file__}")