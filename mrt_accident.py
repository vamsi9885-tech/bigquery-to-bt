import json
from pyspark.sql.functions import col, lit, when, rank, upper, date_format, to_date, date_sub, lag
from src.utils.df_utils import add_end_date,move_partitions
from src.modules.mart.mart_builder import read_source_table, get_delta_records,write_data_to_temp_table, save_data_to_target_table, derive_lag_hierarchy
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def transform_data(df_accident, df_claim):

    # LEFT JOIN on acc_no
    joined = df_accident.alias("acc").join(
            df_claim.alias("clm"),
            (F.col("acc.acc_no") == F.col("clm.acc_no")),
            how="left"
    )

    # Derived development fields (using acc.acc_dt)
    joined = (
        joined
        .withColumn("acc_dev_day", F.datediff(F.current_date(), F.to_date(F.col("acc.acc_dt"))))
        .withColumn("acc_dev_mth", F.floor(F.months_between(F.current_date(), F.to_date(F.col("acc.acc_dt")))))
        .withColumn("acc_dev_qtr", F.floor(F.months_between(F.current_date(), F.to_date(F.col("acc.acc_dt"))) / F.lit(3)))
    )

    # Select the projected columns (renaming to match the SQL output)
    final_df = joined.select(
        F.col("acc.acc_no").alias("acc_no"),
        F.col("acc.acc_null_flg").alias("acc_null_flg"),
        F.col("acc.acc_nulled_dt").alias("acc_nulled_dt"),
        F.col("acc.acc_nulled_rs").alias("acc_nulled_rs"),
        F.col("acc.acc_circum").alias("acc_circum"),
        F.col("acc.acc_dt").alias("acc_dt"),
        F.col("acc.acc_loc").alias("acc_loc"),
        F.col("acc.acc_blameless_flg").alias("acc_blameless_flg"),
        F.col("acc.acc_postcd").alias("acc_postcd"),
        F.col("acc.acc_profile").alias("acc_profile"),
        F.col("acc.acc_st_nm").alias("acc_st_nm"),
        F.col("acc.acc_st_nm_type").alias("acc_st_nm_type"),
        F.col("acc.acc_st_type_cd").alias("acc_st_type_cd"),
        F.col("acc.acc_suburb").alias("acc_suburb"),
        F.col("acc.acc_coord_geo").alias("acc_coord_geo"),
        F.col("acc.acc_tm").alias("acc_tm"),
        F.col("acc.acc_yr").alias("acc_yr"),
        F.col("acc.acc_yr_grp_fnl_tgt").alias("acc_yr_grp_fnl_tgt"),
        F.col("acc.acc_created_ts").alias("acc_created_ts"),
        F.col("acc.acc_entrd_dt").alias("acc_entrd_dt"),
        F.col("acc.acc_mth").alias("acc_mth"),
        F.col("acc.acc_rcvd_dt").alias("acc_rcvd_dt"),
        F.col("acc.acc_ads_involv").alias("acc_ads_involv"),
        F.col("acc.acc_nsw").alias("acc_nsw"),
        F.col("acc.acc_police_rpt_unavail").alias("acc_police_rpt_unavail"),
        F.col("acc.acc_rptd_police_dt").alias("acc_rptd_police_dt"),
        F.col("acc.acc_state").alias("acc_state"),
        F.col("acc_dev_day"),
        F.col("acc_dev_mth"),
        F.col("acc_dev_qtr"),
        F.col("acc.acc_mnging_insur_cd").alias("acc_mnging_insur_cd"),
        F.col("acc.acc_police_attnd_scene").alias("acc_police_attnd_scene"),
        F.col("acc.acc_police_rpt_no").alias("acc_police_rpt_no"),
        F.col("acc.active_dt").alias("active_dt"),
        F.col("acc.active_ym").alias("active_ym"),
        F.col("acc.load_dt").alias("load_dt"),
        F.col("acc.source_system_cd").alias("source_system_cd")

        
    )

    # GROUP BY the first 33 selected columns and COUNT(*)
    result = (
        final_df.groupBy(
            "acc_no",
            "acc_null_flg",
            "acc_nulled_dt",
            "acc_nulled_rs",
            "acc_circum",
            "acc_dt",
            "acc_loc",
            "acc_blameless_flg",
            "acc_postcd",
            "acc_profile",
            "acc_st_nm",
            "acc_st_nm_type",
            "acc_st_type_cd",
            "acc_suburb",
            "acc_coord_geo",
            "acc_tm",
            "acc_yr",
            "acc_yr_grp_fnl_tgt",
            "acc_created_ts",
            "acc_entrd_dt",
            "acc_mth",
            "acc_rcvd_dt",
            "acc_ads_involv",
            "acc_nsw",
            "acc_police_rpt_unavail",
            "acc_rptd_police_dt",
            "acc_state",
            "acc_dev_day",
            "acc_dev_mth",
            "acc_dev_qtr",
            "acc_mnging_insur_cd",
            "acc_police_attnd_scene",
            "acc_police_rpt_no",
            "active_dt",
            "active_ym",
            "load_dt",
            "source_system_cd"
        )
        .agg(F.count("*").alias("acc_cnt_of_clms"))
    )

    return result


def mrt_accident(spark, props, logger):

    conf_file = props['CONFIG_DIC_FILE_NAME']

    # Read config dic file
    json_obj = json.load(open(str(conf_file), 'r'))
    logger.info(__name__, f"Config Dic File: {json_obj}")

    target_df = spark.read.table(props['TARGET_DB'] + "." + props['TARGET_TBL'])
    source_table = props['SOURCE_TBL'].upper()
    source_filter = props['source_type'].upper()
    source_column = json_obj[source_table].get('source_column')
    if source_column is None:
        source_column = 'source_system_cd'
    key_columns = json_obj[source_table]['key_columns']
    orderby_columns = json_obj[source_table]['orderby_columns']
    partition_columns = json_obj[source_table]['partition_columns']
    enddate_deriving_column = json_obj[source_table]['enddate_deriving_column']
    
    df_accident = read_source_table(spark, props, json_obj, logger, source_col = source_column)
    df_claim = read_source_table(spark, props, json_obj, logger, source_col = source_column)
    source_df = transform_data(df_accident, df_claim)

    partition_path = props["TARGET_DB_PATH"] + "/" + props['TARGET_TBL'] + "/source=()/'".format(source_filter)

    if len(target_df.head(1)) == 0:
        logger.info(__name__, "No records in target table for particualr source, Executing full load")

        # Adding end date to source dataframe
        final_df = add_end_date(source_df, key_columns, orderby_columns, enddate_deriving_column)

        save_data_to_target_table(spark, props, json_obj, final_df, logger, partition_columns)
    else:
        delta_df = get_delta_records(target_df, source_df, props, json_obj, logger)

        if len(delta_df.take(1)) == 0:
            logger.info(__name__, "No delta records to process")
        else:
            final_df = derive_lag_hierarchy(delta_df, target_df, key_columns, orderby_columns, enddate_deriving_column)

            write_data_to_temp_table(spark, props, final_df, partition_columns, logger)

            temp_table = props['TARGET_TBL'] + '_' + props['source_type'] + '_tmp'

            move_partitions(spark, temp_table, props['TARGET_TBL'], props['TARGET_DB'], props["TARGET_DB_PATH"])

            # Drop temp table
            spark.sql("DROP TABLE IF EXISTS {}".format(props['TARGET_DB'] + "." + temp_table))
