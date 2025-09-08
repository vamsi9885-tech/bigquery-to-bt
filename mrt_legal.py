import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.df_utils import add_end_date, move_partitions
from src.modules.mart.mart_builder import (
    read_source_table,
    get_delta_records,
    write_data_to_temp_table,
    save_data_to_target_table,
    derive_lag_hierarchy
)


def transform_data(df_legal, df_claims, df_contact):
    # --------------------------
    # rank_unique_legal
    # --------------------------
    w_legal = Window.partitionBy("clm_no", "legl_aCode", "legl_rank_first").orderBy(F.col("active_dt").desc())
    w_legal_dense = Window.partitionBy("clm_no", "legl_aCode").orderBy(F.col("legl_rank_first").desc())

    rank_unique_legal = (
        df_legal
        .filter(F.col("legl_aCode").isin("01", "02"))
        .withColumn("rank_unique_legal", F.row_number().over(w_legal))
        .withColumn("legl_rank_latest_latest", F.dense_rank().over(w_legal_dense))
    )

    # --------------------------
    # rank_unique_claims
    # --------------------------
    w_claims = Window.partitionBy("clm_no").orderBy(F.col("active_dt").desc())
    rank_unique_claims = df_claims.withColumn("rank_unique_claims", F.row_number().over(w_claims))

    # --------------------------
    # rank_unique_contact
    # --------------------------
    w_contact = Window.partitionBy("con_id", "con_type").orderBy(F.col("active_dt").desc())
    rank_unique_contact = (
        df_contact
        .filter(F.col("con_type") == "ClaimLink")
        .withColumn("rank_unique_contact", F.row_number().over(w_contact))
    )

    # --------------------------
    # legal_plaintaiff_change
    # --------------------------
    legal_plaintaiff_change = (
        rank_unique_legal
        .filter((F.col("legl_aCode") == "02") & (F.col("rank_unique_legal") == 1))
        .groupBy("clm_no")
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("clm_no")
    )

    # --------------------------
    # legal_plaintaiff_current_month
    # --------------------------
    first_day = F.trunc(F.current_date(), "MM")
    legal_plaintaiff_current_month = (
        rank_unique_legal
        .filter(
            (F.col("legl_aCode") == "02") &
            (F.col("rank_unique_legal") == 1) &
            (F.col("legl_rep_date") >= first_day) &
            (F.col("legl_rep_date") <= F.current_date())
        )
        .select("clm_no").distinct()
    )

    # --------------------------
    # legal_plaintaiff_change_current_month
    # --------------------------
    legal_plaintaiff_change_current_month = (
        legal_plaintaiff_change.alias("pc")
        .join(legal_plaintaiff_current_month.alias("pcm"), "clm_no", "inner")
        .select("pc.clm_no")
    )

    # --------------------------
    # unique_legal_first_and_latest
    # --------------------------
    unique_legal_first_and_latest = (
        rank_unique_legal
        .filter(F.col("rank_unique_legal") == 1)
        .groupBy("clm_no", "legl_consultatn_dt", "legl_defendnt_counsel",
                 "legl_plntif_counsel", "legl_defendnt_refrl_type")
        .agg(
            F.max(F.when((F.col("legl_aCode") == "01") & (F.col("legl_rank_latest_latest") == 1),
                         F.col("legl_rep_date"))).alias("legl_defendnt_dt"),
            F.max(F.when((F.col("legl_aCode") == "01") & (F.col("legl_rank_latest_latest") == 1),
                         F.col("legl_Contact_Id"))).alias("legl_defendnt_Contact_Id"),
            F.max(F.when((F.col("legl_aCode") == "02") & (F.col("legl_rank_latest_latest") == 1),
                         F.col("legl_rep_date"))).alias("legl_plntif_reprstd_dt"),
            F.max(F.when((F.col("legl_aCode") == "02") & (F.col("legl_rank_latest_latest") == 1),
                         F.col("legl_Contact_Id"))).alias("legl_plntif_Contact_Id"),
            F.max(F.when((F.col("legl_aCode") == "01") & (F.col("legl_rank_first") == 1),
                         F.col("legl_rep_date"))).alias("legl_first_defendnt_rep_dt"),
            F.max(F.when((F.col("legl_aCode") == "02") & (F.col("legl_rank_first") == 1),
                         F.col("legl_rep_date"))).alias("legl_first_plntif_reprstd_dt"),
            (F.when(F.sum(F.when(F.col("legl_aCode") == "01", 1).otherwise(0)) > 0, F.lit(1))
             .otherwise(F.lit(0))).alias("legl_defendnt_rep_flg"),
            (F.when(F.sum(F.when(F.col("legl_aCode") == "02", 1).otherwise(0)) > 0, F.lit(1))
             .otherwise(F.lit(0))).alias("legl_plntif_reprstd_flg"),
        )
    )

    # --------------------------
    # Final SELECT with joins
    # --------------------------
    final_df = (
        unique_legal_first_and_latest
        .join(legal_plaintaiff_change_current_month.alias("pccm"), "clm_no", "left")
        .join(rank_unique_claims.filter(F.col("rank_unique_claims") == 1), "clm_no", "left")
        .join(rank_unique_contact.alias("def_contact").filter(F.col("rank_unique_contact") == 1),
              F.col("legl_defendnt_Contact_Id") == F.col("def_contact.con_id"), "left")
        .join(rank_unique_contact.alias("plain_contact").filter(F.col("rank_unique_contact") == 1),
              F.col("legl_defendnt_Contact_Id") == F.col("plain_contact.con_id"), "left")
        .select(
            "clm_no",
            "legl_consultatn_dt",
            "legl_defendnt_counsel",
            "legl_plntif_counsel",
            "legl_defendnt_refrl_type",
            "legl_defendnt_rep_flg",
            "legl_defendnt_dt",
            "legl_defendnt_Contact_Id",
            "legl_plntif_reprstd_flg",
            "legl_plntif_reprstd_dt",
            "legl_plntif_Contact_Id",
            "legl_first_defendnt_rep_dt",
            "legl_first_plntif_reprstd_dt",
            F.when(F.col("pccm.clm_no").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("legl_plntif_swtchd_mtd_ind"),
            F.lit(1).alias("legl_plntif_rep_at_lodgmnt_ind"),
            F.col("plain_contact.con_first_nm").alias("con_plntif_legl_reprsntatve_first_nm"),
            F.col("plain_contact.con_surname").alias("con_plntif_legl_reprsntatve_surname"),
            F.col("plain_contact.con_home_email").alias("con_plntif_legl_email_addr"),
            F.concat(F.col("plain_contact.con_home_ph_area_cd"),
                     F.col("plain_contact.con_home_ph_no")).alias("con_plntif_legl_ph_no"),
            F.col("def_contact.con_abn").alias("legl_defendnt_abn"),
            F.col("def_contact.con_firm").alias("legl_defendnt_firm"),
            F.col("def_contact.con_phy_addr_PostCode").alias("con_defendnt_con_postcd"),
            F.col("plain_contact.con_abn").alias("con_plntif_legl_abn"),
            F.col("plain_contact.con_firm").alias("con_plntif_legl_firm"),
            F.col("plain_contact.con_phy_addr_PostCode").alias("con_plntif_legl_phy_postcd"),
            F.col("plain_contact.con_post_addr_PostCode").alias("con_plntif_legl_postcd"),
            F.when(F.col("plain_contact.con_classification") == "Individual",
                   F.concat_ws(" ", "plain_contact.con_title", "plain_contact.con_first_nm", "plain_contact.con_surname"))
            .when(F.col("plain_contact.con_classification") == "Organisation",
                  F.col("plain_contact.con_org_nm")).alias("con_plntif_legl_reprsntatve_nm"),
            F.col("plain_contact.con_brch").alias("con_plntif_legl_brch"),
            F.concat_ws(",",
                        "plain_contact.con_phy_addr_line_1",
                        "plain_contact.con_phy_addr_line_2",
                        "plain_contact.con_phy_addr_Name",
                        "plain_contact.con_phy_addr_propertyName",
                        "plain_contact.con_phy_addr_unitNumber",
                        "plain_contact.con_phy_addr_LevelNo",
                        "plain_contact.con_phy_addr_StreetNumber",
                        "plain_contact.con_phy_addr_StreetName",
                        "plain_contact.con_phy_addr_StreetType",
                        "plain_contact.con_phy_addr_SuburbName",
                        "plain_contact.con_phy_addr_StateName",
                        "plain_contact.con_phy_addr_StateCode"
                        ).alias("con_plntif_legl_phy_addr")
        )
    )

    return final_df


def mrt_pimsctp_legal(spark, props, logger):
    conf_file = props['CONFIG_DIC_FILE_NAME']

    # Read config dic file
    json_obj = json.load(open(str(conf_file), 'r'))
    logger.info(__name__, f"Config Dic File: {json_obj}")

    target_df = spark.read.table(props['TARGET_DB'] + "." + props['TARGET_TBL'])
    source_table = props['SOURCE_TBL'].upper()
    source_filter = props['source_type'].upper()
    source_column = json_obj[source_table].get('source_column', 'source_system_cd')
    key_columns = json_obj[source_table]['key_columns']
    orderby_columns = json_obj[source_table]['orderby_columns']
    partition_columns = json_obj[source_table]['partition_columns']
    enddate_deriving_column = json_obj[source_table]['enddate_deriving_column']

    # Read source tables
    df_legal = read_source_table(spark, props, json_obj, logger, source_col=source_column)
    df_claims = read_source_table(spark, props, json_obj, logger, source_col=source_column)
    df_contact = read_source_table(spark, props, json_obj, logger, source_col=source_column)

    source_df = transform_data(df_legal, df_claims, df_contact)

    partition_path = props["TARGET_DB_PATH"] + "/" + props['TARGET_TBL'] + "/source={}/".format(source_filter)

    if len(target_df.head(1)) == 0:
        logger.info(__name__, "No records in target table for particular source, Executing full load")
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
            spark.sql("DROP TABLE IF EXISTS {}".format(props['TARGET_DB'] + "." + temp_table))
