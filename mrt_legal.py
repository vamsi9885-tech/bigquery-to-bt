import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.df_utils import add_end_date, move_partitions , selective_df
from src.modules.mart.mart_builder import (
    read_source_table,
    get_delta_records,
    write_data_to_temp_table,
    save_data_to_target_table,
    derive_lag_hierarchy
)


def transform_data(df_legal, df_claims, df_contact,logger):
    w_legal = Window.partitionBy("clm_no", "legl_aCode", "legl_rank_first").orderBy(F.col("active_dt").desc())
    w_legal_dense = Window.partitionBy("clm_no", "legl_aCode").orderBy(F.col("legl_rank_first").desc())
    rank_unique_legal = (
        df_legal
        .filter(F.col("legl_aCode").isin("01", "02"))
        .withColumn("rank_unique_legal", F.row_number().over(w_legal))
        .withColumn("legl_rank_latest_latest", F.dense_rank().over(w_legal_dense))
    )
    logger.info(__name__,f"{__name__} : rank_unique_legal schema {rank_unique_legal.printSchema()}")
    w_claims = Window.partitionBy("clm_no").orderBy(F.col("active_dt").desc())
    rank_unique_claims = df_claims.withColumn("rank_unique_claims", F.row_number().over(w_claims))
    logger.info(__name__,f"{__name__} : rank_unique_claims schema {rank_unique_claims.printSchema()}")
    w_contact = Window.partitionBy("con_id", "con_type").orderBy(F.col("active_dt").desc())
    rank_unique_contact = (
        df_contact
        .filter(F.col("con_type") == "ClaimLink")
        .withColumn("rank_unique_contact", F.row_number().over(w_contact))
    )
    logger.info(__name__,f"{__name__} : rank_unique_contact schema {rank_unique_contact.printSchema()}")
    legal_plaintaiff_change = (
        rank_unique_legal
        .filter((F.col("legl_aCode") == "02") & (F.col("rank_unique_legal") == 1))
        .groupBy("clm_no")
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("clm_no")
    )
    logger.info(__name__,f"{__name__} : legal_plaintaiff_change schema : {legal_plaintaiff_change.printSchema()}")
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
    logger.info(__name__,f"{__name__} : legal_plaintaiff_current_month schema {legal_plaintaiff_current_month.printSchema()}")
    legal_plaintaiff_change_current_month = (
        legal_plaintaiff_change.alias("pc")
        .join(legal_plaintaiff_current_month.alias("pcm"), "clm_no", "inner")
        .select("pc.clm_no")
    )
    logger.info(__name__,f"{__name__} : legal_plaintaiff_change_current_month schema : {legal_plaintaiff_change_current_month.printSchema()}")
    unique_legal_first_and_latest = (
        rank_unique_legal
        .filter(F.col("rank_unique_legal") == 1)
        .groupBy("clm_no", "legl_consultatn_dt", "legl_defendnt_counsel",
                 "legl_plntif_counsel", "legl_defendnt_refrl_type","active_dt","active_ym","load_dt","source_system_cd")
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
    logger.info(__name__,f"{__name__} : unique_legal_first_and_latest schema {unique_legal_first_and_latest.printSchema()}")
    final_df = unique_legal_first_and_latest.alias("unique_legal").join(
        legal_plaintaiff_change_current_month.alias("pccm"), "clm_no", "left"
        )
    logger.info(__name__,"Join with legal_plaintaiff_change_current_month completed. Current DataFrame: final_df")
    final_df.printSchema()
    # Step 2: Join with rank_unique_claims
    final_df = final_df.join(
        rank_unique_claims.filter(F.col("rank_unique_claims") == 1), "clm_no", "left"
    )
    logger.info(__name__,"Join with rank_unique_claims completed. Current DataFrame: final_df")
    final_df.printSchema()

    # Step 3: Join with rank_unique_contact for defendant contact
    final_df = final_df.join(
        rank_unique_contact.alias("def_contact").filter(F.col("rank_unique_contact") == 1),
        F.col("legl_defendnt_Contact_Id") == F.col("def_contact.con_id"), "left"
    )
    logger.info(__name__,"Join with rank_unique_contact for defendant contact completed. Current DataFrame: final_df")
    # Step 4: Join with rank_unique_contact for plaintiff contact
    final_df = final_df.join(
        rank_unique_contact.alias("plain_contact").filter(F.col("rank_unique_contact") == 1),
        F.col("legl_defendnt_Contact_Id") == F.col("plain_contact.con_id"), "left"
    )
    logger.info(__name__,"Join with rank_unique_contact for plaintiff contact completed. Current DataFrame: final_df")
    final_df.printSchema()
    logger.info(__name__,"Columns in final_df:")
    logger.info(__name__,f"{final_df.columns}")
    # Step 5: Select the required columns
    logger.info(__name__,f"count of the final df{final_df.count()}")
    final_df = final_df.select(
    "clm_no",
    "legl_consultatn_dt",
    "legl_defendnt_counsel",
    "legl_plntif_counsel",
    "legl_defendnt_refrl_type",
    "legl_defendnt_rep_flg",
    "legl_defendnt_dt",
    "legl_plntif_reprstd_flg",
    "legl_plntif_reprstd_dt",
    "legl_first_defendnt_rep_dt",
    "legl_first_plntif_reprstd_dt",
    F.when(F.col("pccm.clm_no").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("legl_plntif_swtchd_mtd_ind"),
    F.lit(1).alias("legl_plntif_rep_at_lodgmnt_ind"),
    F.col("plain_contact.con_first_nm").alias("con_plntif_legl_reprsntatve_first_nm"),
    F.col("plain_contact.con_surname").alias("con_plntif_legl_reprsntatve_surname"),
    F.col("plain_contact.con_home_email_addr").alias("con_plntif_legl_email_addr"),
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
                ).alias("con_plntif_legl_phy_addr"),
    F.col("unique_legal.active_dt").alias("active_dt"),
    F.col("unique_legal.active_ym").alias("active_ym"),
    F.col("unique_legal.load_dt").alias("load_dt"),
    F.col("unique_legal.source_system_cd").alias("source_system_cd")
    # F.col("unique_legal.legl_acode").alias("legl_acode"),
    # F.col("unique_legal.legl_rank_first").alias("legl_rank_first")
    
    
    )

    logger.info(__name__,f"count of the final data frame is {final_df.count()}")
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
    source_db = props['SOURCE_DB']

    # Read source tables
    logger.info(__name__,f"json_obj : {json_obj}")
    logger.info(__name__,f"props : {props}")
    df_legal = read_source_table(spark, props, json_obj,logger,source_col= source_column)
    df_claims = selective_df(spark, f"{source_db}.hub_pimsctp_claims", "ALL")
    df_contact = selective_df(spark, f"{source_db}.hub_pimsctp_contact", "ALL")
    logger.info(__name__,"read all the source data")
    source_df = transform_data(df_legal, df_claims, df_contact,logger)

    partition_path = props["TARGET_DB_PATH"] + "/" + props['TARGET_TBL'] + "/source={}/".format(source_filter)

    if len(target_df.head(1)) == 0:
        logger.info(__name__, "No records in target table for particular source, Executing full load")
        
        final_df = add_end_date(source_df, key_columns, orderby_columns, enddate_deriving_column)
        logger.info(__name__,f"added the end date to  the findal df")
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
