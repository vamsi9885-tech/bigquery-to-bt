import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws, lit, date_format, to_date, from_unixtime, unix_timestamp, col, flatten, rank
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from datetime import datetime  
from pyspark.sql.types import DecimalType, StringType, DateType
import sys
import pyspark

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Selective DataFrame") \
    .config("spark.executor.cores", "8") \
    .config("spark.driver.cores", "8") \
    .config("spark.driver.memory", "40g") \
    .config("spark.executor.memory", "40g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

source_db = "d_gi_lak_pims"
target_db = "d_gi_hub_ctp"
target_tbl = "hub_pimsctp_treatment_test_1"

def date_time_now():
    logger.info("Fetching current date-time.")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def create_sha_field(df, column_name, included_columns=[], num_bits=256):
    logger.info(f"Creating SHA field '{column_name}' with {num_bits}-bit encryption.")
    filtered_type = df.select(*included_columns).dtypes
    return df.withColumn(column_name, sha2(concat_ws("|", *map(
        lambda col_dtypes: flatten(df[col_dtypes[0]]) if "array<array" in col_dtypes[1] else col_dtypes[0],
        filtered_type)), num_bits))

def add_audit_columns(df, schema, excluded_columns, source_dt_format="yyyy-MM-dd"):
    logger.info("Adding audit columns.")
    sel_list = [x for x in df.columns if x not in excluded_columns]
    df = create_sha_field(df, "rec_sha", sel_list)
    if schema:
        df = df.withColumn("source_system_cd", lit(schema))
    df = df.withColumn("active_ym", date_format(to_date(df.active_dt, source_dt_format), "yyyy-MM"))
    time_now = date_time_now()
    df = df.withColumn("load_dt", lit(time_now))
    logger.info("Audit columns added successfully.")
    return df

def get_active_date(df, active_dt_column, input_date_format="yyyyMMDD", output_date_format="yyyy-MM-dd"):
    logger.info(f"Converting active date from {input_date_format} to {output_date_format}.")
    df = df.withColumn("active_dt",
                       from_unixtime(unix_timestamp(df[active_dt_column], input_date_format), output_date_format).cast(
                           TimestampType()))
    df = df.withColumn("active_ym", date_format(to_date(col("active_dt"), "yyyy-MM-dd"), "yyyy-MM"))
    logger.info("Active date conversion completed.")
    return df

def get_rec_version(df: DataFrame, key_cols: list) -> DataFrame:
    logger.info("Calculating record version number.")
    window_spec = Window.partitionBy(*[col(key) for key in key_cols]).orderBy(col("trtmt_care_advised_dt"))
    df_with_rec_version = df.withColumn("record_version_no", rank().over(window_spec))
    logger.info("Record version number calculated.")
    return df_with_rec_version     

def order_cols(my_cols, tbl_cols):
    logger.info("Ordering columns to match target table schema.")
    ordered_cols = [col(c) if c in my_cols else lit(None).alias(c) for c in tbl_cols]
    logger.info("Columns ordered successfully.")
    return ordered_cols

def selective_df(spark, table_name, columns, limit=100):
    logger.info(f"Selecting columns from table {table_name}.")
    if columns.strip().upper() == "ALL":
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
    else:
        query = f"SELECT {columns} FROM {table_name} LIMIT {limit}"
    df = spark.sql(query)
    logger.info(f"DataFrame '{table_name}' loaded successfully.")
    df.printSchema()
    return df

def save_hive(spark, df, hive_db_name, hive_table_name, mode="append", partition_column="source,active_ym",
              repartition=None, app_logger=None, target="TARGET"):
    logger.info(f"Saving DataFrame to Hive table {hive_db_name}.{hive_table_name}.")
    try:
        df.persist(pyspark.StorageLevel(True, True, False, False, 1))

        write_to_hive = {
            "append": df.write.mode("append").format("hive"),
            "append_parquet": df.write.mode("append").format("parquet"),
            "append_partition": df.write.format("parquet").partitionBy(*partition_column.split(",")).mode("append")
        }
        write_to_hive[mode].saveAsTable(hive_db_name + "." + hive_table_name)
        logger.info(f"DataFrame saved to Hive table {hive_db_name}.{hive_table_name} successfully.")
    except Exception as e:
        df.unpersist()
        spark.catalog.refreshTable(hive_db_name + "." + hive_table_name)
        logger.error(f"Error saving DataFrame to Hive: {e}")
        sys.exit(1)
    finally:
        df.unpersist()
        spark.catalog.refreshTable(hive_db_name + "." + hive_table_name)

logger.info("Starting data processing.")
treatment_applied_df = selective_df(spark, f"{source_db}.DL_TIMTreatmentApplied", "oid_clsno,oid_instid,_operation,row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TreatmentApplied,aUniqueId,aSessionsApproved,aSessionsRequested,aCompleted,aResponse,aServiceProvider,myType_clsno,myType_instid,aDateAdvised,aDateRequested,myClaimPlanInvestigation_clsno,myClaimPlanInvestigation_instid")

treatment_df = selective_df(spark, f"{source_db}.dl_TIMTreatment", "oid_clsno,oid_instid,_operation,row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_Treatment,aDescription")

medical_investigation_df = selective_df(spark, f"{source_db}.DL_TIMMedicalInvestigation", "oid_clsno,oid_instid,_operation,row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_MedicalInvestigation,aDescription")

claim_plan_investigation_df = selective_df(spark, f"{source_db}.dl_TIMClaimPlanInvestigation", "oid_clsno, oid_instid, _operation, row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ClaimPlanInvestigation, aRehabPlanInitiatedBy, aTreatmentManager, myClaim_clsno, myClaim_instid, myMedicalCertificate_clsno, myMedicalCertificate_instid, myExpectedTreatmentPeriod_clsno, myExpectedTreatmentPeriod_instid")

policy_claim_df = selective_df(spark, f"{source_db}.dl_TIMPolicyClaim", "oid_clsno, oid_instid, _operation, row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_PolicyClaim, aClaimNumber")

logger.info("Filtering and ranking unique records.")
rank_unique_treatment_applied_df = (treatment_applied_df
    .filter((sf.col("rank_unique_TreatmentApplied") == 1) & (sf.col("_operation") != 'D'))
)

rank_unique_treatment_df = (treatment_df
    .filter((sf.col("rank_unique_Treatment") == 1) & (sf.col("_operation") != 'D'))
)

rank_unique_medical_investigation_df = (medical_investigation_df
    .filter((sf.col("rank_unique_MedicalInvestigation") == 1) & (sf.col("_operation") != 'D'))
)

rank_unique_claim_plan_investigation_df = (claim_plan_investigation_df
    .filter((sf.col("rank_unique_ClaimPlanInvestigation") == 1) & (sf.col("_operation") != 'D'))
)

rank_unique_policy_claim_df = (policy_claim_df
    .filter((sf.col("rank_unique_PolicyClaim") == 1) & (sf.col("_operation") != 'D'))
)

logger.info("Joining tables.")
result_df = (rank_unique_treatment_applied_df
    .join(rank_unique_treatment_df,
          (rank_unique_treatment_applied_df.myType_clsno == rank_unique_treatment_df.oid_clsno) &
          (rank_unique_treatment_applied_df.myType_instid == rank_unique_treatment_df.oid_instid),
          "left")
    .join(rank_unique_medical_investigation_df,
          (rank_unique_treatment_applied_df.myType_clsno == rank_unique_medical_investigation_df.oid_clsno) &
          (rank_unique_treatment_applied_df.myType_instid == rank_unique_medical_investigation_df.oid_instid),
          "left")
    .join(rank_unique_claim_plan_investigation_df,
          (rank_unique_treatment_applied_df.myClaimPlanInvestigation_clsno == rank_unique_claim_plan_investigation_df.oid_clsno) &
          (rank_unique_treatment_applied_df.myClaimPlanInvestigation_instid == rank_unique_claim_plan_investigation_df.oid_instid),
          "left")
    .join(rank_unique_policy_claim_df,
          (rank_unique_claim_plan_investigation_df.myClaim_clsno == rank_unique_policy_claim_df.oid_clsno) &
          (rank_unique_claim_plan_investigation_df.myClaim_instid == rank_unique_policy_claim_df.oid_instid),
          "left")
)

logger.info("Selecting and transforming columns.")
result_df = result_df.select(
    rank_unique_treatment_applied_df.aUniqueId.alias('trtmt_no'),
    rank_unique_policy_claim_df.aClaimNumber.alias('clm_no'),
    rank_unique_treatment_applied_df.aSessionsApproved.alias('trtmt_no_of_sess_apprvd'),
    rank_unique_treatment_applied_df.aSessionsRequested.alias('trtmt_no_of_sess_reqstd'),
    rank_unique_treatment_applied_df.aCompleted.alias('trtmt_care_completed'),
    sf.when(rank_unique_treatment_applied_df.aResponse == 1, 'A')
      .when(rank_unique_treatment_applied_df.aResponse == 2, 'P')
      .when(rank_unique_treatment_applied_df.aResponse == 3, 'D')
      .otherwise('')
      .alias('trtmt_care_respnse'),
    rank_unique_treatment_applied_df.aServiceProvider.alias('trtmt_care_servc_provider'),
    sf.coalesce(rank_unique_treatment_df.aDescription, rank_unique_medical_investigation_df.aDescription).alias('trtmt_care_servc_type'),
    rank_unique_treatment_applied_df.aDateAdvised.alias('trtmt_care_advised_dt'),
    rank_unique_treatment_applied_df.aDateRequested.alias('trtmt_care_reqstd_dt'),
    rank_unique_claim_plan_investigation_df.aTreatmentManager.alias('trtmt_manager'),
    rank_unique_claim_plan_investigation_df.aRehabPlanInitiatedBy.alias('trtmt_first_plan_intiated_by')
)

logger.info("Adding audit columns.")
final_df = get_active_date(result_df, "trtmt_care_advised_dt")
final_df = add_audit_columns(final_df, "PIMS", ["trtmt_care_servc_type", "trtmt_care_advised_dt", "trtmt_care_reqstd_dt"])
final_df = get_rec_version(final_df, ["trtmt_no", "clm_no"])

logger.info("Ordering columns.")
df_cols = final_df.columns
tbl_cols = spark.sql(f"select * from {target_db}.{target_tbl}").columns
final_df = final_df.select(order_cols(df_cols, tbl_cols))
logger.info("Final DataFrame schema:")
final_df.printSchema()

logger.info("Saving final DataFrame to Hive.")
save_hive(spark, final_df, target_db, target_tbl)