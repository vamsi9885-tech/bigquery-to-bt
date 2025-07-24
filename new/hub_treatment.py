#!/bin/python
#####################################################
# File Name: hub_pims_treatment.py
# Type: Pyspark
# Purpose: Hub layer ETL for Treatment data using HubBase framework
# Created: 23/07/2025
# Author: IDP Team
#####################################################

from argparse import Namespace
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce
)
from src.ltc.hub_base import HubBase
from src.run.custom_logging import Log4j


class HubPimsctpTreatment(HubBase):
    """
    Treatment Hub table processor using HubBase framework.
    Implements transformation logic for treatment-applied data.
    """

    def __init__(self, hc: SparkSession, args: Namespace) -> None:
        """
        Initialize Treatment Hub ETL processor.

        Args:
            hc (SparkSession): Spark session.
            args (Namespace): Runtime arguments.
        """
        granularity_keys = "trtmt_no,clm_no"  # Unique keys for SCD versioning
        super().__init__(hc, args, granularity_keys=granularity_keys, hive_partitions="source,active_ym")

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """
        Transform treatment source tables into hub schema.

        Args:
            source (str): Source system name.
            ds (Dict[str, DataFrame]): Loaded DataFrames (keyed by table name).

        Returns:
            DataFrame: Transformed treatment DataFrame.
        """

        self.log.info(self.__class__.__name__, "Starting treatment transformation")

        # Base Treatment Applied Table
        trt_applied = ds["dl_TIMTreatmentApplied"].alias("ta")
        trt = ds["dl_TIMTreatment"].alias("t")
        med_inv = ds["dl_TIMMedicalInvestigation"].alias("mi")
        claim_plan = ds["dl_TIMClaimPlanInvestigation"].alias("cpi")
        policy_claim = ds["dl_TIMPolicyClaim"].alias("pc")

        # Joins
        base_df = (
            trt_applied
            .join(trt, (col("ta.myType_clsno") == col("t.oid_clsno")) &
                        (col("ta.myType_instid") == col("t.oid_instid")), "left")
            .join(med_inv, (col("ta.myType_clsno") == col("mi.oid_clsno")) &
                           (col("ta.myType_instid") == col("mi.oid_instid")), "left")
            .join(claim_plan, (col("ta.myClaimPlanInvestigation_clsno") == col("cpi.oid_clsno")) &
                              (col("ta.myClaimPlanInvestigation_instid") == col("cpi.oid_instid")), "left")
            .join(policy_claim, (col("cpi.myClaim_clsno") == col("pc.oid_clsno")) &
                                 (col("cpi.myClaim_instid") == col("pc.oid_instid")), "left")
        )

        # Transformations / Final Column Mapping
        final_df = base_df.select(
            col("ta.aUniqueId").alias("trtmt_no"),
            col("pc.aClaimNumber").alias("clm_no"),
            col("ta.aSessionsApproved").alias("trtmt_no_of_sess_apprvd"),
            col("ta.aSessionsRequested").alias("trtmt_no_of_sess_reqstd"),
            col("ta.aCompleted").alias("trtmt_care_completed"),
            when(col("ta.aResponse") == 1, lit("A"))
            .when(col("ta.aResponse") == 2, lit("P"))
            .when(col("ta.aResponse") == 3, lit("D"))
            .otherwise(lit("")).alias("trtmt_care_respnse"),
            col("ta.aServiceProvider").alias("trtmt_care_servc_provider"),
            coalesce(col("t.aDescription"), col("mi.aDescription")).alias("trtmt_care_servc_type"),
            col("ta.aDateAdvised").alias("trtmt_care_advised_dt"),
            col("ta.aDateRequested").alias("trtmt_care_reqstd_dt"),
            col("cpi.aTreatmentManager").alias("trtmt_manager"),
            col("cpi.aRehabPlanInitiatedBy").alias("trtmt_first_plan_intiated_by")
        )

        self.log.info(self.__class__.__name__, "Treatment transformation completed successfully")
        return final_df


# Entry point
def hub_treatment_processor(hc: SparkSession, args: Namespace, app_logger):
    """
    Entry point function to execute treatment hub processor.

    Args:
        hc (SparkSession): Spark session.
        args (Namespace): Runtime arguments.
        app_logger: Logger.
    """
    processor = HubPimsctpTreatment(hc, args)
    processor.run()
