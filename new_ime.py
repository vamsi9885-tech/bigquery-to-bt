#!/bin/python
#####################################################
# File Name: hub_pims_ime.py
# Type: Pyspark
# Purpose: Hub layer ETL for IME data using HubBase framework
# Created: 24/09/2025
# Author: IDP Team
#####################################################

from argparse import Namespace
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, concat_ws
)
from src.claim.hub_base import HubBase
from src.run.custom_logging import Log4j
import yaml, sys

class HubPimsIME(HubBase):
    """
    IME Hub table processor using HubBase framework.
    Implements transformation logic for IME Assessment + Claim data.
    """

    def __init__(self, hc: SparkSession, args: Namespace, yaml_config_file="hub_pims_ime.yaml") -> None:
        properties = {}
        with open(yaml_config_file, "r") as con:
            try:
                properties = yaml.safe_load(con)
            except yaml.YAMLError:
                sys.exit(1)

        granularity_keys = properties['HUB_PIMS_IME'].get("granularity_keys", "clm_no,ime_investigation_id")

        args.update(properties['HUB_PIMS_IME'])
        super().__init__(hc, args, granularity_keys=granularity_keys,
                         hive_partitions="source_system_cd,active_ym")

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        self.log.info(self.__class__.__name__, "Starting IME transformation")

        ime_line = ds["dL_TIMMedicalAssesmentIMELine"].alias("ime")
        claim_plan = ds["dl_TIMClaimPlanInvestigation"].alias("cpi")
        policy_claim = ds["dl_TIMPolicyClaim"].alias("pc")
        appt_status = ds["dl_TIMIMEAppointmentStatus"].alias("as")
        expert_spec = ds["dl_TIMExpertSpecialityCode"].alias("es")
        contact_prov = ds["dl_TIMContactTypeProvider"].alias("cp")

        # Joins
        base_df = (
            ime_line
            .join(claim_plan,
                  (col("ime.myInvestigationPlan_clsno") == col("cpi.oid_clsno")) &
                  (col("ime.myInvestigationPlan_instid") == col("cpi.oid_instid")),
                  "inner")
            .join(policy_claim,
                  (col("cpi.myClaim_clsno") == col("pc.oid_clsno")) &
                  (col("cpi.myClaim_instid") == col("pc.oid_instid")),
                  "inner")
            .join(appt_status,
                  (col("ime.myIMEApprovalStatus_clsno") == col("as.oid_clsno")) &
                  (col("ime.myIMEApprovalStatus_instid") == col("as.oid_instid")),
                  "left")
            .join(expert_spec,
                  (col("ime.myExpertSpeciality_clsno") == col("es.oid_clsno")) &
                  (col("ime.myExpertSpeciality_instid") == col("es.oid_instid")),
                  "left")
            .join(contact_prov,
                  (col("ime.mySpecialist_clsno") == col("cp.oid_clsno")) &
                  (col("ime.mySpecialist_instid") == col("cp.oid_instid")),
                  "left")
        )

        # Select & transform columns
        final_df = base_df.select(
            col("pc.aClaimNumber").alias("clm_no"),
            col("cpi.aUniqueId").alias("ime_investigation_id"),
            col("ime.aDateTime").alias("ime_dt"),
            concat_ws(": ", col("as.aCode"), col("as.aDescription")).alias("ime_appointmt_stat"),
            col("ime.aDateAssessment").alias("ime_assessmt_dt"),
            col("ime.aDateDoctorRequestBriefSent").alias("ime_dr_reqst_brief_sent"),
            concat_ws(": ", col("es.aCode"), col("es.aDescription")).alias("ime_experts_speciality"),
            col("cpi.aDateIMEPanelSent").alias("ime_panel_sent"),
            col("ime.aInvestigationType").alias("ime_investgtn_type"),
            col("ime.aOtherSpeciality").alias("ime_oth_specialty"),
            col("ime.aDateReportReceived").alias("ime_rpt_rcvd_dt"),
            col("cp.aKey").alias("ime_specialist_con_id"),
            col("cp._type").alias("ime_specialist_con_type")
        )

        self.log.info(self.__class__.__name__, "IME transformation completed successfully")
        return final_df









#!/usr/bin/env python
# hub_pims_ime.py
from argparse import Namespace
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import yaml, sys
from src.claim.hub_base import HubBase

class HubPimsIME(HubBase):
    def __init__(self, hc: SparkSession, args: Namespace, yaml_config_file="hub_pims_ime.yaml") -> None:
        properties = {}
        with open(yaml_config_file, "r") as con:
            try:
                properties = yaml.safe_load(con)
            except yaml.YAMLError:
                sys.exit(1)

        granularity_keys = properties['HUB_PIMS_IME'].get("granularity_keys", "clm_no,ime_investigation_id")
        args.update(properties['HUB_PIMS_IME'])
        super().__init__(hc, args, granularity_keys=granularity_keys, hive_partitions="source_system_cd,active_ym")

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        self.log.info(self.__class__.__name__, "Starting IME transformation")

        # source frames (names must match keys in ds)
        ime_line = ds["dL_TIMMedicalAssesmentIMELine"]
        cpi = ds["dl_TIMClaimPlanInvestigation"]
        pc = ds["dl_TIMPolicyClaim"]
        appt = ds["dl_TIMIMEAppointmentStatus"]
        expert = ds["dl_TIMExpertSpecialityCode"]
        contact = ds["dl_TIMContactTypeProvider"]

        # WINDOW definitions (use _timestamp desc, tie-breaker _tranid desc)
        w_oid = Window.partitionBy("oid_clsno", "oid_instid").orderBy(col("_timestamp").desc(), col("_tranid").desc())

        # rank_unique_TIMMedicalAssesmentIMELine -> pick latest per (oid_clsno, oid_instid) and non-deleted
        ime_ranked = ime_line.withColumn("rank_unique_TIMMedicalAssesmentIMELine", row_number().over(w_oid))
        unique_ime = ime_ranked.filter((col("rank_unique_TIMMedicalAssesmentIMELine") == 1) & (col("_operation") != 'D')).alias("ime")

        # rank_unique_TIMClaimPlanInvestigation -> latest per oid, then unique per myClaim (two-step as SQL)
        cpi_ranked = cpi.withColumn("rank_unique_TIMClaimPlanInvestigation", row_number().over(w_oid))
        cpi_ranked_f = cpi_ranked.filter((col("rank_unique_TIMClaimPlanInvestigation") == 1) & (col("_operation") != 'D'))
        w_claim = Window.partitionBy("myClaim_clsno", "myClaim_instid").orderBy(col("_timestamp").desc(), col("_tranid").desc())
        cpi_unique = cpi_ranked_f.withColumn("unique_TIMClaimPlanInvestigation", row_number().over(w_claim))
        claim_unique = cpi_unique.filter(col("unique_TIMClaimPlanInvestigation") == 1).alias("cpi")

        # rank_unique_TIMPolicyClaim -> latest per oid and non-deleted
        pc_ranked = pc.withColumn("rank_unique_TIMPolicyClaim", row_number().over(w_oid))
        pc_unique = pc_ranked.filter((col("rank_unique_TIMPolicyClaim") == 1) & (col("_operation") != 'D')).alias("pc")

        # rank_unique_TIMIMEAppointmentStatus
        appt_ranked = appt.withColumn("rank_unique_TIMIMEAppointmentStatus", row_number().over(w_oid))
        appt_unique = appt_ranked.filter((col("rank_unique_TIMIMEAppointmentStatus") == 1) & (col("_operation") != 'D')).alias("as")

        # rank_unique_TIMExpertSpecialityCode
        expert_ranked = expert.withColumn("rank_unique_TIMExpertSpecialityCode", row_number().over(w_oid))
        expert_unique = expert_ranked.filter((col("rank_unique_TIMExpertSpecialityCode") == 1) & (col("_operation") != 'D')).alias("es")

        # rank_unique_TIMContactTypeProvider
        contact_ranked = contact.withColumn("rank_unique_TIMContactTypeProvider", row_number().over(w_oid))
        contact_unique = contact_ranked.filter((col("rank_unique_TIMContactTypeProvider") == 1) & (col("_operation") != 'D')).alias("cp")

        # Joins (follow SQL: inner join claim_unique then inner join policy_claim, left joins for statuses)
        base_df = (
            unique_ime
            .join(claim_unique,
                  (col("ime.myInvestigationPlan_clsno") == col("cpi.oid_clsno")) &
                  (col("ime.myInvestigationPlan_instid") == col("cpi.oid_instid")),
                  "inner")
            .join(pc_unique,
                  (col("cpi.myClaim_clsno") == col("pc.oid_clsno")) &
                  (col("cpi.myClaim_instid") == col("pc.oid_instid")),
                  "inner")
            .join(appt_unique,
                  (col("ime.myIMEApprovalStatus_clsno") == col("as.oid_clsno")) &
                  (col("ime.myIMEApprovalStatus_instid") == col("as.oid_instid")),
                  "left")
            .join(expert_unique,
                  (col("ime.myExpertSpeciality_clsno") == col("es.oid_clsno")) &
                  (col("ime.myExpertSpeciality_instid") == col("es.oid_instid")),
                  "left")
            .join(contact_unique,
                  (col("ime.mySpecialist_clsno") == col("cp.oid_clsno")) &
                  (col("ime.mySpecialist_instid") == col("cp.oid_instid")),
                  "left")
        )

        # Final select (mirror SQL)
        final_df = base_df.select(
            col("pc.aClaimNumber").alias("clm_no"),
            col("cpi.aUniqueID").alias("ime_investigation_id"),
            col("ime.aDateTime").alias("ime_dt"),
            concat_ws(": ", col("as.aCode"), col("as.aDescription")).alias("ime_appointmt_stat"),
            col("ime.aDateAssessment").alias("ime_assessmt_dt"),
            col("ime.aDateDoctorRequestBriefSent").alias("ime_dr_reqst_brief_sent"),
            concat_ws(": ", col("es.aCode"), col("es.aDescription")).alias("ime_experts_speciality"),
            col("cpi.aDateIMEPanelSent").alias("ime_panel_sent"),
            col("ime.aInvestigationType").alias("ime_investgtn_type"),
            col("ime.aOtherSpeciality").alias("ime_oth_specialty"),
            col("ime.aDateReportReceived").alias("ime_rpt_rcvd_dt"),
            col("cp.aKey").alias("ime_specialist_con_id"),
            col("cp._type").alias("ime_specialist_con_type")
        )

        self.log.info(self.__class__.__name__, "IME transformation completed successfully")
        return final_df
