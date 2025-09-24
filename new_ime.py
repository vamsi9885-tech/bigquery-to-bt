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
