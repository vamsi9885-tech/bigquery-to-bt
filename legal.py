{
    "{db_env}_gi_lak_pims.dL_TIMMedicalAssesmentIMELine": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aDateTime,aDateAssessment,aDateDoctorRequestBriefSent,aInvestigationType,aOtherSpeciality,aDateReportReceived,myIMEApprovalStatus_clsno,myIMEApprovalStatus_instid,myExpertSpeciality_clsno,myExpertSpeciality_instid,mySpecialist_clsno,mySpecialist_instid,myInvestigationPlan_clsno,myInvestigationPlan_instid",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    },
    "{db_env}_gi_lak_pims.dl_TIMClaimPlanInvestigation": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aUniqueId,aDateIMEPanelSent,myClaim_clsno,myClaim_instid",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    },
    "{db_env}_gi_lak_pims.dl_TIMPolicyClaim": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aClaimNumber",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    },
    "{db_env}_gi_lak_pims.dl_TIMIMEAppointmentStatus": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aCode,aDescription",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    },
    "{db_env}_gi_lak_pims.dl_TIMExpertSpecialityCode": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aCode,aDescription",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    },
    "{db_env}_gi_lak_pims.dl_TIMContactTypeProvider": {
        "part_fields": "date(_timestamp)",
        "rnk_fields": "_timestamp desc,_tranid desc",
        "sel_fields": "_timestamp,oid_clsno,oid_instid,_operation,aKey,_type",
        "filter_con": "_operation != 'D'",
        "active_field": "_timestamp"
    }
}




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
from src.claim.hub_base import HubBase
from src.run.custom_logging import Log4j
import yaml,sys

class HubPimsctpTreatment(HubBase):
    """
    Treatment Hub table processor using HubBase framework.
    Implements transformation logic for treatment-applied data.
    """

    def __init__(self, hc: SparkSession, args: Namespace, yaml_config_file = "hub_pimsctp_ltc.yaml") -> None:
        """
        Initialize Treatment Hub ETL processor.

        Args:
            hc (SparkSession): Spark session.
            args (Namespace): Runtime arguments.
        """
        properties = {}
        with open(yaml_config_file, 'r') as con:
            try:
                properties = yaml.safe_load(con)
            except yaml.YAMLError as exc:
                sys.exit(1)

        # These fields uniquely identify an entity for SCD Type 2 versioning
        granularity_keys = properties['HUB_PIMSCTP_TREATMENT'].get("granularity_keys", "trtmt_no,clm_no")

        args.update(properties['HUB_PIMSCTP_TREATMENT'])
        super().__init__(hc, args, granularity_keys=granularity_keys, hive_partitions="source_system_cd,active_ym")

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
        self.log.info(self.__class__.__name__, f"trt_applied count {trt_applied.count()}")
        self.log.info(self.__class__.__name__, f"trt count {trt.count()}")
        self.log.info(self.__class__.__name__, f"med_inv count {med_inv.count()}")
        self.log.info(self.__class__.__name__, f"claim_plan count {claim_plan.count()}")
        self.log.info(self.__class__.__name__, f"policy claim count {policy_claim.count()}")
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
        self.log.info(self.__class__.__name__, f"count after base count {base_df.count()}")
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






With rank_unique_TIMMedicalAssesmentIMELine AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMMedicalAssesmentIMELine,
        aDateTime,
        aDateAssessment,
        aDateDoctorRequestBriefSent,
        aInvestigationType,
        aOtherSpeciality,
        aDateReportReceived,
        myIMEApprovalStatus_clsno,--supposed to be int -- Type mismatches in the source as date, this issue is fixed now..
        myIMEApprovalStatus_instid,
        myExpertSpeciality_clsno,
        myExpertSpeciality_instid,
        mySpecialist_clsno,
        mySpecialist_instid,
        myInvestigationPlan_clsno,
        myInvestigationPlan_instid
    from dL_TIMMedicalAssesmentIMELine
),
unique_TIMMedicalAssesmentIMELine AS
(
    Select
        *
    from rank_unique_TIMMedicalAssesmentIMELine
    Where
        rank_unique_TIMMedicalAssesmentIMELine = 1 AND _operation <> 'D'
),
rank_unique_TIMIMEAppointmentStatus AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMIMEAppointmentStatus,
        aCode,
        aDescription
    from dl_TIMIMEAppointmentStatus
),
rank_unique_TIMExpertSpecialityCode AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMExpertSpecialityCode,
        aCode,
        aDescription
    from dl_TIMExpertSpecialityCode
),
rank_unique_TIMContactTypeProvider AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMContactTypeProvider,
        aKey,
        _type
    from dl_TIMContactTypeProvider
),
rank_unique_TIMClaimPlanInvestigation AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMClaimPlanInvestigation,
        aUniqueId,
        aDateIMEPanelSent,
        myClaim_clsno,
        myClaim_instid
    From dl_TIMClaimPlanInvestigation
),
unique_TIMClaimPlanInvestigation AS
(
    Select
        *,
        row_number() over (partition by myClaim_clsno, myClaim_instid order by _timestamp desc) AS unique_TIMClaimPlanInvestigation
    from rank_unique_TIMClaimPlanInvestigation
    Where
        rank_unique_TIMClaimPlanInvestigation = 1 AND _operation <> 'D'
),
claim_unique_TIMClaimPlanInvestigation AS
(
    Select
        *
    from unique_TIMClaimPlanInvestigation
    Where
        unique_TIMClaimPlanInvestigation = 1
),
rank_unique_TIMPolicyClaim AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TIMPolicyClaim,
        aClaimNumber
    FROM dl_TIMPolicyClaim
)
Select --25 --11 --10 --220 --72--10 Ignore the number for validations purpose
    --distinct aClaimNumber, aUniqueID,aDateTime
    --*
    --distinct aClaimNumber
    aClaimNumber AS clm_no,
    aUniqueID AS ime_investigation_id,
    aDateTime AS ime_dt,
    concat(rank_unique_TIMIMEAppointmentStatus.aCode,': ',rank_unique_TIMIMEAppointmentStatus.aDescription) AS ime_appointmt_stat,
    aDateAssessment AS ime_assessmt_dt,
    aDateDoctorRequestBriefSent AS ime_dr_reqst_brief_sent,
    concat(rank_unique_TIMExpertSpecialityCode.aCode,': ',rank_unique_TIMExpertSpecialityCode.aDescription) AS ime_experts_speciality,
    aDateIMEPanelSent AS ime_panel_sent,
    aInvestigationType AS ime_investgtn_type,
    aOtherSpeciality AS ime_oth_specialty,
    aDateReportReceived AS ime_rpt_rcvd_dt,
    aKey AS ime_specialist_con_id,
    _type AS ime_specialist_con_type
From unique_TIMMedicalAssesmentIMELine
Inner Join claim_unique_TIMClaimPlanInvestigation
    ON(myInvestigationPlan_clsno = claim_unique_TIMClaimPlanInvestigation.oid_clsno
    AND myInvestigationPlan_instid = claim_unique_TIMClaimPlanInvestigation.oid_instid)
Inner Join rank_unique_TIMPolicyClaim
    ON(myClaim_clsno = rank_unique_TIMPolicyClaim.oid_clsno
    AND myClaim_instid = rank_unique_TIMPolicyClaim.oid_instid
    AND rank_unique_TIMPolicyClaim = 1
    AND rank_unique_TIMPolicyClaim._operation <> 'D')
Left Join rank_unique_TIMIMEAppointmentStatus
    ON(myIMEApprovalStatus_clsno = rank_unique_TIMIMEAppointmentStatus.oid_clsno AND
    myIMEApprovalStatus_instid = rank_unique_TIMIMEAppointmentStatus.oid_instid
    AND rank_unique_TIMIMEAppointmentStatus = 1
    AND rank_unique_TIMIMEAppointmentStatus._operation <> 'D')
Left Join rank_unique_TIMExpertSpecialityCode
    ON(myExpertSpeciality_clsno = rank_unique_TIMExpertSpecialityCode.oid_clsno AND
    myExpertSpeciality_instid = rank_unique_TIMExpertSpecialityCode.oid_instid
    AND rank_unique_TIMExpertSpecialityCode = 1
    AND rank_unique_TIMExpertSpecialityCode._operation <> 'D')
Left Join rank_unique_TIMContactTypeProvider
    ON(mySpecialist_clsno = rank_unique_TIMContactTypeProvider.oid_clsno AND
    mySpecialist_instid = rank_unique_TIMContactTypeProvider.oid_instid
    AND rank_unique_TIMContactTypeProvider = 1
    AND rank_unique_TIMContactTypeProvider._operation <> 'D')
