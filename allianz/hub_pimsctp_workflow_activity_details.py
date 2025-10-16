#!/bin/python

#####################################################
# File Name: workflow_activity_details.py
# Type: Pyspark
# Purpose: Transform workflow / contact event activity details
#          into the canonical hub schema using a
#          dictionary-driven approach (standalone transform).
# Created: 2025-10-16
# Author: Allianz Data Engineering
#####################################################

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit


def transform(ds: Dict[str, DataFrame]) -> DataFrame:
    """
    Build workflow/activity details DataFrame from input dictionary of DataFrames.

    Args:
        ds (Dict[str, DataFrame]): dictionary keyed by table name (no DB prefix).
            Required keys:
              - dl_TIMContactEvent
              - dl_TIMPolicyClaim
              - dl_TIMPolicy
              - dl_CTPAccident
              - dl_TIMContactEventMethod
              - dl_TIMContactEventReason
              - dl_TIMContactEventType
              - dl_TIMContactEventStatus
              - dl_TIMContactIndividual
              - dl_TIMContactTypeUser

    Returns:
        DataFrame: transformed workflow activity details
    """
    print("Starting workflow_activity_details transformation...")

    # ---------------------------------------------------------------------
    # Validate input tables
    # ---------------------------------------------------------------------
    required_tables = [
        "dl_TIMContactEvent",
        "dl_TIMPolicyClaim",
        "dl_TIMPolicy",
        "dl_CTPAccident",
        "dl_TIMContactEventMethod",
        "dl_TIMContactEventReason",
        "dl_TIMContactEventType",
        "dl_TIMContactEventStatus",
        "dl_TIMContactIndividual",
        "dl_TIMContactTypeUser",
    ]
    missing = [t for t in required_tables if t not in ds]
    if missing:
        raise ValueError(f"Missing required DataFrames in ds: {missing}")

    # ---------------------------------------------------------------------
    # Assign aliases directly (since data is already deduplicated upstream)
    # ---------------------------------------------------------------------
    ev = ds["dl_TIMContactEvent"].alias("ev")
    clm = ds["dl_TIMPolicyClaim"].alias("clm")
    pol = ds["dl_TIMPolicy"].alias("pol")
    acc = ds["dl_CTPAccident"].alias("acc")
    meth = ds["dl_TIMContactEventMethod"].alias("meth")
    rsn = ds["dl_TIMContactEventReason"].alias("rsn")
    typ = ds["dl_TIMContactEventType"].alias("typ")
    sts = ds["dl_TIMContactEventStatus"].alias("sts")
    indv = ds["dl_TIMContactIndividual"].alias("indv")
    usr = ds["dl_TIMContactTypeUser"].alias("usr")

    print("Assigned aliases to all input tables")

    # ---------------------------------------------------------------------
    # Join sequence as per SQL:
    # INNER JOINs: ev–acc, acc–clm
    # LEFT JOINs:  pol, meth, rsn, typ, sts, usr, indv
    # ---------------------------------------------------------------------
    joined = (
        ev
        .join(
            acc,
            (col("acc.oid_clsno") == col("ev.myAccident_clsno")) &
            (col("acc.oid_instid") == col("ev.myAccident_instid")),
            "inner"
        )
        .join(
            clm,
            (col("clm.myAccident_clsno") == col("acc.oid_clsno")) &
            (col("clm.myAccident_instid") == col("acc.oid_instid")),
            "inner"
        )
        .join(
            pol,
            (col("pol.oid_clsno") == col("clm.myPolicy_clsno")) &
            (col("pol.oid_instid") == col("clm.myPolicy_instid")),
            "left"
        )
        .join(
            meth,
            (col("meth.oid_clsno") == col("ev.myContactEventMethod_clsno")) &
            (col("meth.oid_instid") == col("ev.myContactEventMethod_instid")),
            "left"
        )
        .join(
            rsn,
            (col("rsn.oid_clsno") == col("ev.myContactEventReason_clsno")) &
            (col("rsn.oid_instid") == col("ev.myContactEventReason_instid")),
            "left"
        )
        .join(
            typ,
            (col("typ.oid_clsno") == col("ev.myContactEventType_clsno")) &
            (col("typ.oid_instid") == col("ev.myContactEventType_instid")),
            "left"
        )
        .join(
            sts,
            (col("sts.oid_clsno") == col("ev.myContactEventStatus_clsno")) &
            (col("sts.oid_instid") == col("ev.myContactEventStatus_instid")),
            "left"
        )
        .join(
            usr,
            (col("usr.oid_clsno") == col("ev.myUser_clsno")) &
            (col("usr.oid_instid") == col("ev.myUser_instid")),
            "left"
        )
        .join(
            indv,
            (col("indv.oid_clsno") == col("ev.myContactToFrom_clsno")) &
            (col("indv.oid_instid") == col("ev.myContactToFrom_instid")),
            "left"
        )
    )

    print("Performed joins successfully")

    # ---------------------------------------------------------------------
    # Final projection: match SQL SELECT fields
    # ---------------------------------------------------------------------
    final = joined.select(
        col("ev.aUniqueID").alias("conev_id"),
        col("clm.aClaimNumber").alias("clm_no"),
        col("acc.aUniqueId").alias("acc_no"),
        col("pol.aPolicyNumber").alias("pol_no"),
        col("usr.aUserID").alias("user_id"),
        col("ev.aDateTime").alias("conev_acc_comm_dt"),
        F.lit(None).alias("conev_acc_comm_frm"),       # Refer to Confluence for logic
        F.lit(None).alias("conev_acc_comms_to"),       # Refer to Confluence for logic
        col("ev.aHeader").alias("conev_acc_comm_subj"),
        F.lit(None).alias("conev_acc_comm_doc_type"),  # Refer to Confluence for logic
        col("meth.aDescription").alias("conev_acc_comm_meth"),
        col("rsn.aDescription").alias("conev_acc_comm_reason"),
        col("sts.aDescription").alias("conev_acc_comm_status"),
        col("typ.aDescription").alias("conev_acc_comm_type"),
        F.lit(None).alias("conev_acc_comm_dist_not_verfd"),  # TBC
        F.lit(None).alias("conev_acc_comm_frm_user_team")    # TBC
    )

    print("Projection complete — returning final DataFrame")

    return final
