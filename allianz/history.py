#!/bin/python
#####################################################
# File Name: workflow_history_no_dedup.py
# Type: PySpark
# Purpose: Build workflow history (activity + process)
#          assuming deduplication is already handled upstream
# Created: 2025-10-16
# Author: Allianz Data Engineering
#####################################################

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

def transform(ds: Dict[str, DataFrame]) -> DataFrame:
    """
    Generate Workflow History (Activity History + Process History)
    using dictionary-driven DataFrames without deduplication.
    """
    print("Starting workflow_history_no_dedup transformation...")

    required_tables = [
        "dl_TIMWflProcess",
        "dl_TIMPolicyClaim",
        "dl_TIMWflActivity",
        "dl_CONWflForwardHistoryUpdate",
        "dl_CONWflReferralChangeReason",
        "dl_CONWflStateHistoryUpdate",
        "dl_CONWflProcessState",
        "dl_CONWflStateChangeReason",
        "dl_TIMContactTypeUser"
    ]

    missing = [t for t in required_tables if t not in ds]
    if missing:
        raise ValueError(f"Missing required DataFrames: {missing}")

    # Aliases
    wfl_proc = ds["dl_TIMWflProcess"].alias("wfl_proc")
    claim = ds["dl_TIMPolicyClaim"].alias("claim")
    wfl_act = ds["dl_TIMWflActivity"].alias("wfl_act")
    fwd_hist = ds["dl_CONWflForwardHistoryUpdate"].alias("fwd_hist")
    ref_reason = ds["dl_CONWflReferralChangeReason"].alias("ref_reason")
    state_hist = ds["dl_CONWflStateHistoryUpdate"].alias("state_hist")
    proc_state = ds["dl_CONWflProcessState"].alias("proc_state")
    state_reason = ds["dl_CONWflStateChangeReason"].alias("state_reason")
    user_tbl = ds["dl_TIMContactTypeUser"].alias("user_tbl")

    print("Aliases assigned — performing joins for Activity History...")

    # --- ACTIVITY HISTORY ---
    activity_history = (
        wfl_proc
        .join(
            claim,
            (claim.oid_clsno == wfl_proc.myClaim_clsno)
            & (claim.oid_instid == wfl_proc.myClaim_instid),
            "inner"
        )
        .join(
            wfl_act,
            (wfl_act.myWflProcess_clsno == wfl_proc.oid_clsno)
            & (wfl_act.myWflProcess_instid == wfl_proc.oid_instid),
            "inner"
        )
        .join(
            fwd_hist,
            (fwd_hist.myHistoriedComponent_clsno == wfl_act.oid_clsno)
            & (fwd_hist.myHistoriedComponent_instid == wfl_act.oid_instid),
            "inner"
        )
        .join(
            user_tbl.alias("user_forward_history"),
            (col("user_forward_history.oid_clsno") == fwd_hist.myCreatedBy_clsno)
            & (col("user_forward_history.oid_instid") == fwd_hist.myCreatedBy_instid),
            "left"
        )
        .join(
            ref_reason,
            (ref_reason.oid_clsno == fwd_hist.myChangeReason_clsno)
            & (ref_reason.oid_instid == fwd_hist.myChangeReason_instid),
            "left"
        )
        .select(
            col("wfl_act.aUniqueID").alias("wrkflw_no"),
            col("claim.aClaimNumber").alias("clm_no"),
            col("fwd_hist.oid_instid").alias("hist_id"),
            lit("Activity History").alias("wrkflw_hist_type"),
            col("fwd_hist._timestamp").alias("wrkflw_hist_time_dt"),
            col("user_forward_history.aKey").alias("wrkflw_hist_user"),
            col("fwd_hist.aPrevReferStatus").alias("wrkflw_hist_frm_stat"),
            col("fwd_hist.aNewReferStatus").alias("wrkflw_hist_to_stat"),
            col("ref_reason.aName").alias("wrkflw_hist_rs")
        )
    )

    print("Activity History join complete.")

    # --- PROCESS HISTORY ---
    print("Performing joins for Process History...")

    process_history = (
        wfl_proc
        .join(
            claim,
            (claim.oid_clsno == wfl_proc.myClaim_clsno)
            & (claim.oid_instid == wfl_proc.myClaim_instid),
            "inner"
        )
        .join(
            state_hist,
            (state_hist.myHistoriedComponent_clsno == wfl_proc.oid_clsno)
            & (state_hist.myHistoriedComponent_instid == wfl_proc.oid_instid),
            "inner"
        )
        .join(
            user_tbl.alias("user_state_history"),
            (col("user_state_history.oid_clsno") == state_hist.myCreatedBy_clsno)
            & (col("user_state_history.oid_instid") == state_hist.myCreatedBy_instid),
            "left"
        )
        .join(
            proc_state.alias("proc_state_from"),
            (col("proc_state_from.oid_clsno") == state_hist.myPreviousState_clsno)
            & (col("proc_state_from.oid_instid") == state_hist.myPreviousState_instid),
            "left"
        )
        .join(
            proc_state.alias("proc_state_to"),
            (col("proc_state_to.oid_clsno") == state_hist.myNewState_clsno)
            & (col("proc_state_to.oid_instid") == state_hist.myNewState_instid),
            "left"
        )
        .join(
            state_reason,
            (state_reason.oid_clsno == state_hist.myChangeReason_clsno)
            & (state_reason.oid_instid == state_hist.myChangeReason_instid),
            "left"
        )
        .select(
            col("wfl_proc.aUniqueID").alias("wrkflw_no"),
            col("claim.aClaimNumber").alias("clm_no"),
            col("state_hist.oid_instid").alias("hist_id"),
            lit("Process History").alias("wrkflw_hist_type"),
            col("state_hist.aLastModified").alias("wrkflw_hist_time_dt"),
            col("user_state_history.aKey").alias("wrkflw_hist_user"),
            col("proc_state_from.aName").alias("wrkflw_hist_frm_stat"),
            col("proc_state_to.aName").alias("wrkflw_hist_to_stat"),
            col("state_reason.aName").alias("wrkflw_hist_rs")
        )
    )

    print("Process History join complete.")

    # --- UNION BOTH HISTORIES ---
    final_df = activity_history.unionByName(process_history)

    print("Workflow History transformation completed successfully.")
    return final_df
