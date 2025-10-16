WITH rank_unique_TIMWflProcess AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMWflProcess
    FROM dl_TIMWflProcess
),
unique_TIMWflProcess AS (
	SELECT *
	FROM rank_unique_TIMWflProcess
	WHERE rank_unique_TIMWflProcess = 1 AND _operation <> 'D'
),
rank_unique_TIMPolicyClaim AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMPolicyClaim
    FROM dl_TIMPolicyClaim
),
rank_unique_TIMWflActivity AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMWflActivity
    FROM dl_TIMWflActivity
),
rank_unique_TIMContactTypeUser AS (
    SELECT 
		aKey,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactTypeUser,
        _operation,
        oid_clsno,
        oid_instid
    FROM dl_TIMContactTypeUser
),
rank_unique_CONWflForwardHistoryUpdate AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_CONWflForwardHistoryUpdate
    FROM dl_CONWflForwardHistoryUpdate
),
rank_unique_CONWflReferralChangeReason AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_CONWflReferralChangeReason
    FROM dl_CONWflReferralChangeReason
),
rank_unique_CONWflStateHistoryUpdate AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY  _timestamp DESC) AS rank_unique_CONWflStateHistoryUpdate
    FROM dl_CONWflStateHistoryUpdate
),
rank_unique_CONWflProcessState AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_CONWflProcessState
    FROM dl_CONWflProcessState
),
rank_unique_CONWflStateChangeReason AS (
    SELECT *,
        RANK() OVER(PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_CONWflStateChangeReason
    FROM dl_CONWflStateChangeReason
)
SELECT
	rank_unique_TIMWflActivity.aUniqueID AS wrkflw_no,
	rank_unique_TIMPolicyClaim.aClaimNumber AS clm_no,
	rank_unique_CONWflForwardHistoryUpdate.oid_instid AS hist_id,	
	'Activity History' AS wrkflw_hist_type,
	rank_unique_CONWflForwardHistoryUpdate._timestamp AS wrkflw_hist_time_dt,
	user_forward_history.aKey AS wrkflw_hist_user,
	rank_unique_CONWflForwardHistoryUpdate.aPrevReferStatus AS wrkflw_hist_frm_stat,
	rank_unique_CONWflForwardHistoryUpdate.aNewReferStatus AS wrkflw_hist_to_stat,
	rank_unique_CONWflReferralChangeReason.aName AS wrkflw_hist_rs
FROM unique_TIMWflProcess
INNER JOIN rank_unique_TIMPolicyClaim
	ON rank_unique_TIMPolicyClaim.oid_clsno = unique_TIMWflProcess.myClaim_clsno
	AND rank_unique_TIMPolicyClaim.oid_instid = unique_TIMWflProcess.myClaim_instid
	AND rank_unique_TIMPolicyClaim = 1
	AND rank_unique_TIMPolicyClaim._operation <> 'D'
INNER JOIN  rank_unique_TIMWflActivity
	ON  rank_unique_TIMWflActivity.myWflProcess_clsno = unique_TIMWflProcess.oid_clsno
	AND rank_unique_TIMWflActivity.myWflProcess_instid = unique_TIMWflProcess.oid_instid
	AND rank_unique_TIMWflActivity = 1
	AND rank_unique_TIMWflActivity._operation <> 'D'
INNER JOIN  rank_unique_CONWflForwardHistoryUpdate
	ON  rank_unique_CONWflForwardHistoryUpdate.myHistoriedComponent_clsno = rank_unique_TIMWflActivity.oid_clsno
	AND rank_unique_CONWflForwardHistoryUpdate.myHistoriedComponent_instid = rank_unique_TIMWflActivity.oid_instid
	AND rank_unique_CONWflForwardHistoryUpdate = 1
	AND rank_unique_CONWflForwardHistoryUpdate._operation <> 'D'
LEFT JOIN  rank_unique_TIMContactTypeUser AS user_forward_history
	ON  user_forward_history.oid_clsno = rank_unique_CONWflForwardHistoryUpdate.myCreatedBy_clsno
	AND user_forward_history.oid_instid = rank_unique_CONWflForwardHistoryUpdate.myCreatedBy_instid
	AND user_forward_history.rank_unique_TIMContactTypeUser = 1
	AND user_forward_history._operation <> 'D'
LEFT JOIN  rank_unique_CONWflReferralChangeReason
	ON  rank_unique_CONWflReferralChangeReason.oid_clsno = rank_unique_CONWflForwardHistoryUpdate.myChangeReason_clsno
	AND rank_unique_CONWflReferralChangeReason.oid_instid = rank_unique_CONWflForwardHistoryUpdate.myChangeReason_instid
	AND rank_unique_CONWflReferralChangeReason = 1
	AND rank_unique_CONWflReferralChangeReason._operation <> 'D'
UNION ALL
SELECT
	unique_TIMWflProcess.aUniqueID AS wrkflw_no,
	rank_unique_TIMPolicyClaim.aClaimNumber AS clm_no,
	rank_unique_CONWflStateHistoryUpdate.oid_instid AS hist_id,
	'Process History' AS wrkflw_hist_type,
	rank_unique_CONWflStateHistoryUpdate.aLastModified AS wrkflw_hist_time_dt,
	user_state_history.aKey AS wrkflw_hist_user,
	ProcessState_from.aName AS wrkflw_hist_frm_stat,
	ProcessState_to.aName AS wrkflw_hist_to_stat,
	rank_unique_CONWflStateChangeReason.aName AS wrkflw_hist_rs
FROM unique_TIMWflProcess
INNER JOIN rank_unique_TIMPolicyClaim
	ON rank_unique_TIMPolicyClaim.oid_clsno = unique_TIMWflProcess.myClaim_clsno
	AND rank_unique_TIMPolicyClaim.oid_instid = unique_TIMWflProcess.myClaim_instid
	AND rank_unique_TIMPolicyClaim = 1
	AND rank_unique_TIMPolicyClaim._operation <> 'D'
INNER JOIN  rank_unique_CONWflStateHistoryUpdate
	ON  rank_unique_CONWflStateHistoryUpdate.myHistoriedComponent_clsno = unique_TIMWflProcess.oid_clsno
	AND rank_unique_CONWflStateHistoryUpdate.myHistoriedComponent_instid = unique_TIMWflProcess.oid_instid
	AND rank_unique_CONWflStateHistoryUpdate = 1
	AND rank_unique_CONWflStateHistoryUpdate._operation <> 'D'
LEFT JOIN  rank_unique_TIMContactTypeUser AS user_state_history
	ON  user_state_history.oid_clsno = rank_unique_CONWflStateHistoryUpdate.myCreatedBy_clsno
	AND user_state_history.oid_instid = rank_unique_CONWflStateHistoryUpdate.myCreatedBy_instid
	AND user_state_history.rank_unique_TIMContactTypeUser = 1
	AND user_state_history._operation <> 'D'
LEFT JOIN  rank_unique_CONWflProcessState AS ProcessState_from
	ON  ProcessState_from.oid_clsno = rank_unique_CONWflStateHistoryUpdate.myPreviousState_clsno
	AND ProcessState_from.oid_instid = rank_unique_CONWflStateHistoryUpdate.myPreviousState_instid
	AND ProcessState_from.rank_unique_CONWflProcessState = 1
	AND ProcessState_from._operation <> 'D'
LEFT JOIN  rank_unique_CONWflProcessState AS ProcessState_to
	ON  ProcessState_to.oid_clsno = rank_unique_CONWflStateHistoryUpdate.myNewState_clsno
	AND ProcessState_to.oid_instid = rank_unique_CONWflStateHistoryUpdate.myNewState_instid
	AND ProcessState_to.rank_unique_CONWflProcessState = 1
	AND ProcessState_to._operation <> 'D'
LEFT JOIN  rank_unique_CONWflStateChangeReason
	ON  rank_unique_CONWflStateChangeReason.oid_clsno = rank_unique_CONWflStateHistoryUpdate.myChangeReason_clsno
	AND rank_unique_CONWflStateChangeReason.oid_instid = rank_unique_CONWflStateHistoryUpdate.myChangeReason_instid
	AND rank_unique_CONWflStateChangeReason = 1
	AND rank_unique_CONWflStateChangeReason._operation <> 'D'
