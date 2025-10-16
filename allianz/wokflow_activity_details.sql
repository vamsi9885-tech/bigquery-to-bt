WITH rank_unique_TIMContactEvent AS (
    SELECT * FROM (SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rnk,
        aUniqueID,
		aDateTime,
		aHeader,
        myAccident_clsno,
		myAccident_instid,
		myContactEventMethod_clsno,
		myContactEventMethod_instid,
		myContactEventReason_clsno,
		myContactEventReason_instid,
		myContactEventType_clsno,
		myContactEventType_instid,
		myContactEventStatus_clsno,
		myContactEventStatus_instid,
		myUser_clsno,
		myUser_instid,
		myContactToFrom_clsno,
		myContactToFrom_instid
    FROM dl_TIMContactEvent) t
    WHERE t.rnk=1
    AND t._operation <> 'D'
),
rank_unique_TIMPolicyClaim AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMPolicyClaim,
        aClaimNumber,
        myAccident_clsno,
		myAccident_instid,
		myPolicy_clsno,
		myPolicy_instid
    from dl_TIMPolicyClaim
),
rank_unique_TIMPolicy AS (
    SELECT 
		oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMPolicy,
        aPolicyNumber
    from dl_TIMPolicy
),
rank_unique_CTPAccident AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_CTPAccident,
        aUniqueId
    from dl_CTPAccident
),
rank_unique_TIMContactEventMethod AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactEventMethod,
        aDescription
    from dl_TIMContactEventMethod
),
rank_unique_TIMContactEventReason AS (
    SELECT 
		oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactEventReason,
        aDescription
    from dl_TIMContactEventReason
),
rank_unique_TIMContactEventType AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactEventType,
        aDescription
    from dl_TIMContactEventType
),
rank_unique_TIMContactEventStatus AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactEventStatus,
        aDescription
    from dl_TIMContactEventStatus
),
rank_unique_TIMContactIndividual AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactIndividual,
        aUniqueID
    from dl_TIMContactIndividual
),
rank_unique_TIMContactTypeUser AS (
    SELECT 
    	oid_clsno,
		oid_instid,
		_timestamp,
		_operation,
        ROW_NUMBER() OVER (PARTITION BY oid_clsno, oid_instid ORDER BY _timestamp DESC) AS rank_unique_TIMContactTypeUser,
        aUserID,
		aKey
    from dl_TIMContactTypeUser
)
SELECT
	rank_unique_TIMContactEvent.aUniqueID AS conev_id,
	rank_unique_TIMPolicyClaim.aClaimNumber AS clm_no, 
	rank_unique_CTPAccident.aUniqueId AS acc_no,
	rank_unique_TIMPolicy.aPolicyNumber AS pol_no,
	rank_unique_TIMContactTypeUser.aUserID AS user_id,
	rank_unique_TIMContactEvent.aDateTime AS conev_acc_comm_dt,
	NULL AS conev_acc_comm_frm, -- Refer to Confluence for detailed logic
	NULL AS conev_acc_comms_to, -- Refer to Confluence for detailed logic 
    rank_unique_TIMContactEvent.aHeader AS conev_acc_comm_subj,
	NULL AS conev_acc_comm_doc_type, -- Refer to Confluence for detailed logic
    rank_unique_TIMContactEventMethod.aDescription AS conev_acc_comm_meth,
	rank_unique_TIMContactEventReason.aDescription AS conev_acc_comm_reason,
	rank_unique_TIMContactEventStatus.aDescription AS conev_acc_comm_status,
	rank_unique_TIMContactEventType.aDescription AS conev_acc_comm_type,
	NULL AS conev_acc_comm_dist_not_verfd, -- TBC
	NULL AS conev_acc_comm_frm_user_team -- TBC
FROM rank_unique_TIMContactEvent
INNER JOIN rank_unique_CTPAccident 
	ON rank_unique_CTPAccident.oid_clsno = rank_unique_TIMContactEvent.myAccident_clsno
	AND rank_unique_CTPAccident.oid_instid = rank_unique_TIMContactEvent.myAccident_instid
	AND rank_unique_CTPAccident = 1
	AND rank_unique_CTPAccident._operation <> 'D'
INNER JOIN rank_unique_TIMPolicyClaim
	ON rank_unique_TIMPolicyClaim.myAccident_clsno = rank_unique_CTPAccident.oid_clsno
	AND rank_unique_TIMPolicyClaim.myAccident_instid = rank_unique_CTPAccident.oid_instid
	AND rank_unique_TIMPolicyClaim = 1
	AND rank_unique_TIMPolicyClaim._operation <> 'D'
LEFT JOIN rank_unique_TIMPolicy
	ON rank_unique_TIMPolicy.oid_clsno = rank_unique_TIMPolicyClaim.myPolicy_clsno
	AND rank_unique_TIMPolicy.oid_instid = rank_unique_TIMPolicyClaim.myPolicy_instid 
	AND rank_unique_TIMPolicy = 1
	AND rank_unique_TIMPolicy._operation <> 'D'
LEFT JOIN rank_unique_TIMContactEventMethod
	ON rank_unique_TIMContactEventMethod.oid_clsno = rank_unique_TIMContactEvent.myContactEventMethod_clsno 
	AND rank_unique_TIMContactEventMethod.oid_instid = rank_unique_TIMContactEvent.myContactEventMethod_instid 
	AND rank_unique_TIMContactEventMethod = 1
	AND rank_unique_TIMContactEventMethod._operation <> 'D'
LEFT JOIN rank_unique_TIMContactEventReason
	ON rank_unique_TIMContactEventReason.oid_clsno = rank_unique_TIMContactEvent.myContactEventReason_clsno 
	AND rank_unique_TIMContactEventReason.oid_instid = rank_unique_TIMContactEvent.myContactEventReason_instid 
	AND rank_unique_TIMContactEventReason = 1
	AND rank_unique_TIMContactEventReason._operation <> 'D'
LEFT JOIN rank_unique_TIMContactEventType
	ON rank_unique_TIMContactEventType.oid_clsno = rank_unique_TIMContactEvent.myContactEventType_clsno 
	AND rank_unique_TIMContactEventType.oid_instid = rank_unique_TIMContactEvent.myContactEventType_instid 
	AND rank_unique_TIMContactEventType = 1
	AND rank_unique_TIMContactEventType._operation <> 'D'
LEFT JOIN rank_unique_TIMContactEventStatus
	ON rank_unique_TIMContactEventStatus.oid_clsno = rank_unique_TIMContactEvent.myContactEventStatus_clsno 
	AND rank_unique_TIMContactEventStatus.oid_instid = rank_unique_TIMContactEvent.myContactEventStatus_instid 
	AND rank_unique_TIMContactEventStatus = 1
	AND rank_unique_TIMContactEventStatus._operation <> 'D'
LEFT JOIN rank_unique_TIMContactTypeUser
	ON rank_unique_TIMContactTypeUser.oid_clsno = rank_unique_TIMContactEvent.myUser_clsno 
	AND rank_unique_TIMContactTypeUser.oid_instid = rank_unique_TIMContactEvent.myUser_instid 
	AND rank_unique_TIMContactTypeUser = 1
	AND rank_unique_TIMContactTypeUser._operation <> 'D'
LEFT JOIN rank_unique_TIMContactIndividual
	ON rank_unique_TIMContactIndividual.oid_clsno = rank_unique_TIMContactEvent.myContactToFrom_clsno 
	AND rank_unique_TIMContactIndividual.oid_instid = rank_unique_TIMContactEvent.myContactToFrom_instid 
	AND rank_unique_TIMContactIndividual = 1
	AND rank_unique_TIMContactIndividual._operation <> 'D'

