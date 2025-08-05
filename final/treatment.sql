With rank_unique_TreatmentApplied AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_TreatmentApplied,
        aUniqueId,
        aSessionsApproved,
        aSessionsRequested,
        aCompleted,
        aResponse,
        aServiceProvider,
        myType_clsno,
        myType_instid,
        aDateAdvised,
        aDateRequested,
        myClaimPlanInvestigation_clsno,
        myClaimPlanInvestigation_instid
    From
        DL_TIMTreatmentApplied
),
unique_TreatmentApplied AS
(
    Select *
    From
        rank_unique_TreatmentApplied
    Where
        rank_unique_TreatmentApplied = 1 AND _operation <> 'D'
),
rank_unique_Treatment AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_Treatment,
        aDescription
    from dl_TIMTreatment
),
rank_unique_MedicalInvestigation AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_MedicalInvestigation,
        aDescription
    from DL_TIMMedicalInvestigation
),
rank_unique_ClaimPlanInvestigation AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ClaimPlanInvestigation,
        aRehabPlanInitiatedBy,
        aTreatmentManager,
        myClaim_clsno,
        myClaim_instid,
        myMedicalCertificate_clsno,
        myMedicalCertificate_instid,
        myExpectedTreatmentPeriod_clsno,
        myExpectedTreatmentPeriod_instid
    From
        dl_TIMClaimPlanInvestigation
),
rank_unique_PolicyClaim AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_PolicyClaim,
        aClaimNumber
    From
        dl_TIMPolicyClaim
),
rank_unique_MedicalCertificate AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_MedicalCertificate,
        myExpectedTreatmentPeriod_clsno,
        myExpectedTreatmentPeriod_instid
    From
        dl_TIMMedicalCertificate
),
rank_unique_ExpectedTreatmentperiod AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ExpectedTreatmentperiod,
        aCode,
        aDescription
    From
        dl_TIMExpectedTreatmentperiod
)
Select
    aUniqueId AS trtmt_no,
    aClaimNumber AS clm_no,
    aSessionsApproved AS trtmt_no_of_sess_apprvd,
    aSessionsRequested AS trtmt_no_of_sess_reqstd,
    aCompleted AS trtmt_care_completed,
    CASE aResponse
        WHEN 1 THEN 'A'
        WHEN 2 THEN 'P'
        WHEN 3 THEN 'D'
        ELSE ''
    END AS trtmt_care_respnse,
    aServiceProvider AS trtmt_care_servc_provider,
    COALESCE(rank_unique_Treatment.aDescription,rank_unique_MedicalInvestigation.aDescription) AS trtmt_care_servc_type,
    aDateAdvised AS trtmt_care_advised_dt,
    aDateRequested AS trtmt_care_reqstd_dt,
    aTreatmentManager AS trtmt_manager,
    aRehabPlanInitiatedBy AS trtmt_first_plan_intiated_by
From unique_TreatmentApplied
Left Join rank_unique_Treatment
    ON(myType_clsno = rank_unique_Treatment.oid_clsno
    AND myType_instid = rank_unique_Treatment.oid_instid
    AND rank_unique_Treatment = 1
    AND rank_unique_Treatment._operation <> 'D')
Left Join rank_unique_MedicalInvestigation
    ON(myType_clsno = rank_unique_MedicalInvestigation.oid_clsno
    AND myType_instid = rank_unique_MedicalInvestigation.oid_instid
    AND rank_unique_MedicalInvestigation = 1
    AND rank_unique_MedicalInvestigation._operation <> 'D')
Left Join rank_unique_ClaimPlanInvestigation
    ON(myClaimPlanInvestigation_clsno = rank_unique_ClaimPlanInvestigation.oid_clsno
    AND myClaimPlanInvestigation_instid = rank_unique_ClaimPlanInvestigation.oid_instid
    AND rank_unique_ClaimPlanInvestigation = 1
    AND rank_unique_ClaimPlanInvestigation._operation <> 'D')
Left Join rank_unique_PolicyClaim
    ON(myClaim_clsno = rank_unique_PolicyClaim.oid_clsno
    AND myClaim_instid = rank_unique_PolicyClaim.oid_instid
    AND rank_unique_PolicyClaim = 1
    AND rank_unique_PolicyClaim._operation <> 'D');
