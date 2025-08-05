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

sha2(concat_ws('||',
                     trtmt_no, clm_no, trtmt_no_of_sess_apprvd, trtmt_no_of_sess_reqstd,
                     trtmt_care_completed, trtmt_care_respnse, trtmt_care_servc_provider,
                     trtmt_care_servc_type, trtmt_care_advised_dt, trtmt_care_reqstd_dt,
                     trtmt_manager, trtmt_first_plan_intiated_by),256) AS rec_sha




-- STEP 1: Get the latest version per granularity key
WITH history_data AS (
    SELECT
        trtmt_no,
        clm_no,
        rec_sha             AS src_rec_sha,
        record_version_no   AS record_version_no,
        ROW_NUMBER() OVER (
            PARTITION BY trtmt_no, clm_no
            ORDER BY active_dt DESC, record_version_no DESC
        ) AS rnk
    FROM hub_treatement
    WHERE source_system_cd = '${source}'
      AND active_dt <= '${active_dt}'
),

-- keep only the latest version per key
latest_history AS (
    SELECT
        trtmt_no      AS src_trtmt_no,
        clm_no        AS src_clm_no,
        src_rec_sha,
        record_version_no
    FROM history_data
    WHERE rnk = 1
),

-- STEP 2: compute SHA for the incoming (final_load) data
src AS (
    SELECT
        trtmt_no,
        clm_no,
        trtmt_no_of_sess_apprvd,
        trtmt_no_of_sess_reqstd,
        trtmt_care_completed,
        trtmt_care_respnse,
        trtmt_care_servc_provider,
        trtmt_care_servc_type,
        trtmt_care_advised_dt,
        trtmt_care_reqstd_dt,
        trtmt_manager,
        trtmt_first_plan_intiated_by,
        sha2(concat_ws('||',
                     trtmt_no, clm_no,
                     trtmt_no_of_sess_apprvd, trtmt_no_of_sess_reqstd,
                     trtmt_care_completed, trtmt_care_respnse,
                     trtmt_care_servc_provider, trtmt_care_servc_type,
                     trtmt_care_advised_dt, trtmt_care_reqstd_dt,
                     trtmt_manager, trtmt_first_plan_intiated_by),256) AS rec_sha
    FROM (<< your big SELECT >>) as final_data
),

-- STEP 3: anti-join to only include records whose SHA doesn't already exist
scd_data AS (
    SELECT s.*
    FROM src s
    LEFT JOIN latest_history h
      ON s.rec_sha = h.src_rec_sha
    WHERE h.src_rec_sha IS NULL
)

-- STEP 4: use left join on business keys to get latest version_no and bump it
INSERT INTO hub_treatement
SELECT
    n.trtmt_no,
    n.clm_no,
    n.trtmt_no_of_sess_apprvd,
    n.trtmt_no_of_sess_reqstd,
    n.trtmt_care_completed,
    n.trtmt_care_respnse,
    n.trtmt_care_servc_provider,
    n.trtmt_care_servc_type,
    n.trtmt_care_advised_dt,
    n.trtmt_care_reqstd_dt,
    n.trtmt_manager,
    n.trtmt_first_plan_intiated_by,
    n.rec_sha,
    current_date()                                                        AS active_dt,
    cast(from_unixtime(unix_timestamp(current_date()), 'yyyyMM') AS INT)  AS active_ym,
    now()                                                                 AS load_dt,
    '${source}'                                                           AS source_system_cd,
    COALESCE(h.record_version_no,0) + 1                                    AS record_version_no
FROM scd_data n
LEFT JOIN latest_history h
  ON n.trtmt_no = h.src_trtmt_no AND n.clm_no = h.src_clm_no;

