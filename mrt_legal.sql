With rank_unique_ClaimContactType AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        _timestamp,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ClaimContactType,
        aDateEngaged,
        myClaim_clsno,
        myClaim_instid,
        myContactTypeSubCategory_clsno,
        myContactTypeSubCategory_instid,
        myContactType_clsno,
        myContactType_instid
    From
        dl_TIMClaimContactType
),
unique_ClaimContactType AS
(
    Select
        *
    from
        rank_unique_ClaimContactType
    Where
        rank_unique_ClaimContactType = 1 AND _operation <> 'D'
),
rank_unique_PolicyClaim AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_PolicyClaim,
        aClaimNumber,
        aDateInitialConsultSolicitor,
        aHasLegalRepresentation,
        aLitDefendantCounsel,
        aLitPlaintiffCounsel,
        myLitReferralType_clsno,
        myLitReferralType_instid
    From
        dl_TIMPolicyClaim
),
rank_unique_LitigationReferralType AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_LitigationReferralType,
        aDescription
    From
        dl_TIMLitigationReferralType
),
rank_unique_ContactTypeSubcategory AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ContactTypeSubcategory,
        aCode,
        aDescription
    From
        dl_TIMContactTypeSubcategory
),
rank_unique_ContactTypeClaimLink AS
(
    Select
        oid_clsno,
        oid_instid,
        _operation,
        row_number() over (partition by oid_clsno, oid_instid order by _timestamp desc) AS rank_unique_ContactTypeClaimLink,
        aKey,
        mycontact_clsno,
        mycontact_instid
    From
        dl_TIMContactTypeClaimLink-- Where oid_instid in (select distinct myContactType_instid from dl_TIMClaimContactType where myContactTypeSubCategory_instid in(select oid_instid from dl_TIMContactTypeSubcategory Where aCode = 1))
),
hub_pimsctp_legal AS
(
    Select
        aClaimNumber AS clm_no,
        aDateInitialConsultSolicitor AS legl_consultatn_dt,
        aDateEngaged AS legl_rep_date,
        aCode AS legl_aCode,
        rank_unique_ContactTypeSubcategory.aDescription AS legl_subCategory,
        'ClaimLink - ' + rank_unique_ContactTypeSubcategory.aDescription AS legl_contact_type,
        aKey AS legl_Contact_Id,
        aLitDefendantCounsel AS legl_defendnt_counsel,
        aLitPlaintiffCounsel AS legl_plntif_counsel,
        rank_unique_LitigationReferralType.aDescription AS legl_defendnt_refrl_type,
        row_number() over (partition by myClaim_clsno, myClaim_instid, myContactTypeSubCategory_clsno,myContactTypeSubCategory_instid
            Order by aDateEngaged Desc, _timestamp Desc, unique_ClaimContactType.oid_instid Desc) AS legl_rank_latest,
        row_number() over (partition by myClaim_clsno, myClaim_instid, myContactTypeSubCategory_clsno,myContactTypeSubCategory_instid
            Order by aDateEngaged ASC, _timestamp ASC, unique_ClaimContactType.oid_instid ASC) AS legl_rank_first,
        null AS active_dt
    From
        unique_ClaimContactType
    Inner Join rank_unique_PolicyClaim
    ON(myClaim_clsno = rank_unique_PolicyClaim.oid_clsno
        AND myClaim_instid = rank_unique_PolicyClaim.oid_instid
        AND rank_unique_PolicyClaim._Operation <> 'D'
        AND rank_unique_PolicyClaim = 1)
    Left Join rank_unique_LitigationReferralType
        ON(myLitReferralType_clsno = rank_unique_LitigationReferralType.oid_clsno
        AND myLitReferralType_instid = rank_unique_LitigationReferralType.oid_instid
        AND rank_unique_LitigationReferralType._Operation <> 'D'
        AND rank_unique_LitigationReferralType = 1)
    Inner Join rank_unique_ContactTypeSubcategory
        ON(myContactTypeSubCategory_clsno = rank_unique_ContactTypeSubcategory.oid_clsno
        AND myContactTypeSubCategory_instid = rank_unique_ContactTypeSubcategory.oid_instid
        AND rank_unique_ContactTypeSubcategory._Operation <> 'D'
        AND rank_unique_ContactTypeSubcategory = 1)
    Left Join rank_unique_ContactTypeClaimLink
        ON(myContactType_clsno = rank_unique_ContactTypeClaimLink.oid_clsno
        AND myContactType_instid = rank_unique_ContactTypeClaimLink.oid_instid
        AND rank_unique_ContactTypeClaimLink._Operation <> 'D'
        AND rank_unique_ContactTypeClaimLink = 1)
),
hub_pimsctp_claims AS
(
    Select
        null AS clm_no,
        null AS active_dt,
        null AS clm_rpted_dt
),
hub_pimsctp_contact AS
(
    Select
        null AS con_id,
        null AS con_classification,
        null AS con_type,
        null AS con_title,
        null AS con_first_nm,
        null AS con_surname,
        null AS con_org_nm,
        null AS con_home_email,
        null AS con_home_ph_area_cd,
        null AS con_home_ph_no,
        null AS con_abn,
        null AS con_firm,
        null AS con_phy_addr_PostCode,
        null AS con_post_addr_PostCode,
        null AS con_brch,
        null AS con_phy_addr_line_1,
        null AS con_phy_addr_line_2,
        null AS con_phy_addr_Name,
        null AS con_phy_addr_propertyName,
        null AS con_phy_addr_unitNumber,
        null AS con_phy_addr_LevelNo,
        null AS con_phy_addr_StreetNumber,
        null AS con_phy_addr_StreetName,
        null AS con_phy_addr_StreetType,
        null AS con_phy_addr_SuburbName,
        null AS con_phy_addr_StateName,
        null AS con_phy_addr_StateCode,
        null AS active_dt
),
--Please use the query from here
rank_unique_legal AS
(
    Select
        *,
        row_number() over(partition by clm_no, legl_aCode, legl_rank_first Order by active_dt Desc) AS rank_unique_legal,
        dense_rank() over(partition by clm_no, legl_aCode order by legl_rank_first desc) AS legl_rank_latest_latest
    From hub_pimsctp_legal
    Where legl_aCode IN('01','02')
),
rank_unique_claims AS
(
    Select
        *,
        row_number() over(partition by clm_no Order by active_dt Desc) AS rank_unique_claims
    From hub_pimsctp_claims
),
rank_unique_contact AS
(
    Select
        *,
        row_number() over(partition by con_id,con_type Order by active_dt Desc) AS rank_unique_contact
    From hub_pimsctp_contact
    Where con_type = 'ClaimLink'
),
legal_plaintaiff_change AS
(
    Select
        clm_no
    From rank_unique_legal
    Where legl_aCode = '02' and rank_unique_legal = 1
    Group by clm_no
    Having count(*) > 1
),
legal_plaintaiff_current_month AS
(
    Select Distinct
        clm_no
    From rank_unique_legal
    Where legl_aCode = '02' and rank_unique_legal = 1
    AND legl_rep_date >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
    AND legl_rep_date <= GETDATE()
),
legal_plaintaiff_change_current_month AS
(
    Select-- Distinct
        pc.clm_no
    From legal_plaintaiff_change pc
    Inner Join legal_plaintaiff_current_month pcm
    ON(pc.clm_no = pcm.clm_no)
),
unique_legal_first_and_latest AS
(
    Select
        clm_no,
        legl_consultatn_dt,
        legl_defendnt_counsel,
        legl_plntif_counsel,
        legl_defendnt_refrl_type,
        MAX(CASE WHEN legl_aCode = '01' AND legl_rank_latest_latest = 1 THEN legl_rep_date END) AS legl_defendnt_dt,
        MAX(CASE WHEN legl_aCode = '01' AND legl_rank_latest_latest = 1 THEN legl_Contact_Id END) AS legl_defendnt_Contact_Id,
        MAX(CASE WHEN legl_aCode = '02' AND legl_rank_latest_latest = 1 THEN legl_rep_date END) AS legl_plntif_reprstd_dt,
        MAX(CASE WHEN legl_aCode = '02' AND legl_rank_latest_latest = 1 THEN legl_Contact_Id END) AS legl_plntif_Contact_Id,
        MAX(CASE WHEN legl_aCode = '01' AND legl_rank_first = 1 THEN legl_rep_date END) AS legl_first_defendnt_rep_dt,
        MAX(CASE WHEN legl_aCode = '02' AND legl_rank_first = 1 THEN legl_rep_date END) AS legl_first_plntif_reprstd_dt,
        CASE WHEN SUM(CASE WHEN legl_aCode = '01' THEN 1 ELSE 0 END) > 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS legl_defendnt_rep_flg,
        CASE WHEN SUM(CASE WHEN legl_aCode = '02' THEN 1 ELSE 0 END) > 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS legl_plntif_reprstd_flg
    from rank_unique_legal Where rank_unique_legal = 1
    Group by clm_no, legl_consultatn_dt, legl_defendnt_counsel, legl_plntif_counsel, legl_defendnt_refrl_type
)
Select
    unique_legal_first_and_latest.clm_no,
    legl_consultatn_dt,
    legl_defendnt_counsel,
    legl_plntif_counsel,
    legl_defendnt_refrl_type,
    legl_defendnt_rep_flg,
    legl_defendnt_dt,
    legl_defendnt_Contact_Id,--We can ignore this into mart table
    legl_plntif_reprstd_flg,
    legl_plntif_reprstd_dt,
    legl_plntif_Contact_Id,--We can ignore this into mart table
    legl_first_defendnt_rep_dt,
    legl_first_plntif_reprstd_dt,
    CASE WHEN pccm.clm_no IS NOT NULL THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS legl_plntif_swtchd_mtd_ind,
    CASE WHEN legl_first_plntif_reprstd_dt <= clm_rpted_dt THEN CAST(1 AS BIT) ELSE CAST(1 AS BIT) END as legl_plntif_rep_at_lodgmnt_ind,
    plain_contact.con_first_nm AS con_plntif_legl_reprsntatve_first_nm,
    plain_contact.con_surname AS con_plntif_legl_reprsntatve_surname,
    plain_contact.con_home_email AS con_plntif_legl_email_addr,
    concat(plain_contact.con_home_ph_area_cd, plain_contact.con_home_ph_no) AS con_plntif_legl_ph_no,
    def_contact.con_abn AS legl_defendnt_abn,
    def_contact.con_firm AS legl_defendnt_firm,
    def_contact.con_phy_addr_PostCode AS con_defendnt_con_postcd,
    plain_contact.con_abn AS con_plntif_legl_abn,
    plain_contact.con_firm AS con_plntif_legl_firm,
    plain_contact.con_phy_addr_PostCode AS con_plntif_legl_phy_postcd,
    plain_contact.con_post_addr_PostCode AS con_plntif_legl_postcd,
    CASE WHEN plain_contact.con_classification = 'Individual' THEN concat(plain_contact.con_title, plain_contact.con_first_nm, plain_contact.con_surname)
        WHEN plain_contact.con_classification = 'Organisation' THEN plain_contact.con_org_nm END AS con_plntif_legl_reprsntatve_nm,
    plain_contact.con_brch AS con_plntif_legl_brch,
    concat(
        coalesce(plain_contact.con_phy_addr_line_1 + ',',''),
        coalesce(plain_contact.con_phy_addr_line_2 + ',',''),
        coalesce(plain_contact.con_phy_addr_Name + ',',''),
        coalesce(plain_contact.con_phy_addr_propertyName + ',',''),
        coalesce(plain_contact.con_phy_addr_unitNumber + ',',''),
        coalesce(plain_contact.con_phy_addr_LevelNo + ',',''),
        coalesce(plain_contact.con_phy_addr_StreetNumber + ',',''),
        coalesce(plain_contact.con_phy_addr_StreetName + ',',''),
        coalesce(plain_contact.con_phy_addr_StreetType + ',',''),
        coalesce(plain_contact.con_phy_addr_SuburbName + ',',''),
        coalesce(plain_contact.con_phy_addr_StateName + ',',''),
        coalesce(plain_contact.con_phy_addr_StateCode,'')) AS con_plntif_legl_phy_addr
From unique_legal_first_and_latest
Left Join legal_plaintaiff_change_current_month pccm
    ON(unique_legal_first_and_latest.clm_no = pccm.clm_no)
Left Join rank_unique_claims--Please note this can be used as Inner Join, we have taken as Left due to above dummy data
    ON(unique_legal_first_and_latest.clm_no = rank_unique_claims.clm_no
    AND rank_unique_claims = 1)
Left Join rank_unique_contact def_contact--this is dummy data
    ON(legl_defendnt_Contact_Id = def_contact.con_id
    AND def_contact.rank_unique_contact = 1)
Left Join rank_unique_contact plain_contact--this is dummy data
    ON(legl_defendnt_Contact_Id = plain_contact.con_id
    AND plain_contact.rank_unique_contact = 1)
