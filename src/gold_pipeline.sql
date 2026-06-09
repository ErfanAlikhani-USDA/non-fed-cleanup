-- ============================================================
-- GOLD PIPELINE: Non-Fed Master Status Materialized View
-- ============================================================
-- Lakeflow Spark Declarative Pipeline (SDP)
-- Automatically refreshes when upstream silver tables update.
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ree_edapt.hr.nonfed_master_status_vw
AS

WITH

-- SECTION 1: Standardize EmpowHR (Contractor Table)
empowhr_clean AS (
  SELECT
    CAST(ID AS INT)                   AS emplid,
    `Account_#`,
    Contract_Status,
    Begin_Date,
    Expire_Date,
    Justify,
    Description                       AS empowhr_description,
    CASE
      WHEN Contract_Status = 'A' AND Expire_Date >= CURRENT_DATE() THEN 'Active'
      WHEN Contract_Status = 'A' AND Expire_Date < CURRENT_DATE()  THEN 'Inactive'
      ELSE 'Inactive'
    END AS EmpowHR_Status,
    COUNT(*) OVER (PARTITION BY CAST(ID AS INT)) AS emplid_count,
    CASE WHEN COUNT(*) OVER (PARTITION BY CAST(ID AS INT)) > 1 THEN 1 ELSE 0 END AS is_duplicate_emplid
  FROM ree_edapt.hr_silver.ree_empowhr_contractor
),

-- SECTION 2: Standardize USAccess (PIV Card Data)
usaccess_clean AS (
  SELECT
    CAST(Employee_ID AS INT)           AS emplid,
    Last_Name,
    First_Name,
    Middle_Name,
    Employment_Status,
    Org_Association_Category,
    Current_Issuance_Status,
    UPN,
    mission_area,
    CASE
      WHEN Current_Issuance_Status = 'TERMINATED'  THEN 'Terminated'
      WHEN Current_Issuance_Status = 'SUSPENDED'   THEN 'Suspended'
      WHEN Current_Issuance_Status IN ('ACTIVE','ISSUANCE REQUEST PENDING',
           'CARD PRINTING IN PROCESS','CARD DELIVERED',
           'CREDENTIAL IN TRANSIT','PRINTING COMPLETE') THEN 'Active'
      ELSE 'Unknown'
    END AS PIV_Status
  FROM ree_edapt.hr_silver.ree_usaccess_applicant_status
),

-- SECTION 3: Standardize Active Directory
ad_clean AS (
  SELECT
    CAST(CAST(employeeid AS INT) AS INT) AS emplid,
    displayname,
    samaccountname,
    userprincipalname,
    description                       AS ad_description,
    lastlogon,
    accountlastmodified,
    CASE
      WHEN accountdisabled = 'Disabled' THEN 'Disabled'
      ELSE 'Active'
    END AS AD_Account_Status
  FROM ree_edapt.ree.ree_active_directory
  WHERE employeeid IS NOT NULL
    AND employeeid != ''
    AND employeeid != '0.0'
),

-- SECTION 4: Outer join all three sources
master_join AS (
  SELECT
    e.emplid,
    e.`Account_#`,
    e.Contract_Status,
    e.Begin_Date,
    e.Expire_Date,
    e.Justify,
    e.empowhr_description,
    e.EmpowHR_Status,
    e.emplid_count          AS empowhr_duplicate_count,
    e.is_duplicate_emplid,
    u.Last_Name,
    u.First_Name,
    u.Org_Association_Category,
    u.Employment_Status,
    u.Current_Issuance_Status,
    u.PIV_Status,
    u.UPN,
    u.mission_area,
    a.displayname,
    a.samaccountname,
    a.ad_description,
    a.lastlogon,
    a.accountlastmodified,
    a.AD_Account_Status,
    CASE WHEN u.emplid IS NULL THEN 1 ELSE 0 END AS missing_usaccess,
    CASE WHEN a.emplid IS NULL THEN 1 ELSE 0 END AS missing_ad
  FROM empowhr_clean e
  LEFT JOIN usaccess_clean u ON e.emplid = u.emplid
  LEFT JOIN ad_clean a       ON e.emplid = a.emplid
),

-- SECTION 5: Status classification
master_classified AS (
  SELECT
    *,
    CASE
      WHEN EmpowHR_Status = 'Active'
       AND AD_Account_Status = 'Active'
       AND PIV_Status NOT IN ('Suspended', 'Terminated')
       AND PIV_Status IS NOT NULL
        THEN 'Active'
      WHEN EmpowHR_Status = 'Active'
       AND PIV_Status = 'Terminated'
        THEN 'Likely Inactive'
      WHEN EmpowHR_Status = 'Active'
       AND AD_Account_Status = 'Disabled'
       AND (ad_description NOT LIKE '%coming back%'
            AND ad_description NOT LIKE '%LWOP%')
        THEN 'Inactive Account'
      WHEN EmpowHR_Status = 'Active'
       AND missing_usaccess = 1
       AND missing_ad = 1
        THEN 'Inactive'
      ELSE 'Needs Review'
    END AS Overall_Status,
    CASE
      WHEN missing_usaccess = 1 AND missing_ad = 0 AND AD_Account_Status = 'Active'
        THEN 'Active in AD, no PIV record'
      WHEN PIV_Status = 'Suspended' AND AD_Account_Status = 'Active'
        THEN 'PIV suspended, AD active'
      WHEN EmpowHR_Status = 'Active'
       AND Expire_Date < CURRENT_DATE()
       AND AD_Account_Status = 'Active'
        THEN 'Contract expired but still active in AD'
      WHEN missing_usaccess = 1 AND missing_ad = 1
        THEN 'No USAccess or AD record found'
      WHEN EmpowHR_Status = 'Inactive'
        THEN 'PoP expired - contract inactive'
      WHEN ad_description LIKE '%LWOP%'
        THEN 'LWOP noted in AD description'
      WHEN ad_description LIKE '%coming back%'
        THEN 'Coming back noted in AD description'
      WHEN PIV_Status = 'Unknown'
        THEN 'Unusual PIV issuance status'
      WHEN is_duplicate_emplid = 1
        THEN 'Duplicate emplid in EmpowHR (' || empowhr_duplicate_count || ' records)'
      ELSE ''
    END AS Review_Notes
  FROM master_join
)

SELECT
  emplid,
  Last_Name,
  First_Name,
  `Account_#`,
  Org_Association_Category,
  mission_area,
  empowhr_description,
  Begin_Date,
  Expire_Date,
  lastlogon            AS ad_last_logon,
  accountlastmodified  AS ad_last_modified,
  EmpowHR_Status,
  PIV_Status,
  AD_Account_Status,
  Overall_Status,
  Review_Notes,
  Contract_Status,
  Current_Issuance_Status,
  ad_description,
  UPN,
  samaccountname,
  missing_usaccess,
  missing_ad,
  is_duplicate_emplid,
  empowhr_duplicate_count
FROM master_classified;
