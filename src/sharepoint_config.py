# SharePoint Configuration
SP_DOMAIN = "usdagcc.sharepoint.com"
FOLDER_PREFIX = "/sites/REE-ARS-DAAFMDataTeam/HRD"
SP_FOLDER = "/Documents/Non-Fed Cleanup Files"
SP_FOLDER_FULL = FOLDER_PREFIX + SP_FOLDER
TENANT_ID = "ed5b36e7-01ee-4ebc-867e-e03cfa0d4697"

# Retry settings
MAX_RETRY = 3
RETRY_DELAY_SEC = 30

# Job output folder (downloaded files + logs live here)
JOB_DIR = "/Volumes/edapt_datastudio_etl/ree_files/landing/non_fed_cleanup"
LOG_DIR = f"{JOB_DIR}/logs"

# Target schemas
TARGET_CATALOG = "ree_edapt"
BRONZE_SCHEMA = "hr_bronze"
GOLD_SCHEMA = "hr"

# File mapping: SharePoint filename substring -> bronze table name
FILE_PATTERN_MAPPING = {
    "Applicant_Status_Export": "ree_usaccess_applicant_status_bronze",
    "REE_Contrct_Contractor": "ree_empowhr_contractor_bronze",
}

# Column definitions per output table (sanitized names, empty = all columns)
# These are used in the bronze-to-gold step to select specific columns
COLUMN_SELECTION = {
    "ree_usaccess_applicant_status_bronze": [],  # all columns
    "ree_empowhr_contractor_bronze": [
        "ID", "Account_#", "Contract_Status", "Begin_Date",
        "Expire_Date", "Justify", "Description"
    ],
}
