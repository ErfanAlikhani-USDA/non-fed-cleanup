# Databricks notebook source
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
SILVER_SCHEMA = "hr_silver"
GOLD_SCHEMA = "hr"

# File mapping: SharePoint filename substring -> bronze table name
# Each USAccess mission area file gets its own bronze table
FILE_PATTERN_MAPPING = {
    "ARS Applicant Status Export": "ree_usaccess_ars_bronze",
    "ERS Applicant Status Export": "ree_usaccess_ers_bronze",
    "NASS Applicant Status Export": "ree_usaccess_nass_bronze",
    "NIFA Applicant Status Export": "ree_usaccess_nifa_bronze",
    "REE EmpowHR Non-Fed": "ree_empowhr_contractor_bronze",
}

# Column definitions per output table (sanitized names, empty = all columns)
# These are used in the bronze-to-silver step to select specific columns
COLUMN_SELECTION = {
    "ree_usaccess_ars_bronze": [],
    "ree_usaccess_ers_bronze": [],
    "ree_usaccess_nass_bronze": [],
    "ree_usaccess_nifa_bronze": [],
    "ree_empowhr_contractor_bronze": [
        "ID", "Account_#", "Contract_Status", "Begin_Date",
        "Expire_Date", "Justify", "Description"
    ],
}

# USAccess bronze tables to UNION into silver
USACCESS_BRONZE_TABLES = [
    "ree_usaccess_ars_bronze",
    "ree_usaccess_ers_bronze",
    "ree_usaccess_nass_bronze",
    "ree_usaccess_nifa_bronze",
]
