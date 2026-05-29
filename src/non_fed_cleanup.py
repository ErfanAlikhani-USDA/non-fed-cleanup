# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install xlrd openpyxl -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./sharepoint_config

# COMMAND ----------

# DBTITLE 1,SharePoint functions
import requests
import time
import logging
from datetime import datetime

logger = logging.getLogger("sharepoint_ingest")
logger.setLevel(logging.INFO)


def get_acs_token(client_id, client_secret, tenant_id, sp_domain):
    """Get SharePoint access token via Legacy ACS (app-only)."""
    acs_url = f"https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
    resource_id = f"00000003-0000-0ff1-ce00-000000000000/{sp_domain}@{tenant_id}"
    client_id_full = f"{client_id}@{tenant_id}"

    resp = requests.post(acs_url, data={
        "grant_type": "client_credentials",
        "client_id": client_id_full,
        "client_secret": client_secret,
        "resource": resource_id
    })

    if resp.status_code == 200:
        return resp.json()["access_token"]
    else:
        raise Exception(f"ACS auth failed ({resp.status_code}): {resp.text[:300]}")


def list_files(token, sp_domain, folder_prefix, sp_folder_full):
    """List all files in a SharePoint folder."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json;odata=verbose"}
    url = f"https://{sp_domain}{folder_prefix}/_api/web/GetFolderByServerRelativeUrl('{sp_folder_full}')/Files"
    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        return resp.json()["d"]["results"]
    else:
        raise Exception(f"Failed to list files ({resp.status_code}): {resp.text[:300]}")


def download_file(token, sp_domain, folder_prefix, file_server_url, output_path, max_retry=3, retry_delay=30):
    """Download a single file from SharePoint to UC Volume with retry."""
    headers = {"Authorization": f"Bearer {token}"}
    download_url = f"https://{sp_domain}{folder_prefix}/_api/web/GetFileByServerRelativeUrl('{file_server_url}')/$value"

    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.get(download_url, headers=headers, stream=True)
            if resp.status_code == 200:
                with open(output_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            else:
                logger.warning(f"Attempt {attempt}: HTTP {resp.status_code} for {file_server_url}")
        except Exception as e:
            logger.warning(f"Attempt {attempt}: {e}")

        if attempt < max_retry:
            time.sleep(retry_delay)

    return False


def sanitize_column_name(col):
    """Replace invalid characters with underscores for Delta compatibility."""
    import re
    return re.sub(r'[ ,;{}()\n\t=]+', '_', col).strip('_')

# COMMAND ----------

# DBTITLE 1,Ingest new/modified files to Delta bronze
import os
import csv
import pandas as pd

# --- Main ingestion job ---
client_id = dbutils.secrets.get(scope="ree-edapt", key="SP_ID")
client_secret = dbutils.secrets.get(scope="ree-edapt", key="SP_SECRET")

# Authenticate
token = get_acs_token(client_id, client_secret, TENANT_ID, SP_DOMAIN)
print("\u2713 Authenticated")

# List files in SharePoint
files = list_files(token, SP_DOMAIN, FOLDER_PREFIX, SP_FOLDER_FULL)
print(f"\u2713 Found {len(files)} file(s) in SharePoint folder")

# Filter by pattern
if FILE_PATTERN_MAPPING:
    filtered = [f for f in files if any(p.lower() in f["Name"].lower() for p in FILE_PATTERN_MAPPING)]
    print(f"  Filtered to {len(filtered)} file(s) matching patterns")
else:
    filtered = files

# Check last ingestion times from log files
last_modified_map = {}
os.makedirs(LOG_DIR, exist_ok=True)
log_files = sorted([f for f in os.listdir(LOG_DIR) if f.endswith(".csv")]) if os.path.exists(LOG_DIR) else []
if log_files:
    latest_log = os.path.join(LOG_DIR, log_files[-1])
    with open(latest_log, "r") as lf:
        reader = csv.DictReader(lf)
        for row in reader:
            if row["status"] == "success":
                last_modified_map[row["file_name"]] = row["file_last_modified"]
    print(f"  Found {len(last_modified_map)} previously ingested file(s) in logs")
else:
    print("  First run \u2014 no log files yet")

# Determine which files are new or modified
files_to_process = []
for file_meta in filtered:
    name = file_meta["Name"]
    sp_modified = file_meta["TimeLastModified"]
    prev_modified = last_modified_map.get(name)

    if prev_modified is None or sp_modified > prev_modified:
        files_to_process.append(file_meta)
        status = "NEW" if prev_modified is None else "MODIFIED"
        print(f"  {status}: {name} (SP modified: {sp_modified})")
    else:
        print(f"  SKIP: {name} (unchanged since last run)")

if not files_to_process:
    print("\n\u2713 No new or modified files \u2014 nothing to do")
    results = {"success": [], "failed": []}
else:
    os.makedirs(JOB_DIR, exist_ok=True)

    results = {"success": [], "failed": []}
    for file_meta in files_to_process:
        name = file_meta["Name"]
        server_url = file_meta["ServerRelativeUrl"]
        sp_modified = file_meta["TimeLastModified"]
        staging_path = os.path.join(JOB_DIR, name)

        # Determine target table name
        table_name = None
        for pattern, tbl in FILE_PATTERN_MAPPING.items():
            if pattern.lower() in name.lower():
                table_name = tbl
                break

        print(f"\n  Downloading: {name} -> {TARGET_CATALOG}.{BRONZE_SCHEMA}.{table_name}")
        success = download_file(token, SP_DOMAIN, FOLDER_PREFIX, server_url, staging_path,
                                max_retry=MAX_RETRY, retry_delay=RETRY_DELAY_SEC)
        if not success:
            results["failed"].append({"name": name, "error": "Max retries exceeded", "last_modified": sp_modified})
            print(f"  \u2717 Download FAILED")
            continue

        try:
            if name.lower().endswith(".csv"):
                pdf = pd.read_csv(staging_path)
            elif name.lower().endswith((".xls", ".xlsx")):
                pdf = pd.read_excel(staging_path, header=1)
            else:
                pdf = pd.read_csv(staging_path)

            # Drop fully empty columns
            pdf = pdf.dropna(axis=1, how='all')

            # Sanitize column names for Delta compatibility
            pdf.columns = [sanitize_column_name(str(col)) for col in pdf.columns]

            # Convert to Spark and write as Delta with mergeSchema
            sdf = spark.createDataFrame(pdf)
            full_table = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
            sdf.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(full_table)

            row_count = len(pdf)
            results["success"].append({"name": name, "table": full_table, "rows": row_count, "last_modified": sp_modified})
            print(f"  \u2713 Written to {full_table} ({row_count:,} rows, mergeSchema=true)")
        except Exception as e:
            results["failed"].append({"name": name, "error": str(e)[:200], "last_modified": sp_modified})
            print(f"  \u2717 Failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"Success: {len(results['success'])} | Failed: {len(results['failed'])}")
    for r in results["success"]:
        print(f"  \u2713 {r['table']} ({r['rows']:,} rows)")

# COMMAND ----------

# DBTITLE 1,Write log file
# --- Write log file (CSV in job folder alongside downloaded files) ---
if results["success"] or results["failed"]:
    log_filename = f"ingest_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    log_filepath = os.path.join(LOG_DIR, log_filename)

    log_cols = ["file_name", "table_name", "row_count", "file_last_modified", "source_folder", "ingested_at", "status"]

    with open(log_filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=log_cols)
        writer.writeheader()
        for r in results["success"]:
            writer.writerow({
                "file_name": r["name"],
                "table_name": r["table"],
                "row_count": r["rows"],
                "file_last_modified": r["last_modified"],
                "source_folder": SP_FOLDER_FULL,
                "ingested_at": datetime.now().isoformat(),
                "status": "success"
            })
        for r in results["failed"]:
            writer.writerow({
                "file_name": r["name"],
                "table_name": "",
                "row_count": 0,
                "file_last_modified": r["last_modified"],
                "source_folder": SP_FOLDER_FULL,
                "ingested_at": datetime.now().isoformat(),
                "status": f"failed: {r['error']}"
            })

    print(f"\u2713 Log written: {log_filepath}")
else:
    print("No files processed \u2014 no log written")

# COMMAND ----------

# DBTITLE 1,Bronze to Gold (cleanse + promote to hr schema)
from pyspark.sql.functions import current_timestamp, trim, col

# --- Bronze to Gold: cleanse and promote to hr schema ---
for bronze_table, gold_suffix in FILE_PATTERN_MAPPING.items():
    # Bronze table name (with _bronze)
    bronze_full = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{gold_suffix}"
    # Gold table name (without _bronze)
    gold_table_name = gold_suffix.replace("_bronze", "")
    gold_full = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.{gold_table_name}"

    print(f"\n  {bronze_full} -> {gold_full}")

    try:
        df = spark.read.table(bronze_full)

        # Select columns if specified, otherwise keep all
        selected_cols = COLUMN_SELECTION.get(gold_suffix, [])
        if selected_cols:
            # Only keep columns that exist in the DataFrame
            available = set(df.columns)
            valid_cols = [c for c in selected_cols if c in available]
            missing = [c for c in selected_cols if c not in available]
            if missing:
                print(f"    WARNING: columns not found: {missing}")
            df = df.select(valid_cols)

        # Data cleansing: trim whitespace from string columns
        for field in df.schema.fields:
            if str(field.dataType) == "StringType":
                df = df.withColumn(field.name, trim(col(field.name)))

        # Drop fully null rows
        df = df.dropna(how="all")

        # Add ingest timestamp
        df = df.withColumn("ingest_date", current_timestamp())

        # Write to gold
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_full)

        row_count = df.count()
        print(f"    \u2713 Written to {gold_full} ({row_count:,} rows)")

    except Exception as e:
        print(f"    \u2717 Failed: {e}")

print(f"\n{'='*60}")
print("Bronze to Gold complete")
