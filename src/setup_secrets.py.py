# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Secrets
# MAGIC One-time setup for SharePoint credentials. Run this once to create the scope and store secrets.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.secrets.create_scope(scope="ree-edapt")
    print("Scope 'ree-edapt' created")
except Exception as e:
    if "RESOURCE_ALREADY_EXISTS" in str(e):
        print("Scope 'ree-edapt' already exists")
    else:
        raise

# COMMAND ----------

w.secrets.put_secret(scope="ree-edapt", key="SP_ID", string_value="XXX")
w.secrets.put_secret(scope="ree-edapt", key="SP_SECRET", string_value="XXX")
print("Secrets stored in scope 'ree-edapt'")

