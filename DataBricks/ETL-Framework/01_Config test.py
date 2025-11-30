# Databricks notebook source
# ==========================================================
# 01_Config  (Simple Version)
# Purpose:
#   - Configure Spark to access Azure Storage using account key
#   - Test reading a CSV file from:
#       https://scrgvkrmade.blob.core.windows.net/project/raw/Store.csv
# ==========================================================

# ---------------------------
# Storage account information
# ---------------------------
STORAGE_ACCOUNT = "scrgvkrmade"
ACCOUNT_KEY = "E4VB7pXWFXttUWbbSBPY35/Dvsw6Fs6XgIWLTj3lCS6v/jCEow9Uxs+r6Usobhenv14UdWEzb+R8+AStNyS0dg=="

# ---------------------------
# Set Spark configuration
# ---------------------------
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    ACCOUNT_KEY
)

print("Storage configuration applied successfully.")

# ---------------------------
# Path to CSV file in Azure Blob
# ---------------------------
csv_path = f"wasbs://project@{STORAGE_ACCOUNT}.blob.core.windows.net/raw/Store.csv"
print("CSV Path:", csv_path)

# ---------------------------
# Read CSV (no header example)
# ---------------------------
try:
    df = (spark.read
                .option("header", "false")     # no header in raw file
                .option("inferSchema", "true")
                .csv(csv_path))

    print("SUCCESS: CSV loaded from Azure Blob Storage!")
    display(df)

except Exception as e:
    print("ERROR reading CSV from Azure Blob Storage:")
    print(e)


# COMMAND ----------

# show current notebook path (helpful to find relative location)
try:
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print("Notebook path:", path)
except Exception as e:
    print("Could not get notebook path via dbutils:", e)

# list files under the parent folder in Workspace/Repos (adjust path as needed)
# If you are in a Repo, try listing the repo folder (example below shows how to compute parent)


# COMMAND ----------

folder = "/Users/u2786997@uel.ac.uk/OSBI/DataBricks/ETL-Framework"
print("Listing:", folder)
for f in dbutils.fs.ls("repos/" + folder.replace("/Repos/","")):   # try repos path if you use Repos
    print(f.path)
# fallback: list workspace folder using workspace API (may not list in Repos)
try:
    items = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceUrl()
    print("workspace base:", items)
except Exception:
    pass

# Simpler: list via workspace import API - safer to use UI if above fails
