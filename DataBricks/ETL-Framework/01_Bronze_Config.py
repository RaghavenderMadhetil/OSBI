# Databricks notebook source
# =====================================================
# 01_Config (FINAL - SIMPLE, NO SECRET SCOPE)
# =====================================================

# Widgets (ADF passes these)
dbutils.widgets.text("domain", "")            
dbutils.widgets.text("layer", "")             
dbutils.widgets.text("batch_name", "")        
dbutils.widgets.text("file_name", "")         
dbutils.widgets.text("transform_flag", "")    
dbutils.widgets.text("direct_account_key", "")  # ADF must pass the account key

# Read widget values
DOMAIN = dbutils.widgets.get("domain")
LAYER = dbutils.widgets.get("layer")
BATCH_NAME = dbutils.widgets.get("batch_name")
FILE_NAME = dbutils.widgets.get("file_name")
TRANSFORM_FLAG = dbutils.widgets.get("transform_flag")
ACCOUNT_KEY = dbutils.widgets.get("direct_account_key")

if ACCOUNT_KEY.strip() == "":
    raise Exception("direct_account_key must be passed from ADF.")

# Storage settings
STORAGE_ACCOUNT = "scrgvkrmade"
RAW_CONTAINER = "project"
BRONZE_CONTAINER = "bronze"

BASE_RAW_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BASE_BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Spark config (no secret scope, pure direct key)
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ACCOUNT_KEY
)

# Expose values for next notebooks (02_Utils / 03_Run)
dbutils.jobs.taskValues.set(key="BASE_RAW_PATH", value=BASE_RAW_PATH)
dbutils.jobs.taskValues.set(key="BASE_BRONZE_PATH", value=BASE_BRONZE_PATH)
dbutils.jobs.taskValues.set(key="DOMAIN", value=DOMAIN)
dbutils.jobs.taskValues.set(key="LAYER", value=LAYER)
dbutils.jobs.taskValues.set(key="BATCH_NAME", value=BATCH_NAME)
dbutils.jobs.taskValues.set(key="FILE_NAME", value=FILE_NAME)
dbutils.jobs.taskValues.set(key="TRANSFORM_FLAG", value=TRANSFORM_FLAG)

# Summary output
print("01_Config completed.")
print("RAW PATH:", BASE_RAW_PATH)
print("BRONZE PATH:", BASE_BRONZE_PATH)
print("DOMAIN:", DOMAIN, "| LAYER:", LAYER, "| FILE:", FILE_NAME)
