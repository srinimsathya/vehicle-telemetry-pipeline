# Databricks notebook source
# DBTITLE 1,Import Functions
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load .env secrets
load_dotenv()

# COMMAND ----------

# DBTITLE 1,Authentication
storage_account_name = os.getenv("AZURE_ACCOUNT_NAME")
storage_account_key = os.getenv("AZURE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

# DBTITLE 1,Read - Current File
# Set the directory path
input_dir = adls_path

# List all files in container
all_files = dbutils.fs.ls(input_dir)

# Target the specific file
target_file_name = "vehicle_data_current.json"
json_files = [f for f in all_files if f.name == target_file_name]

if not json_files:
    raise FileNotFoundError(f"File '{target_file_name}' not found in the directory.")

# Read the file
latest_file_path = json_files[0].path
df = spark.read.json(latest_file_path)

df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Clean
# Convert timestamp to Spark TimestampType
df_cleaned = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Inspect for nulls
df_cleaned.select([col(c).isNull().alias(c) for c in df_cleaned.columns])
display(df_cleaned)

# COMMAND ----------

# DBTITLE 1,Load
# Set delta path
delta_path = adls_path + "delta_folder_name/"

# Write as Delta
df_cleaned.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

# DBTITLE 1,UC Catalog Creation
%sql
CREATE CATALOG IF NOT EXISTS <your_catalog_name>
MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/<delta_folder_name>/';

# COMMAND ----------

# DBTITLE 1,UC Schema Creation
%sql
CREATE SCHEMA IF NOT EXISTS <your_catalog_name>.<your_schema_name>;

# COMMAND ----------

# DBTITLE 1,Select Catalog & Schema
%sql
USE CATALOG <your_catalog_name>;
USE SCHEMA <your_schema_name>;

# COMMAND ----------

# DBTITLE 1,UC Table Creation
%sql
CREATE TABLE IF NOT EXISTS <your_catalog_name>.<your_schema_name>.<table_name>
USING DELTA
LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/<delta_folder_name>/';

# COMMAND ----------

# DBTITLE 1,Read Table Preview
%sql
SELECT * FROM <your_catalog_name>.<your_schema_name>.<your_table_name>;
