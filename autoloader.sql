-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Incrementally ingesting csv from S3 bucket

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE bronze_customers 
COMMENT "Raw telco customers profile dataset"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *
FROM
  cloud_files("/FileStore/tables/telco/", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Set expectations and enforce data quality checks 

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE silver_customers (
  CONSTRAINT `internetService must be fiber optic/no/dsl`  EXPECT (internetService IN ('Fiber optic','No','DSL')) ON VIOLATION DROP ROW
)
COMMENT "Livestream of new telco customer profile data, cleaned and compliant"
TBLPROPERTIES ("quality" = "silver")
AS SELECT * FROM stream(LIVE.bronze_customers)

-- COMMAND ----------


