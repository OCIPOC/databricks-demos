# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls 

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget https://raw.githubusercontent.com/CSS-Electronics/api-examples/master/examples/data-processing/requirements.txt

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .load("/mnt/canedge2/*/*/*.MF4") \
  .writeStream \
  .option("checkpointLocation", "/dbfs/canlogger-demo-checkpoint") \
  .start("/dbfs/tables/canlogger-demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table if not exists canlogger_bronze USING DELTA LOCATION '/dbfs/tables/canlogger-demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC select path, modificationTime, length/1048576 as size_in_mb from canlogger_bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE canlogger_bronze

# COMMAND ----------


