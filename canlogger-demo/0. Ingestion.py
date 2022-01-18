# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import mdf_iter
import canedge_browser

import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, load_dbc_files, restructure_data, add_custom_sig, ProcessData

# COMMAND ----------



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


