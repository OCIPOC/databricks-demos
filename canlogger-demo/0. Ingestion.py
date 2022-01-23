# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import mdf_iter
import canedge_browser

import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, load_dbc_files, restructure_data, add_custom_sig, ProcessData

# COMMAND ----------

sample_file = '/dbfs/mnt/canedge2/vehicle-1/00000002/00000001-61E3B0FF.MF4'

# COMMAND ----------

proc = ProcessData(fs, db_list, signals=[])
df_phys_all = pd.DataFrame()
df_raw, device_id = proc.get_raw_data(sample_file)
df_phys = proc.extract_phys(df_raw)
proc.print_log_summary(device_id, log_file, df_phys)


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


