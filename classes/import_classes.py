# Databricks notebook source
import os
import random
import shutil

from pyspark.sql.functions import * 
from pyspark.sql.types import *

from delta.tables import *

# COMMAND ----------

demo_path = "/Users/ivan.tang@databricks.com/unpacking-transaction-log"
parquet_path = demo_path + "/loans_parquet"
download_path = "/tmp/sais_eu_19_demo/loans"

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

import random
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

def clear_working_dirs():
  # Delete a new parquet table with the parquet file
  if os.path.exists("/dbfs" + parquet_path + "/_delta_log"):
    print("Deleting path " + parquet_path + "/_delta_log")
    shutil.rmtree("/dbfs" + parquet_path + "/_delta_log")
    
  # Delete a new parquet table with the parquet file
  if os.path.exists("/dbfs" + parquet_path):
    print("Deleting path " + parquet_path)
    shutil.rmtree("/dbfs" + parquet_path)
    
  if os.path.exists("/dbfs" + download_path):
    print("Deleting path " + download_path)
    shutil.rmtree("/dbfs" + download_path)

def random_checkpoint_dir(): 
  return "/sais_eu_19_demo/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state

states = ["CA", "TX", "NY", "WA", "TEXAS"]

@udf(returnType=StringType())
def random_state():
  return str(random.choice(states))

# Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_path):
  
  stream_data = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("loan_id", 10000 + col("value")) \
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) \
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) \
    .withColumn("addr_state", random_state())

  query = stream_data.writeStream \
    .format(table_format) \
    .option("checkpointLocation", random_checkpoint_dir()) \
    .trigger(processingTime = "10 seconds") \
    .start(table_path)

  return query

# Function to stop all streaming queries 
def stop_all_streams():
  # Stop all the streams
  print("Stopping all streams")
  for s in spark.streams.active:
    s.stop()
  print("Stopped all streams")
  print("Deleting checkpoints")  
  dbutils.fs.rm("/sais_eu_19_demo/chkpt/", True)
  print("Deleted checkpoints")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
