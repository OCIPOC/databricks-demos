# Databricks notebook source
# MAGIC %run ../classes/import_classes

# COMMAND ----------

clear_working_dirs()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Yak-shaving

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downloading data from Lending Club
# MAGIC 
# MAGIC 
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.lendingclub.com/), containing the following columns:
# MAGIC 
# MAGIC * 
# MAGIC 
# MAGIC For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br><br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Creating a parquet table and temp view

# COMMAND ----------

# DBTITLE 1,Creating a parquet table, then a temporary view
spark.read.format("parquet") \
  .load("/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet") \
  .write.format("parquet").save(parquet_path)


spark.read.format("parquet") \
  .load(parquet_path) \
  .createOrReplaceTempView("loans_parquet")

# COMMAND ----------

display(dbutils.fs.ls(parquet_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br><br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Parquet ➡️ Delta

# COMMAND ----------

delta_path = parquet_path

spark.sql("CONVERT TO DELTA parquet.`%s`" %delta_path)

df_loans = spark.read.format("delta").load(delta_path)
df_loans.createOrReplaceTempView("loans_delta")

# COMMAND ----------

display(dbutils.fs.ls(parquet_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM loans_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Replace State Abbreviations 

# COMMAND ----------

import pandas as pd
df_states = pd.read_csv('https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv')
mapping = dict([(t[1], t[0]) for t in df_states.values.tolist()])

df_loans = df_loans.withColumn("addr_state", df_loans["addr_state"])\
    .replace(to_replace=list(mapping.keys()), value=list(mapping.values()), subset="addr_state")

df_loans.write.format("delta").mode('overwrite').save(delta_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from loans_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC # 🕰 Time Travel

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Table versioning

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, delta_path)
display(deltaTable.history())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Querying earlier versions

# COMMAND ----------

spark.sql("SELECT * FROM delta.`%s` VERSION AS OF 0" %(delta_path)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restoring

# COMMAND ----------

spark.sql("RESTORE TABLE delta.`%s` VERSION AS OF 0" %(delta_path)).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loans_delta

# COMMAND ----------

# MAGIC %md
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC #🚰 Stream + Batch

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Read stream

# COMMAND ----------

spark.readStream.format("delta").load(delta_path).createOrReplaceTempView("loans_delta_stream")

display(spark.sql("select count(*) from loans_delta_stream"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Batch read

# COMMAND ----------

# DBTITLE 1,Batch read
# MAGIC %sql
# MAGIC SELECT count(*) FROM loans_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch update

# COMMAND ----------

spark.sql("UPDATE loans_delta SET addr_state='TEXAS' WHERE addr_state = 'Texas'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #🌱 Schema Evolution
# MAGIC 
# MAGIC Each output row from `Rate source` contains `timestamp` and `value`.
# MAGIC 
# MAGIC See [Input Sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources) for other built-in sources with details.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Enforcement of schema

# COMMAND ----------

stream_query_2 = generate_and_append_data_stream(table_format = "delta", table_path = delta_path)

# COMMAND ----------

spark.sql("DESCRIBE loans_delta_stream").show()

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 🚅 Optimization

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lh /dbfs/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Bin-packing

# COMMAND ----------

spark.sql("OPTIMIZE delta.`%s`" %(delta_path)).show()

# COMMAND ----------

spark.sql("VACUUM delta.`%s` RETAIN 0 HOURS" %(delta_path)).show()

# COMMAND ----------

spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2FoOK-6lOYIp.png?alt=media&token=cb50b6db-3089-4be2-89bc-d4f2ecf2af56)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Enabling Auto Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   delta.`/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet`
# MAGIC SET
# MAGIC   TBLPROPERTIES (
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC   )

# COMMAND ----------

# MAGIC %sql show tblproperties delta.`/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet`

# COMMAND ----------

stop_all_streams()

# COMMAND ----------


