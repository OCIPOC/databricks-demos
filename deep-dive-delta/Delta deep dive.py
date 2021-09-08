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

parquet_path

# COMMAND ----------

spark.sql("select count(*) from loans_parquet").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br><br>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Parquet ➡️ Delta

# COMMAND ----------

delta_path = "/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet"

spark.sql("CONVERT TO DELTA parquet.`%s`" %delta_path)

spark.read.format("delta").load(delta_path).createOrReplaceTempView("loans_delta")

# COMMAND ----------

# MAGIC  %sh
# MAGIC  
# MAGIC  ls /dbfs/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #🤓 DML queries!

# COMMAND ----------

spark.sql("UPDATE loans_delta SET addr_state='TEXAS' WHERE addr_state = 'TX'").show()

# COMMAND ----------

spark.sql("DELETE FROM loans_delta WHERE funded_amnt<40000").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 📃 Table versioning

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, delta_path)
display(deltaTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC # 🕰 Time Travel

# COMMAND ----------

spark.sql("SELECT * FROM delta.`%s` VERSION AS OF 3" %(delta_path)).show()

# COMMAND ----------

spark.sql("RESTORE TABLE delta.`%s` VERSION AS OF 150" %(delta_path)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC <br><br><br><br><br>

# COMMAND ----------

# MAGIC %md
# MAGIC #🚰 Stream + Batch

# COMMAND ----------

# DBTITLE 1,Read Stream
spark.readStream.format("delta").load(delta_path).createOrReplaceTempView("loans_delta_stream")

display(spark.sql("select count(*) from loans_delta_stream"))

# COMMAND ----------

df = spark.readStream.format("delta").load(delta_path)

# COMMAND ----------

# DBTITLE 1,Batch read
spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

# DBTITLE 1,Batch update
spark.sql("UPDATE loans_delta SET addr_state='CALIFORNIA' WHERE addr_state = 'CA'").show()

# COMMAND ----------

# DBTITLE 1,Write Stream
stream_query_2 = generate_and_append_data_stream(table_format = "delta", table_path = delta_path)

# COMMAND ----------

spark.sql("DESCRIBE loans_delta_stream").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema evolution
# MAGIC 
# MAGIC Each output row from `Rate source` contains `timestamp` and `value`.
# MAGIC 
# MAGIC See [Input Sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources) for other built-in sources with details.

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

delta_path

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lh /dbfs/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 🚅 Optimization

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
# MAGIC ALTER TABLE delta.`/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet` SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tblproperties delta.`/Users/ivan.tang@databricks.com/unpacking-transaction-log/loans_parquet`

# COMMAND ----------

stop_all_streams()

# COMMAND ----------


