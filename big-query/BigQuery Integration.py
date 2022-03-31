# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Reading data from BigQuery

# COMMAND ----------

table = "fe-dev-sandbox.xl_axiata_demo_dataset.customer_demographics" #FIXME
df = spark.read.format("bigquery").option("table",table).load()
df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Querying your BigQuery table through Spark DataFrame

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   cd_gender,
# MAGIC   cd_marital_status,
# MAGIC   min(cd_purchase_estimate) as minimum,
# MAGIC   max(cd_purchase_estimate) as maximum,
# MAGIC   percentile(cd_purchase_estimate, 0.5) as P50,
# MAGIC   percentile(cd_purchase_estimate, 0.90) as P90
# MAGIC from
# MAGIC   temp_view
# MAGIC group by
# MAGIC   cd_gender,
# MAGIC   cd_marital_status
# MAGIC order by
# MAGIC   cd_gender,
# MAGIC   cd_marital_status

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delegating execution of SQL query to BigQuery with the `query()` API

# COMMAND ----------

table = "fe-dev-sandbox.xl_axiata_demo_dataset.customer_demographics"

tempLocation = "mdataset"

q = """select
  cd_gender,
  cd_marital_status,
  min(cd_purchase_estimate) as minimum,
  max(cd_purchase_estimate) as maximum,
  percentile(cd_purchase_estimate, 0.5) as P50,
  percentile(cd_purchase_estimate, 0.90) as P90
from
  fe-dev-sandbox.xl_axiata_demo_dataset.customer_demographics
group by
  cd_gender,
  cd_marital_status
order by
  cd_gender,
  cd_marital_status"""


df2 = spark.read.format("bigquery").option("query", q).option("materializationProject","fe-dev-sandbox").option("materializationDataset", "mdataset").load()


df2.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing data to BigQuery
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC The connector writes the data to BigQuery by first buffering all the data into a Cloud Storage temporary table, and then it copies all data from into BigQuery in one operation. 
# MAGIC The connector attempts to delete the temporary files once the BigQuery load operation has succeeded and once again when the Spark application terminates. 
# MAGIC If the job fails, you may need to manually remove any remaining temporary Cloud Storage files. 
# MAGIC Typically, you'll find temporary BigQuery exports in gs://[bucket]/.spark-bigquery-[jobid]-[UUID].
# MAGIC ```
# MAGIC Source: https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example

# COMMAND ----------

from pyspark.sql.types import StringType
mylist = ["Google", "Databricks", "better together"]

df = spark.createDataFrame(mylist, StringType())


table = "fe-dev-sandbox.xl_axiata_demo_dataset.better_together"

#Indirect write
bucket = 'hiimivantang'
df.write.format("bigquery").option("temporaryGcsBucket", bucket).mode("overwrite").save(table)

#Direct write
# df.write.format("bigquery").option("writeMethod", "direct").mode("overwrite").save(table)
