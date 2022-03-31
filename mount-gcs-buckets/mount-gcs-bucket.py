# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Ensure the following are included in your spark config:
# MAGIC 
# MAGIC ```
# MAGIC spark.hadoop.google.cloud.auth.service.account.enable true
# MAGIC spark.hadoop.fs.gs.auth.service.account.email <client_email>
# MAGIC spark.hadoop.fs.gs.project.id <project_id>
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key <private_key>
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key.id <private_key_id>
# MAGIC ```

# COMMAND ----------

bucket_name = "hiimivantang"
mount_name = "hiimivantang-gcs-bucket"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/hiimivantang-gcs-bucket/test`
