# Databricks notebook source
scope = 'azure_storage_keys'
storage_account_name = 'hiimivantang'

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://tardis6@hiimivantang.blob.core.windows.net",
  mount_point = "/mnt/tardis6",
  extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope=scope, key=storage_account_name)})

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/

# COMMAND ----------


