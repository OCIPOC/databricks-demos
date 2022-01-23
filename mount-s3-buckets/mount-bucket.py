# Databricks notebook source
access_key = dbutils.secrets.get(scope = "aws_storage_keys", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws_storage_keys", key = "aws-secret-key")

# COMMAND ----------

aws_bucket_name = "hiimivantang-canedge"
mount_name = "canedge2"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, secret_key.replace("/", "%2F"), aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))


# COMMAND ----------


