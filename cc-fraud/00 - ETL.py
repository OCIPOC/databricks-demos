# Databricks notebook source
# DBTITLE 1,Load data
from pyspark.sql.functions import * 
import cloudpickle
import pyspark.pandas as ks
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id 
dbutils.widgets.text("database", "techcombank", label="Database Name")
database_name = dbutils.widgets.get('database')


# File location and type, in this example is a synthetic dataset
# but features would eventually be served from a delta lake table
raw_data_path = "/path/Fraud.csv"
#raw_data_path = dbutils.fs.ls("/FileStore/shared_uploads/ricardo.portilla@databricks.com/Fraud_final-1.csv")
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load('dbfs:/FileStore/shared_uploads/ricardo.portilla@databricks.com/Fraud_final-1.csv')

df = df.withColumn("ID", monotonically_increasing_id())

# COMMAND ----------

# DBTITLE 1,Create managed bronze table
schema_directory = "/{}".format(database_name)
bronze_tbl_path = "/techcombank/bronze_table"


_ = spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(database_name,schema_directory))

df.write.format('delta').mode('overwrite').save("/techcombank/bronze_table")

_ = spark.sql("""
CREATE TABLE IF NOT EXISTS `{}`.bronze_table
USING DELTA
LOCATION '{}'
""".format(database_name, bronze_tbl_path))

# COMMAND ----------

# DBTITLE 1,Registering features
df = table("{}.bronze_table".format(database_name))

data = df.to_pandas_on_spark()
data = data.drop(columns=['AUTH_ID', 'ACCT_ID_TOKEN'])
numeric_columns = data.columns.to_list()
numeric_columns.remove('FRD_IND')
display(data)


from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
features_df = data

feature_table = fs.create_feature_table(
  name='{}.cc_fraud_features'.format(database_name),
  keys='ID',
  schema=data.spark.schema(),
  description='These features are derived from {}.bronze_table and label column (FRD_IND) has being dropped and not included...'.format(database_name)
)

fs.write_table(df=data.to_spark(), name='{}.cc_fraud_features'.format(database_name), mode='overwrite')
