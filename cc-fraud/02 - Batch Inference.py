# Databricks notebook source
import mlflow
from databricks.feature_store import FeatureStoreClient

model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/techcombank-cc-fraud/staging") # may need to replace with your own model name

# COMMAND ----------

fs = FeatureStoreClient()
features = fs.read_table('techcombank.cc_fraud_features')

# COMMAND ----------

predictions = features.withColumn('predictions', model(*features.columns))
display(predictions.select("ID", "FRD_IND", "predictions"))
