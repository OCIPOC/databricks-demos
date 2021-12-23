# Databricks notebook source
# MAGIC %md
# MAGIC # Deploying model to Sagemaker as container

# COMMAND ----------

region = "us-east-1"
model_uri = "models:/imda-demo-model/staging"
image_ecr_url = "997819012307.dkr.ecr.us-west-1.amazonaws.com/databricks:latest"

# COMMAND ----------

import mlflow.sagemaker as mfs
app_name = "telco-churn-app"

mfs.deploy(app_name=app_name, model_uri=model_uri, image_url=image_ecr_url, mode="create", flavor="python_function", region_name=region)

# COMMAND ----------



# COMMAND ----------

import boto3
 
def check_status(app_name):
  sage_client = boto3.client('sagemaker', region_name=region)
  endpoint_description = sage_client.describe_endpoint(EndpointName=app_name)
  endpoint_status = endpoint_description["EndpointStatus"]
  return endpoint_status
 
print("Application status is: {}".format(check_status(app_name)))

# COMMAND ----------

test_data = spark.read.table('telco.churn_features')
query_df = test_data.limit(10)
display(query_df)

# COMMAND ----------

import json
 
def query_endpoint(app_name, input_json):
  client = boto3.session.Session().client("sagemaker-runtime", region)
  response = client.invoke_endpoint(
      EndpointName=app_name,
      Body=input_json,
      ContentType='application/json',
  )
  preds = response['Body'].read().decode("ascii")
  preds = json.loads(preds)
  print("Received response: {}".format(preds))
  return preds
 
print("Sending batch prediction request with input: {input_df}".format(input_df=query_df))
 
# Convert the test dataframe into a JSON-serialized Pandas dataframe
input_json = query_df.toPandas().to_json(orient="split")
 
# Evaluate the input by posting it to the deployed model
prediction = query_endpoint(app_name=app_name, input_json=input_json)

