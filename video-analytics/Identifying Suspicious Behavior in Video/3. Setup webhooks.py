# Databricks notebook source
import mlflow
from mlflow.utils.rest_utils import http_request
import json
import urllib 

# COMMAND ----------

dbutils.widgets.text('model_name','20211103-demo-model')

def client():
  return mlflow.tracking.client.MlflowClient()

host_creds = client()._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

# COMMAND ----------

import urllib 
import json 
model_name = dbutils.widgets.get("model_name")
slack_webhook = dbutils.secrets.get("rk_webhooks", "slack") # You have to set up your own webhook!

# consider REGISTERED_MODEL_CREATED to run tests and autoamtic deployments to stages 
trigger_slack = json.dumps({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED","MODEL_VERSION_TRANSITIONED_STAGE"],
  "description": "Notify the MLOps team on model transition request and success",
  "status": "ACTIVE",
  "http_url_spec": {
    "url": slack_webhook
  }
})

mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)

# COMMAND ----------

# list_model_webhooks = json.dumps({"model_name": dbutils.widgets.get('model_name')})

# mlflow_call_endpoint("registry-webhooks/list", method = "GET", body = list_model_webhooks)

# COMMAND ----------

# list_model_webhooks = json.dumps({"model_name": dbutils.widgets.get("model_name")})
# webhooks = mlflow_call_endpoint("registry-webhooks/list", method = "GET", body = list_model_webhooks)
# for i in webhooks['webhooks']:
#   mlflow_call_endpoint("registry-webhooks/delete",
#                      method="DELETE",
#                      body = json.dumps({'id': i['id']}))

# COMMAND ----------


