# Databricks notebook source
# MAGIC %md
# MAGIC ### Managing the model lifecycle with Model Registry
# MAGIC 
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/step4.png?raw=true">
# MAGIC 
# MAGIC One of the primary challenges among data scientists and ML engineers is the absence of a central repository for models, their versions, and the means to manage them throughout their lifecycle.  
# MAGIC 
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) addresses this challenge and enables members of the data team to:
# MAGIC <br><br>
# MAGIC * **Discover** registered models, current stage in model development, experiment runs, and associated code with a registered model
# MAGIC * **Transition** models to different stages of their lifecycle
# MAGIC * **Deploy** different versions of a registered model in different stages, offering MLOps engineers ability to deploy and conduct testing of different model versions
# MAGIC * **Test** models in an automated fashion
# MAGIC * **Document** models throughout their lifecycle
# MAGIC * **Secure** access and permission for model registrations, transitions or modifications
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg">

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Use the Model Registry
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. During the course of this development cycle, they will select the best run within an experiment and register its model with the registry.  Think of this as **committing** the model to the registry, much as you would commit code to a version control system.  
# MAGIC 
# MAGIC The registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Promote to Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC 
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC registered_model_version = mlflow.register_model(model_uri, model_name)
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text('run_id','a4c408651502439b90e0b43b042d6938')
dbutils.widgets.text('model_name', 'e2e-mlops-demo-model')

run_id = dbutils.widgets.get('run_id') 
model_name = dbutils.widgets.get('model_name')
model_uri = f"runs:/{run_id}/model"

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

client.set_tag(run_id, key='db_table', value='ibm_telco_churn.churn_features')
client.set_tag(run_id, key='demographic_vars', value='seniorCitizen,gender_Female')

model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

run_info = client.get_run(run_id)

run_info.info

# COMMAND ----------

# MAGIC %md
# MAGIC At this point the model will be in `None` stage.  Let's update the description before moving it to `Staging`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update Description

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_version_details = client.get_model_version(name=model_name, version=model_details.version)

client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a customer will churn using features from the ibm_telco_churn database.  It is used to update the Telco Churn Dashboard in SQL Analytics."
)

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using LightGBM for demo purposes..."
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Request Transition to Staging
# MAGIC 
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/webhooks2.png?raw=true" width = 800>

# COMMAND ----------

# Helper functions
import mlflow
from mlflow.utils.rest_utils import http_request
import json

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

# Transition request to staging
staging_request = {'name': model_name,
                   'version': model_details.version,
                   'stage': 'Staging',
                   'archive_existing_versions': 'true'}

mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))

# COMMAND ----------

# Leave a comment for the ML engineer who will be reviewing the tests
comment = "This was the one best model from AutoML within <30 minutes, I think we can use it as a baseline for demo..."
comment_body = {'name': model_name, 'version': model_details.version, 'comment': comment}
mlflow_call_endpoint('comments/create', 'POST', json.dumps(comment_body))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Go to [05_ops_validation](https://adb-2095731916479437.17.azuredatabricks.net/?o=2095731916479437#notebook/2057356028947983/command/2057356028948011) notebook.

# COMMAND ----------


