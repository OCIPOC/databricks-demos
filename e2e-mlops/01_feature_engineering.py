# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2FeShKp9ffsT.png?alt=media&token=1d1949a6-e560-4e6b-8933-26355abaf2b6)
# MAGIC 
# MAGIC _Source: [Hidden Technical Debt in Machine Learning Systems](https://papers.nips.cc/paper/2015/file/86df7dcfd896fcaf2674f757a2463eba-Paper.pdf)_

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC <img src="https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2Fbje2dSlofK.png?alt=media&token=54e42f51-8a46-4f8c-b070-8f2cd6c5d7f9" width=1000 />

# COMMAND ----------

# MAGIC %md
# MAGIC <br><br><br><br><br>
# MAGIC 
# MAGIC ## Churn Prediction Feature Engineering
# MAGIC 
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/step1.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Featurization Logic
# MAGIC 
# MAGIC This is a fairly clean dataset so we'll just do some one-hot encoding, and clean up the column names afterward.

# COMMAND ----------

# Read into Spark
telcoDF = spark.table("ibm_telco_churn.bronze_customers")

display(telcoDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from 

# COMMAND ----------

# MAGIC %md
# MAGIC Using `koalas` to scale my teammates' `pandas` code.

# COMMAND ----------

from databricks.feature_store import feature_table
import databricks.koalas as ks



def compute_churn_features(data):
  
  # Convert to koalas
  data = data.to_koalas()
  
  # OHE
  data = ks.get_dummies(data, 
                        columns=['gender', 'partner', 'dependents',
                                 'phoneService', 'multipleLines', 'internetService',
                                 'onlineSecurity', 'onlineBackup', 'deviceProtection',
                                 'techSupport', 'streamingTV', 'streamingMovies',
                                 'contract', 'paperlessBilling', 'paymentMethod'],dtype = 'int64')
  
  # Convert label to int and rename column
  data['churnString'] = data['churnString'].map({'Yes': 1, 'No': 0})
  data = data.astype({'churnString': 'int32'})
  data = data.rename(columns = {'churnString': 'churn'})
  
  # Clean up column names
  data.columns = data.columns.str.replace(' ', '')
  data.columns = data.columns.str.replace('(', '-')
  data.columns = data.columns.str.replace(')', '')
  
  # Drop missing values
  data = data.dropna()
  
  return data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Compute and write features
# MAGIC 
# MAGIC ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2F29TxD_KMu8.png?alt=media&token=de7c69de-f91d-4bc9-80ea-cfbbbbac6ac7)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

churn_features_df = compute_churn_features(telcoDF)

churn_feature_table = fs.create_feature_table(
  name='ibm_telco_churn.churn_features',
  keys='customerID',
  schema=churn_features_df.spark.schema(),
  description='These features are derived from the ibm_telco_churn.bronze_customers table in the lakehouse.  I created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)

fs.write_table(df=churn_features_df.to_spark(), name='ibm_telco_churn.churn_features', mode='overwrite')

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
df = fs.read_table(name="ibm_telco_churn.churn_features")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC As an alternative we could always write to Delta Lake:

# COMMAND ----------

# # Write out silver-level data to Delta lake
# trainingDF = spark.createDataFrame(training_df)

# trainingDF.write.format('delta').mode('overwrite').save(silver_tbl_path)

# # Create silver table
# spark.sql('''
#   CREATE TABLE `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(database_name,silver_tbl_name,silver_tbl_path))

# # Drop customer ID for AutoML
# automlDF = trainingDF.drop('customerID')

# # Write out silver-level data to Delta lake
# automlDF.write.format('delta').mode('overwrite').save(automl_tbl_path)

# # Create silver table
# _ = spark.sql('''
#   CREATE TABLE `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(database_name,automl_tbl_name,automl_tbl_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Go to: [02_auto_ml_baseline](https://adb-2095731916479437.17.azuredatabricks.net/?o=2095731916479437#notebook/3683571799076275/command/3683571799076277) Notebook
