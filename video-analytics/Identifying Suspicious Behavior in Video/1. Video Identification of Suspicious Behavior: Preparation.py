# Databricks notebook source
displayHTML("""<iframe src="https://docs.google.com/presentation/d/e/2PACX-1vRqbBxKxTaHqfTa3-Qzc9ahmb1YXzd3gOFxTEwm5Q9n3y7t6DcRbiMqgMcrp4AxigOHAhEVDdU8Of6U/embed?start=false&loop=false&delayms=60000" frameborder="0" width="960" height="569" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe>""")

# COMMAND ----------

# DBTITLE 0,Video Identification of Suspicious Behavior: Preparation
# MAGIC %md 
# MAGIC 
# MAGIC # Video Identification of Suspicious Behavior: Preparation
# MAGIC 
# MAGIC This notebook will **prepare** your video data by:
# MAGIC * Processing the images by extracting out individual images (using `open-cv`) and saving them to DBFS / cloud storage
# MAGIC * With the saved images, extract image features by using transfer learning with a pre-trained Inception V3 model and saving them to DBFS / cloud storage in Parquet format
# MAGIC  * Perform this task for both the training and test datasets
# MAGIC 
# MAGIC The source data used in this notebook can be found at [EC Funded CAVIAR project/IST 2001 37540](http://homepages.inf.ed.ac.uk/rbf/CAVIAR/)
# MAGIC 
# MAGIC <img src="https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2FokOBL95oGT.png?alt=media&token=6ee848d4-a2f1-4231-9191-5a7b17ecb040" width=900/>
# MAGIC 
# MAGIC 
# MAGIC ### Cluster Configuration
# MAGIC * Suggested cluster configuration:
# MAGIC  * Databricks Runtime Version: `Databricks Runtime for ML` (e.g. 4.1 ML, 4.2 ML, etc.)
# MAGIC  * Driver: 64GB RAM Instance (e.g. `Azure: Standard_D16s_v3, AWS: r4.4xlarge`)
# MAGIC  * Workers: 2x 64GB RAM Instance (e.g. `Azure: Standard_D16s_v3, AWS: r4.4xlarge`)
# MAGIC  * Python: `Python 3`
# MAGIC  
# MAGIC ### Need to install manually
# MAGIC To install, refer to **Upload a Python PyPI package or Python Egg** [Databricks](https://docs.databricks.com/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) | [Azure Databricks](https://docs.azuredatabricks.net/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg)
# MAGIC 
# MAGIC * Python Libraries:
# MAGIC  * `opencv-python`: 3.4.2 
# MAGIC  
# MAGIC ### Libraries Already Included in Databricks Runtime for ML
# MAGIC Because we're using *Databricks Runtime for ML*, you do **not** need to install the following libraires
# MAGIC * Python Libraries:
# MAGIC  * `h5py`: 2.7.1
# MAGIC  * `tensorflow`: 1.7.1
# MAGIC  * `keras`: 2.1.5 (Using TensorFlow backend)
# MAGIC  * *You can check by `import tensorflow as tf; print(tf.__version__)`*
# MAGIC 
# MAGIC * JARs:
# MAGIC  * `spark-deep-learning-1.0.0-spark2.3-s_2.11.jar`
# MAGIC  * `tensorframes-0.3.0-s_2.11.jar`
# MAGIC  * *You can check by reviewing cluster's Spark UI > Environment)*

# COMMAND ----------

# DBTITLE 1,Include Video Configuration and Display Helper Functions
# MAGIC %run ./video_config

# COMMAND ----------

# DBTITLE 1,Directory of Training Videos
display(dbutils.fs.ls(srcVideoPath))

# COMMAND ----------

# DBTITLE 1,Example Video
# MAGIC %md
# MAGIC Here is an example video that we will perform our training on to identify suspicious behavior.
# MAGIC * The source of this data is from the [EC Funded CAVIAR project/IST 2001 37540](http://homepages.inf.ed.ac.uk/rbf/CAVIAR/).
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/09/Browse2.gif)

# COMMAND ----------

# DBTITLE 1,Process Videos - Extract Video Frames
# MAGIC %md
# MAGIC Extract JPG images from MPG videos using OpenCV (`cv2`)

# COMMAND ----------

# Extract and Save Images using CV2
def extractImagesSave(src, tgt):
  import cv2
  import uuid
  import re

  ## Extract one video frame per second and save frame as JPG
  def extractImages(pathIn):
      count = 0
      srcVideos = "/dbfs" + src + "(.*).mpg"
      p = re.compile(srcVideos)
      vidName = str(p.search(pathIn).group(1))
      vidcap = cv2.VideoCapture(pathIn)
      success,image = vidcap.read()
      success = True
      while success:
        try:
            vidcap.set(cv2.CAP_PROP_POS_MSEC,(count*1000))
            success,image = vidcap.read()
            print ('Read a new frame: ', success)
            cv2.imwrite("/dbfs" + tgt + vidName + "frame%04d.jpg" % count, image)     # save frame as JPEG file
            count = count + 1
            print ('Wrote a new frame')
        except:
            print("/dbfs" + tgt + vidName + "frame%04d.jpg" % count, image)
            print(vidcap.isOpened())
            print(count)
        
  ## Extract frames from all videos and save in s3 folder
  def createFUSEpaths(dbfsFilePath):
    return "/dbfs/" + dbfsFilePath[0][6:]
  
  # Build up fileList RDD
  fileList = dbutils.fs.ls(src)
  FUSEfileList = map(createFUSEpaths, fileList)
  FUSEfileList_rdd = sc.parallelize(FUSEfileList)
  
  # Ensure directory is created
  dbutils.fs.mkdirs(tgt)
  
  # Extract and save images
  FUSEfileList_rdd.map(extractImages).count()

  
# Remove Empty Files
def removeEmptyFiles(pathDir):
  import sys
  import os

  rootDir = '/dbfs' + pathDir
  for root, dirs, files in os.walk(rootDir):
    for f in files:
      fileName = os.path.join(root, f)
      if os.path.getsize(fileName) == 0:
        print ("empty fileName: %s \n" % fileName)
        os.remove(fileName)


# COMMAND ----------

# MAGIC %md # Training Dataset

# COMMAND ----------

# DBTITLE 1,Extract Training Images
# Extract Images
extractImagesSave(srcVideoPath, targetImgPath)

# Remove Empty Files
removeEmptyFiles(targetImgPath)

# View file list of images extracted from video
display(dbutils.fs.ls(targetImgPath))

# COMMAND ----------

# DBTITLE 1,Review Training Images
images = spark.read.format("binaryFile") \
  .option("pathGlobFilter", "*.jpg") \
  .option("recursiveFileLookup", "true") \
  .load(targetImgPath)

# COMMAND ----------

# DBTITLE 1,Feature Extraction with Transfer Learning
# MAGIC %md
# MAGIC 
# MAGIC Computing features using a pre-trained deep learning model, i.e. InceptionV3 

# COMMAND ----------

import pandas as pd
from PIL import Image
import numpy as np
import io

import tensorflow as tf
from tensorflow.keras.applications.inception_v3 import InceptionV3, preprocess_input
from tensorflow.keras.preprocessing.image import img_to_array

from pyspark.sql.functions import col, pandas_udf, PandasUDFType

model = InceptionV3(include_top=False)
model.summary()  # verify that the top layer is removed
bc_model_weights = sc.broadcast(model.get_weights())

def model_fn():
  """
  Returns a InceptionV3 model with top layer removed and broadcasted pretrained weights.
  """
  model = InceptionV3(weights=None, include_top=False)
  model.set_weights(bc_model_weights.value)
  return model

# COMMAND ----------

def preprocess(content):
  """
  Preprocesses raw image bytes for prediction.
  """
  img = Image.open(io.BytesIO(content)).resize([224, 224])
  arr = img_to_array(img)
  return preprocess_input(arr)

def featurize_series(model, content_series):
  """
  Featurize a pd.Series of raw images using the input model.
  :return: a pd.Series of image features
  """
  input = np.stack(content_series.map(preprocess))
  preds = model.predict(input)
  # For some layers, output features will be multi-dimensional tensors.
  # We flatten the feature tensors to vectors for easier storage in Spark DataFrames.
  output = [p.flatten() for p in preds]
  return pd.Series(output)


@pandas_udf('array<double>', PandasUDFType.SCALAR_ITER)
def featurize_udf(content_series_iter):
  '''
  This method is a Scalar Iterator pandas UDF wrapping our featurization function.
  The decorator specifies that this returns a Spark DataFrame column of type ArrayType(Double).
  
  :param content_series_iter: This argument is an iterator over batches of data, where each batch
                              is a pandas Series of image data.
  '''
  # With Scalar Iterator pandas UDFs, we can load the model once and then re-use it
  # for multiple data batches.  This amortizes the overhead of loading big models.
  model = model_fn()
  for content_series in content_series_iter:
    yield featurize_series(model, content_series)

from pyspark.ml.linalg import Vectors,VectorUDT
from pyspark.sql.functions import udf

def seq_as_vector(s):
  return Vectors.dense(s)
squared_udf = udf(seq_as_vector, VectorUDT())

# COMMAND ----------

# DBTITLE 1,Save Training Image Features
# saveImageFeatures(trainImages, imgFeaturesPath)
features_df = images.repartition(16).select(col("path"), featurize_udf("content").alias("features"))
features_df.withColumn("features",squared_udf("features"))
# features_df.write.mode("overwrite").parquet(imgFeaturesPath)
features_df.write.format('delta').mode("overwrite").save(imgFeaturesPath)

# COMMAND ----------

# View Parquet Features
display(dbutils.fs.ls(imgFeaturesPath))

# COMMAND ----------

# MAGIC %md # Test Dataset

# COMMAND ----------

# DBTITLE 1,Directory of Test Videos
display(dbutils.fs.ls(srcTestVideoPath))

# COMMAND ----------

# DBTITLE 1,Extract Test Images
# Extract Images
extractImagesSave(srcTestVideoPath, targetImgTestPath)

# Remove Empty Files
removeEmptyFiles(targetImgTestPath)

# View file list of images extracted from video
display(dbutils.fs.ls(targetImgTestPath))

# COMMAND ----------

# DBTITLE 1,Review Test Images
from pyspark.ml.image import ImageSchema

testImages = spark.read.format("binaryFile") \
  .option("pathGlobFilter", "*.jpg") \
  .option("recursiveFileLookup", "true") \
  .load(targetImgTestPath)
display(testImages)

# COMMAND ----------

# DBTITLE 1,Save Test Image Features
test_features_df = testImages.repartition(16).select(col("path"), featurize_udf("content").alias("features"))
test_features_df.withColumn("features",squared_udf("features"))
# test_features_df.write.mode("overwrite").parquet(imgFeaturesTestPath)
test_features_df.write.format('delta').mode("overwrite").save(imgFeaturesTestPath)

# COMMAND ----------

# View Parquet Features
display(dbutils.fs.ls(imgFeaturesTestPath))

# COMMAND ----------


