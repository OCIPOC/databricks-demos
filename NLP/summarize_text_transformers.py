# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Summarize text with Transformers pre-trained models
# MAGIC authors:
# MAGIC - Peyman Mohajerian
# MAGIC - Stuart Lynn
# MAGIC tags:
# MAGIC - python
# MAGIC - machine-learning
# MAGIC - bert
# MAGIC - huggingfaces
# MAGIC - nlp
# MAGIC - gpu
# MAGIC - pytorch
# MAGIC - transfer-learning
# MAGIC - t5
# MAGIC - transformers
# MAGIC - pandas-udf
# MAGIC - summarization
# MAGIC created_at: 2020-07-28
# MAGIC updated_at: 2020-07-28
# MAGIC tldr: "Summarisation of large text corpus using pre-trained models"
# MAGIC thumbnail: https://huggingface.co/landing/assets/transformers-docs/huggingface_logo.svg
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/7712125](https://demo.cloud.databricks.com/#notebook/7712125)

# COMMAND ----------

# MAGIC %pip install rouge transformers

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Summarize text with Transformers pre-trained models
# MAGIC <div style="margin:auto;display:block;float:right"><img src="https://huggingface.co/landing/assets/transformers-docs/huggingface_logo.svg"></div>
# MAGIC __What is Transformers?__
# MAGIC 
# MAGIC From the [huggingface repository documentation][1]:
# MAGIC 
# MAGIC _Transformers (formerly known as pytorch-transformers and pytorch-pretrained-bert) provides general-purpose architectures (BERT, GPT-2, RoBERTa, XLM, DistilBert, XLNet…) for Natural Language Understanding (NLU) and Natural Language Generation (NLG) with over 32+ pretrained models in 100+ languages and deep interoperability between TensorFlow 2.0 and PyTorch._
# MAGIC 
# MAGIC Transformers gives us access to a number of state-of-the-art pretrained models with standardised pipelines for the most common NLP tasks, including:
# MAGIC * Sequence classification
# MAGIC * Question answering
# MAGIC * Text summarization (demonstrated in this notebook)
# MAGIC * Translation
# MAGIC * Named entity recognition
# MAGIC 
# MAGIC __In this notebook, we will:__
# MAGIC 1. Create a Spark Dataframe containing paths to news articles sourced from CNN;
# MAGIC 2. Create a custom PyTorch Dataset to wrap and preprocess this data;
# MAGIC 3. Instantiate a custom tokenization and summarization pipeline using a pretrained text-to-text-transfer-transformer ("T5") model;
# MAGIC 4. Distribute the task of summarizing the corpus across a cluster using a Spark Pandas UDF; and
# MAGIC 5. Assess the quality of our summaries against some ground truth using the [ROUGE metric][2].
# MAGIC 
# MAGIC [1]: https://huggingface.co/transformers/index.html
# MAGIC [2]: https://en.wikipedia.org/wiki/ROUGE_(metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corpus for analysis
# MAGIC Let's start by examining the raw data. It has been provided in a format of raw story with 'highlights' appended and decorated accordingly. We'll use these as our 'ground truth' for evaluating our summaries.

# COMMAND ----------

# MAGIC %sh
# MAGIC tar -xvzf /dbfs/Users/ivan.tang@databricks.com/cnn_stories.tgz -C /dbfs/Users/ivan.tang@databricks.com/

# COMMAND ----------

import os
import rouge
import torch
import numpy as np
import pandas as pd
from collections import deque
from tqdm import tqdm
from transformers import pipeline, AutoModelForSeq2SeqLM, AutoTokenizer
from torch.utils.data import DataLoader, Dataset
from pyspark.sql.functions import pandas_udf, PandasUDFType, spark_partition_id
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-processing
# MAGIC Simple source and ground-truth extraction functions.

# COMMAND ----------

def process_story(raw_story):
  """ Extract the story and summary from a story file.
  Arguments:
      raw_story (str): content of the story file as an utf-8 encoded string.
  Raises:
      IndexError: If the story is empty or contains no highlights.
  """
  nonempty_lines = list(filter(lambda x: len(x) != 0, [line.strip() for line in raw_story.split("\n")]))

  # for some unknown reason some lines miss a period, add it
  nonempty_lines = [_add_missing_period(line) for line in nonempty_lines]

  # gather article lines
  story_lines = []
  lines = deque(nonempty_lines)
  while True:
    try:
      element = lines.popleft()
      if element.startswith("@highlight"):
        break
      story_lines.append(element)
    except IndexError:
      # if "@highlight" is absent from the file we pop
      # all elements until there is None, raising an exception.
      return story_lines, []

  # gather summary lines
  summary_lines = list(filter(lambda t: not t.startswith("@highlight"), lines))

  return story_lines, summary_lines


def _add_missing_period(line):
  END_TOKENS = [".", "!", "?", "...", "'", "`", '"', "\u2019", "\u2019", ")"]
  if line.startswith("@highlight"):
    return line
  if line[-1] in END_TOKENS:
    return line
  return line + "."

# COMMAND ----------

# MAGIC %md
# MAGIC PyTorch custom Dataset class used to incrementally load files batch-at-a-time for model inference.

# COMMAND ----------

class StoryDataset(Dataset):
  def __init__(self, paths):
    self.paths = paths
  def __len__(self):
    return len(self.paths)
  def __getitem__(self, index):
    with open(self.paths[index], encoding="utf-8") as source:
      raw_story = source.read()
      story_lines, summary_lines = process_story(raw_story)
    return " ".join(story_lines), " ".join(summary_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference
# MAGIC Here we define a Pandas UDF (of 'GROUPED_MAP' type) that receives a Pandas DataFrame (containing file paths) and outputs a second DataFrame with ground truth and hypothesised summaries and evaluation metrics.

# COMMAND ----------

@pandas_udf("path string, reference string, hypothesis string, f_score float, precision float, recall float", PandasUDFType.GROUPED_MAP)
def summarize(paths_pdf):

  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  print(f"device for inference: {device}")

  summarizer = pipeline(
    task="summarization", 
    model=AutoModelForSeq2SeqLM.from_pretrained("t5-base"), 
    tokenizer=AutoTokenizer.from_pretrained("t5-base"),
    device=torch.cuda.current_device() if device.type == "cuda" else -1
  )
  
  rouge_evaluator = rouge.Rouge(metrics=["rouge-l"])
  
  stories = StoryDataset(paths_pdf["path"].values)
  loader = DataLoader(dataset=stories, batch_size=3)
  
  all_hypotheses, all_references = [], []
  for source_batch, reference_batch in tqdm(loader):
    hypotheses = summarizer(list(source_batch))
    all_hypotheses += [hyp["summary_text"] for hyp in hypotheses]
    all_references += reference_batch
    
  metrics = rouge_evaluator.get_scores(all_hypotheses, all_references)
  return_pdf = pd.DataFrame([m["rouge-l"] for m in metrics])
  return_pdf.columns = ["f_score", "precision", "recall"]
  
  return_pdf["path"] = paths_pdf["path"]
  return_pdf["reference"] = all_references
  return_pdf["hypothesis"] = all_hypotheses
  
  return return_pdf
  

# COMMAND ----------

# MAGIC %md
# MAGIC Catalogue the files, count and transfer these paths to a Spark Dataframe.

# COMMAND ----------

dataset_dir = "/dbfs/Users/ivan.tang@databricks.com/cnn/stories/"
files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(dataset_dir) for f in filenames if os.path.splitext(f)[1] == '.story']
len(files)

# COMMAND ----------

stories_sdf = spark.createDataFrame(map(lambda path: (path,), files), ["path"])
display(stories_sdf.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the inference pipeline on a subset of the corpus. Write results and examine metrics.

# COMMAND ----------

n_partitions = 4 # controls parallelism (best make this a small multiple of nodes in the cluster)
dataset_fraction = 0.0001 # use this to test the scalability of the inference process

spark.conf.set("spark.sql.shuffle.partitions", f"{n_partitions}")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

sample_fracs = dict([(i, dataset_fraction) for i in range(n_partitions)])
sample_sdf = stories_sdf.repartition(n_partitions).sampleBy(spark_partition_id(), sample_fracs)

(
  sample_sdf
  .groupBy(spark_partition_id())
  .apply(summarize)
  .write.format("delta")
  .mode("overwrite")
  .save("/tmp/KnowledgeRepo/transformers-summarization")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC Let's see what output we obtained from the model and plot the f-scores obtained by comparing these outputs to our ground truth.

# COMMAND ----------

summarized_stories = spark.read.format("delta").load("/tmp/KnowledgeRepo/transformers-summarization")
display(summarized_stories)

# COMMAND ----------

display(summarized_stories)
