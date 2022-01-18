# Databricks notebook source
displayHTML("""<iframe src="https://docs.google.com/presentation/d/e/2PACX-1vQEwYRzqXxhyChj68LgOPKVYXS3Kq7DpX7fl_18VHN9AqKWEAkdjH83k9eVYpRRclDcLr4bEqB66m85/embed?start=false&loop=false&delayms=60000" frameborder="0" width="960" height="569" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fdata-and-ai%2FNXlBmG6icz.png?alt=media&token=adcadb9e-f5e0-48f9-9e9c-e27d8db2c13e)

# COMMAND ----------

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sap://20.212.186.225:39041") \
    .option("driver","com.sap.db.jdbc.Driver") \
    .option("dbtable", "BIKE_STATIONS") \
    .option("user", "SYSTEM") \
    .option("password", "SAPhxe123") \
    .load()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bike_stations USING DELTA LOCATION '/mnt/tardis6/bike_stations'

# COMMAND ----------

jdbcDF.write.format('delta').mode('overwrite').save(path="/mnt/tardis6/bike_stations")
