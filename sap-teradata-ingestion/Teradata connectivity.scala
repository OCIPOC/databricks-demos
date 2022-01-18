// Databricks notebook source
// MAGIC %md
// MAGIC 1. Download Teradata JDBC driver: https://downloads.teradata.com/download/connectivity/jdbc-driver
// MAGIC 2. From workspace home page, got to Add LIbraries, upload both jar files.  

// COMMAND ----------

import import java.util.Properties

class.forName("com.teradata.jdbc.TeraDriver")
val url = "jdbc:teradata://xxx/database=xxx,CHARSET=UTF8,TMODE=TERA,TYPE=FASTEXPORT,RECONNECT_COUNT=50"

val connectionProperties = new Properties()
connectionProperties.put("user", s"dbc")
connectionProperties.put("password", s"dbc")

val driverClass = "com.teradata.jdbc.TeraDriver"
connectionProperties.setProperty("Driver", driverClass)

// Read table via fast export
val tdDF = spark.read.jdbc(jdbcUrl, "database.table", connectionProperties)

display(tdDF.select("*"))

// Save dataframe as delta table
tdDF.write.format("delta").mode("append").saveAsTable("tablename")


