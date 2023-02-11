# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * We can read the data from CSV files into Spark Data Frame using multiple approaches.
# MAGIC * Approach 1: `spark.read.csv('path_to_folder')`
# MAGIC * Approach 2: `spark.read.format('csv').load('path_to_folder')`
# MAGIC * We can explicitly specify the schema as `string` or using `StructType`.
# MAGIC * We can also read the data which is delimited or separated by other characters than comma.
# MAGIC * If the files have header we can create the Data Frame with schema by using options such as `header` and `inferSchema`. It will pick column names from the header while data types will be inferred based on the data.
# MAGIC * If the files does not have header we can create the Data Frame with schema by passing column names using `toDF` and by using `inferSchema` option.

# COMMAND ----------

# Default behavior
# It will delimit the data using comma as separator
# Column names will be system generated
# All the fields will be of type strings
orders = spark.read.csv('/public/retail_db/orders')

# COMMAND ----------

orders.columns

# COMMAND ----------

orders.dtypes

# COMMAND ----------

