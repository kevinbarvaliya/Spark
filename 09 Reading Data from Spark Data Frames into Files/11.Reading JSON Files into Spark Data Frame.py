# Databricks notebook source
help(spark.read.json)

# COMMAND ----------

# Schema will be inferred by default
df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df = spark.read.format('json').load('/public/retail_db_json/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

# Data type for order_id as well as order_customer_id is inferred as bigint
# Data type for order_date is inferred as string
df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------

