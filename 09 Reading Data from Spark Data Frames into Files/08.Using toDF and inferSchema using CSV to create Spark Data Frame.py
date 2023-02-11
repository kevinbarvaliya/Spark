# Databricks notebook source
columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']

# COMMAND ----------

type(columns)

# COMMAND ----------

spark.read.option('inferSchema', True).csv('/public/retail_db/orders').dtypes

# COMMAND ----------

help(spark.read.option('inferSchema', True).csv('/public/retail_db/orders').toDF)

# COMMAND ----------

spark.read.option('inferSchema', True).csv('/public/retail_db/orders').toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

spark.read.option('inferSchema', True).csv('/public/retail_db/orders').toDF(*columns)

# COMMAND ----------

spark.read.csv('/public/retail_db/orders', inferSchema=True).toDF(*columns)

# COMMAND ----------

spark.read.csv('/public/retail_db/orders', inferSchema=True).toDF(*columns).show()

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

