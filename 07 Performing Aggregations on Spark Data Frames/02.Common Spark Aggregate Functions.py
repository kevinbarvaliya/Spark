# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Here are the common aggregate functions that are available as part of `pyspark.sql.functions`
# MAGIC * `count`
# MAGIC * `sum`
# MAGIC * `min`
# MAGIC * `max`
# MAGIC * `avg`
# MAGIC 
# MAGIC Spark also supports some statistical functions such as stddev.

# COMMAND ----------

orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

help(count)

# COMMAND ----------

orders.select(count("*")).show()

# COMMAND ----------

orders.groupBy('order_status').agg(count("*")).show()

# COMMAND ----------

