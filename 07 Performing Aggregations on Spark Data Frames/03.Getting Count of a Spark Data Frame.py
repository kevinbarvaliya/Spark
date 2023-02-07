# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get number of records in **order_items**.

# COMMAND ----------

# Function count on data frame is action. It will trigger execution.
order_items.count()

# COMMAND ----------

type(order_items.count())

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

order_items.select(count("*"))

# COMMAND ----------

# count is transformation (wide).
# Execution will be triggered when we perform actions such as show
order_items.select(count("*")).show()

# COMMAND ----------

