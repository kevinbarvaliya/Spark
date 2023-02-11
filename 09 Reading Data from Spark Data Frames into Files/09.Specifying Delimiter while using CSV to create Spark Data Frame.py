# Databricks notebook source
schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

# Displays rows with all nulls as delimiter in each record is not comma
spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders').show()

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|').show()

# COMMAND ----------

spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|').count()

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep='|', schema=schema).show()

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep='|', schema=schema).dtypes

# COMMAND ----------

