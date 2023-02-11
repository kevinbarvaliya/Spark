# Databricks notebook source
spark.read.json('/public/retail_db_json/orders').show()

# COMMAND ----------

spark.read.json('/public/retail_db_json/orders').dtypes

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

# This will run faster as data will not be read to infer the schema
spark.read.schema(schema).json('/public/retail_db_json/orders').show()

# COMMAND ----------

spark.read.json('/public/retail_db_json/orders', schema=schema).show()

# COMMAND ----------

# Able to type cast order_id and order_customer_id to int
# Able to type case order_date to string
spark.read.json('/public/retail_db_json/orders', schema=schema).dtypes

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', TimestampType()),
    StructField('order_customer_id', IntegerType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

type(schema)

# COMMAND ----------

spark.read.schema(schema).json('/public/retail_db_json/orders').show()

# COMMAND ----------

spark.read.json('/public/retail_db_json/orders', schema=schema).show()

# COMMAND ----------

