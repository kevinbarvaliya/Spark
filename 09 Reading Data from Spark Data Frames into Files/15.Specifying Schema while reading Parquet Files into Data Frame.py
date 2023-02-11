# Databricks notebook source
import getpass
username = getpass.getuser()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').dtypes

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

# This will run faster as data will not be read to infer the schema
# Fail to convert order_id as well as order_customer_id as int
spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = """
    order_id BIGINT,
    order_date TIMESTAMP,
    order_customer_id BIGINT,
    order_status STRING
"""

# COMMAND ----------

# Fail to type cast order_date to timestamp. In the files, it is represented as string
spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = """
    order_id BIGINT,
    order_date STRING,
    order_customer_id BIGINT,
    order_status STRING
"""

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders', schema=schema).show()

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# COMMAND ----------

schema = StructType([
    StructField('order_id', LongType()),
    StructField('order_date', StringType()),
    StructField('order_customer_id', LongType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

orders = spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# We can type cast fields such as order_date using cast function
orders. \
    withColumn('order_date', col('order_date').cast('timestamp')). \
    dtypes

# COMMAND ----------

orders. \
    withColumn('order_date', col('order_date').cast('timestamp')). \
    show()

# COMMAND ----------

