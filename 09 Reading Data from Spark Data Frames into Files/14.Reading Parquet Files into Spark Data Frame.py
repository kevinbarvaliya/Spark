# Databricks notebook source
help(spark.read.parquet)

# COMMAND ----------

# Schema will be inferred by default
import getpass
username = getpass.getuser()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

# Data type for order_id as well as order_customer_id is inferred as bigint
# Data type for order_date is inferred as string
df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.format('parquet').load(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------

