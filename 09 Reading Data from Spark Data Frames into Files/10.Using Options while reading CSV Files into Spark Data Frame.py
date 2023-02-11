# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC We can pass the options using different ways while creating the Data Frame.
# MAGIC * Using key word arguments as part of APIs. We can use key word arguments as part of `load` as well as direct API (`csv`).
# MAGIC * `spark.read.option`
# MAGIC * `spark.read.options`
# MAGIC * If key in the option is incorrect then the options will be ignored.
# MAGIC 
# MAGIC Depending up on the API based on the file format the options as well as arguments vary.

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

# Default behavior
# It will delimit the data using comma as separator
# Column names will be system generated
# All the fields will be of type strings
orders = spark.read.csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

# schema, sep, quote, header, mode (to deal with corrupt records)
# inferSchema, ignoring spaces, null values, multiLine, etc
help(spark.read.csv)

# COMMAND ----------

orders = spark. \
    read. \
    csv(
        f'/user/{username}/retail_db_pipe/orders',
        sep='|',
        header=None,
        inferSchema=True
    ). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

help(spark.read.format('csv').load)

# COMMAND ----------

orders = spark. \
    read. \
    format('csv'). \
    load(
        f'/user/{username}/retail_db_pipe/orders',
        sep='|',
        header=None,
        inferSchema=True
    ). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

help(spark.read.option)

# COMMAND ----------

orders = spark. \
    read. \
    option('sep', '|'). \
    option('header', None). \
    option('inferSchema', True). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

help(spark.read.options)

# COMMAND ----------

orders = spark. \
    read. \
    options(sep='|', header=None, inferSchema=True). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

options = {
    'sep': '|',
    'header': None,
    'inferSchema': True
}

# COMMAND ----------

orders = spark. \
    read. \
    options(**options). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark. \
    read. \
    options(**options). \
    format('csv'). \
    load(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

