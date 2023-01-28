# Databricks notebook source
# MAGIC %md
# MAGIC ## Pre-defined Functions
# MAGIC 
# MAGIC We typically process data in the columns using functions in `pyspark.sql.functions`. Let us understand details about these functions in detail as part of this module.

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us recap about Functions or APIs to process Data Frames.
# MAGIC  * Projection - `select` or `withColumn` or `drop` or `selectExpr`
# MAGIC  * Filtering - `filter` or `where`
# MAGIC  * Grouping data by key and perform aggregations - `groupBy`
# MAGIC  * Sorting data - `sort` or `orderBy` 
# MAGIC * We can pass column names or literals or expressions to all the Data Frame APIs.
# MAGIC * Expressions include arithmetic operations, transformations using functions from `pyspark.sql.functions`.
# MAGIC * There are approximately 300 functions under `pyspark.sql.functions`.
# MAGIC * We will talk about some of the important functions used for String Manipulation, Date Manipulation etc.

# COMMAND ----------

# MAGIC %md
# MAGIC * Here are some of the examples of using functions to take care of required transformations.

# COMMAND ----------

# Reading data

orders = spark.read.csv(
    '/public/retail_db/orders',
    schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

# Importing functions

from pyspark.sql.functions import date_format

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# Function as part of projections

orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month', date_format('order_date', 'yyyyMM')).show()

# COMMAND ----------

# Function as part of where or filter

orders. \
    filter(date_format('order_date', 'yyyyMM') == 201401). \
    show()

# COMMAND ----------

# Function as part of groupBy

orders. \
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
    count(). \
    show()

# COMMAND ----------

