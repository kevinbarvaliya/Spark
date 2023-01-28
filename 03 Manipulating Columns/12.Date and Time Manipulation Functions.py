# Databricks notebook source
# MAGIC %md
# MAGIC ## Date and Time Manipulation Functions
# MAGIC Let us get started with Date and Time manipulation functions. As part of this topic we will focus on the date and timestamp format.

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use `current_date` to get today’s server date. 
# MAGIC   * Date will be returned using **yyyy-MM-dd** format.
# MAGIC * We can use `current_timestamp` to get current server time. 
# MAGIC   * Timestamp will be returned using **yyyy-MM-dd HH:mm:ss:SSS** format.
# MAGIC   * Hours will be by default in 24 hour format.

# COMMAND ----------

l = [("X", )]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

# COMMAND ----------

df.select(current_date()).show() #yyyy-MM-dd

# COMMAND ----------

df.select(current_timestamp()).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS

# COMMAND ----------

# MAGIC %md
# MAGIC * We can convert a string which contain date or timestamp in non-standard format to standard date or time using `to_date` or `to_timestamp` function respectively.

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, to_timestamp

# COMMAND ----------

df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()