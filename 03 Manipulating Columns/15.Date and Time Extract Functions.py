# Databricks notebook source
# MAGIC %md
# MAGIC ## Date and Time Extract Functions
# MAGIC Let us get an overview about Date and Time extract functions. Here are the extract functions that are useful which are self explanatory.

# COMMAND ----------

# MAGIC %md
# MAGIC * `year`
# MAGIC * `month`
# MAGIC * `weekofyear`
# MAGIC * `dayofyear`
# MAGIC * `dayofmonth`
# MAGIC * `dayofweek`
# MAGIC * `hour`
# MAGIC * `minute`
# MAGIC * `second`
# MAGIC 
# MAGIC There might be few more functions. You can review based up on your requirements.

# COMMAND ----------

l = [("X", )]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import year, month, weekofyear, dayofmonth, \
    dayofyear, dayofweek, current_date

# COMMAND ----------

df.select(
    current_date().alias('current_date'), 
    year(current_date()).alias('year'),
    month(current_date()).alias('month'),
    weekofyear(current_date()).alias('weekofyear'),
    dayofyear(current_date()).alias('dayofyear'),
    dayofmonth(current_date()).alias('dayofmonth'),
    dayofweek(current_date()).alias('dayofweek')
).show() #yyyy-MM-dd

# COMMAND ----------

help(dayofweek)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, hour, minute, second

# COMMAND ----------

from pyspark.sql import functions

# COMMAND ----------

help(functions)

# COMMAND ----------

df.select(
    current_timestamp().alias('current_timestamp'), 
    year(current_timestamp()).alias('year'),
    month(current_timestamp()).alias('month'),
    dayofmonth(current_timestamp()).alias('dayofmonth'),
    hour(current_timestamp()).alias('hour'),
    minute(current_timestamp()).alias('minute'),
    second(current_timestamp()).alias('second')
).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS

# COMMAND ----------

