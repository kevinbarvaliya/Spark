# Databricks notebook source
# MAGIC %md
# MAGIC ## Date and Time Arithmetic
# MAGIC Let us perform Date and Time Arithmetic using relevant functions over Spark Data Frames.

# COMMAND ----------

# MAGIC %md
# MAGIC * Adding days to a date or timestamp - `date_add`
# MAGIC * Subtracting days from a date or timestamp - `date_sub`
# MAGIC * Getting difference between 2 dates or timestamps - `datediff`
# MAGIC * Getting the number of months between 2 dates or timestamps - `months_between`
# MAGIC * Adding months to a date or timestamp - `add_months`
# MAGIC * Getting next day from a given date - `next_day`
# MAGIC * All the functions are self explanatory. We can apply these on standard date or timestamp. All the functions return date even when applied on timestamp field.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tasks
# MAGIC 
# MAGIC Let us perform some tasks related to date arithmetic.
# MAGIC * Get help on each and every function first and understand what all arguments need to be passed.
# MAGIC * Create a Dataframe by name datetimesDF with columns date and time.

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Add 10 days to both date and time values.
# MAGIC * Subtract 10 days from both date and time values.

# COMMAND ----------

from pyspark.sql.functions import date_add, date_sub

# COMMAND ----------

help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

datetimesDF. \
    withColumn("date_add_date", date_add("date", 10)). \
    withColumn("date_add_time", date_add("time", 10)). \
    withColumn("date_sub_date", date_sub("date", 10)). \
    withColumn("date_sub_time", date_sub("time", 10)). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the difference between current_date and date values as well as current_timestamp and time values.

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp, datediff

# COMMAND ----------

datetimesDF. \
    withColumn("datediff_date", datediff(current_date(), "date")). \
    withColumn("datediff_time", datediff(current_timestamp(), "time")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the number of months between current_date and date values as well as current_timestamp and time values.
# MAGIC * Add 3 months to both date values as well as time values.

# COMMAND ----------

from pyspark.sql.functions import months_between, add_months, round

# COMMAND ----------

datetimesDF. \
    withColumn("months_between_date", round(months_between(current_date(), "date"), 2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"), 2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)

# COMMAND ----------

