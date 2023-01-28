# Databricks notebook source
# MAGIC %md
# MAGIC ## Using Date and Time Trunc Functions
# MAGIC In Data Warehousing we quite often run to date reports such as week to date, month to date, year to date etc. Let us understand how we can take care of such requirements using appropriate functions over Spark Data Frames.

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use `trunc` or `date_trunc` for the same to get the beginning date of the week, month, current year etc by passing date or timestamp to it.
# MAGIC * We can use `trunc` to get beginning date of the month or year by passing date or timestamp to it - for example `trunc(current_date(), "MM")` will give the first of the current month.
# MAGIC * We can use `date_trunc` to get beginning date of the month or year as well as beginning time of the day or hour by passing timestamp to it.
# MAGIC   * Get beginning date based on month - `date_trunc("MM", current_timestamp())`
# MAGIC   * Get beginning time based on day - `date_trunc("DAY", current_timestamp())`

# COMMAND ----------

from pyspark.sql.functions import trunc, date_trunc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tasks
# MAGIC 
# MAGIC Let us perform few tasks to understand trunc and date_trunc in detail.
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
# MAGIC * Get beginning month date using date field and beginning year date using time field.

# COMMAND ----------

from pyspark.sql.functions import trunc

# COMMAND ----------

datetimesDF. \
    withColumn("date_trunc", trunc("date", "MM")). \
    withColumn("time_trunc", trunc("time", "yy")). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get beginning hour time using date and time field.

# COMMAND ----------

from pyspark.sql.functions import date_trunc

# COMMAND ----------

datetimesDF. \
    withColumn("date_trunc", date_trunc('MM', "date")). \
    withColumn("time_trunc", date_trunc('yy', "time")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)

# COMMAND ----------

