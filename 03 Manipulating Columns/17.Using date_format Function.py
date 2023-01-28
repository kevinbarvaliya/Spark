# Databricks notebook source
# MAGIC %md
# MAGIC ## Using date_format Function
# MAGIC 
# MAGIC Let us understand how to extract information from dates or times using `date_format` function.

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC 
# MAGIC * We can use `date_format` to extract the required information in a desired format from standard date or timestamp. Earlier we have explored `to_date` and `to_timestamp` to convert non standard date or timestamp to standard ones respectively.
# MAGIC * There are also specific functions to extract year, month, day with in a week, a day with in a month, day with in a year etc. These are covered as part of earlier topics in this section or module.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks
# MAGIC 
# MAGIC Let us perform few tasks to extract the information we need from date or timestamp.
# MAGIC 
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

from pyspark.sql.functions import date_format

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the year and month from both date and time columns using `yyyyMM` format. Also make sure that the data type is converted to integer.

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

# yyyy
# MM
# dd
# DD
# HH
# hh
# mm
# ss
# SSS

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    printSchema()

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    printSchema()

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the information from time in yyyyMMddHHmmss format.

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss")). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss").cast('long')). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss").cast('long')). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get year and day of year using `yyyyDDD` format.

# COMMAND ----------

datetimesDF. \
    withColumn("date_yd", date_format("date", "yyyyDDD").cast('int')). \
    withColumn("time_yd", date_format("time", "yyyyDDD").cast('int')). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get complete description of the date.

# COMMAND ----------

datetimesDF. \
    withColumn("date_desc", date_format("date", "MMMM d, yyyy")). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get name of the week day using date.

# COMMAND ----------

datetimesDF. \
    withColumn("day_name_abbr", date_format("date", "EE")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("day_name_full", date_format("date", "EEEE")). \
    show(truncate=False)

# COMMAND ----------

