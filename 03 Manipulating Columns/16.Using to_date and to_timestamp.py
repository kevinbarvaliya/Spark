# Databricks notebook source
# MAGIC %md
# MAGIC ## Using to_date and to_timestamp
# MAGIC 
# MAGIC Let us understand how to convert non standard dates and timestamps to standard dates and timestamps.

# COMMAND ----------

# MAGIC %md
# MAGIC * `yyyy-MM-dd` is the standard date format
# MAGIC * `yyyy-MM-dd HH:mm:ss.SSS` is the standard timestamp format
# MAGIC * Most of the date manipulation functions expect date and time using standard format. However, we might not have data in the expected standard format.
# MAGIC * In those scenarios we can use `to_date` and `to_timestamp` to convert non standard dates and timestamps to standard ones respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks
# MAGIC 
# MAGIC Let us perform few tasks to extract the information we need from date or timestamp.
# MAGIC 
# MAGIC * Create a Dataframe by name datetimesDF with columns date and time.

# COMMAND ----------

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes, schema="date BIGINT, time STRING")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lit, to_date

# COMMAND ----------

l = [("X", )]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(to_date(lit('20210302'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

# year and day of year to standard date
df.select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02/03/2021'), 'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-03-2021'), 'dd-MM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-March-2021'), 'dd-MMMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('March 2, 2021'), 'MMMM d, yyyy').alias('to_date')).show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us convert data in datetimesDF to standard dates or timestamps

# COMMAND ----------

datetimesDF.printSchema()

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp

# COMMAND ----------

datetimesDF. \
    withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd')). \
    withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')). \
    show(truncate=False)

# COMMAND ----------

