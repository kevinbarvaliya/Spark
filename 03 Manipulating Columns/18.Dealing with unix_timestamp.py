# Databricks notebook source
# MAGIC %md
# MAGIC ## Dealing with Unix Timestamp
# MAGIC 
# MAGIC Let us understand how to deal with Unix Timestamp in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC * It is an integer and started from January 1st 1970 Midnight UTC.
# MAGIC * Beginning time is also known as epoch and is incremented by 1 every second.
# MAGIC * We can convert Unix Timestamp to regular date or timestamp and vice versa.
# MAGIC * We can use `unix_timestamp` to convert regular date or timestamp to a unix timestamp value. For example `unix_timestamp(lit("2019-11-19 00:00:00"))`
# MAGIC * We can use `from_unixtime` to convert unix timestamp to regular date or timestamp. For example `from_unixtime(lit(1574101800))`
# MAGIC * We can also pass format to both the functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks
# MAGIC 
# MAGIC Let us perform few tasks to understand how to deal with Unix Timestamp.
# MAGIC 
# MAGIC *   Create a Dataframe by name datetimesDF with columns dateid, date and time.

# COMMAND ----------

datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get unix timestamp for dateid, date and time.

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

# COMMAND ----------

datetimesDF. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    withColumn("unix_time", unix_timestamp("time")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Create a Dataframe by name unixtimesDF with one column unixtime using 4 values. You can use the unix timestamp generated for time column in previous task.

# COMMAND ----------

unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]

# COMMAND ----------

unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")

# COMMAND ----------

unixtimesDF.show()

# COMMAND ----------

unixtimesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Get date in yyyyMMdd format and also complete timestamp.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

# COMMAND ----------

unixtimesDF. \
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")). \
    withColumn("time", from_unixtime("unixtime")). \
    show()
#yyyyMMdd

# COMMAND ----------

