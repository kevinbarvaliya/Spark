# Databricks notebook source
# MAGIC %md
# MAGIC ## Trimming Characters from Strings
# MAGIC Let us go through how to trim unwanted characters using Spark Functions.

# COMMAND ----------

# MAGIC %md
# MAGIC * We typically use trimming to remove unnecessary characters from fixed length records.
# MAGIC * Fixed length records are extensively used in Mainframes and we might have to process it using Spark.
# MAGIC * As part of processing we might want to remove leading or trailing characters such as 0 in case of numeric types and space or some standard character in case of alphanumeric types.
# MAGIC * As of now Spark trim functions take the column as argument and remove leading or trailing spaces. However, we can use `expr` or `selectExpr` to use Spark SQL based trim functions to remove leading or trailing spaces or any other such characters.
# MAGIC   * Trim spaces towards left - `ltrim`
# MAGIC   * Trim spaces towards right - `rtrim`
# MAGIC   * Trim spaces on both sides - `trim`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - Trimming Strings
# MAGIC 
# MAGIC Let us understand how to use trim functions to remove spaces on left or right or both.
# MAGIC * Create a Dataframe with one column and one record.
# MAGIC * Apply trim functions to trim spaces.

# COMMAND ----------

l = [("   Hello.    ",) ]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import col, ltrim, rtrim, trim

# COMMAND ----------

df.withColumn("ltrim", ltrim(col("dummy"))). \
  withColumn("rtrim", rtrim(col("dummy"))). \
  withColumn("trim", trim(col("dummy"))). \
  show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

# if we do not specify trimStr, it will be defaulted to space
df.withColumn("ltrim", expr("ltrim(dummy)")). \
  withColumn("rtrim", expr("rtrim('.', rtrim(dummy))")). \
  withColumn("trim", trim(col("dummy"))). \
  show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)")). \
  withColumn("rtrim", expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
  withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")). \
  show()

# COMMAND ----------

