# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview of Narrow and Wide Transformations
# MAGIC 
# MAGIC Let us get an overview of Narrow and Wide Transformations.
# MAGIC * Here are the functions related to narrow transformations. Narrow transformations doesn't result in shuffling. These are also known as row level transformations.
# MAGIC   * `df.select`
# MAGIC   * `df.filter`
# MAGIC   * `df.withColumn`
# MAGIC   * `df.withColumnRenamed`
# MAGIC   * `df.drop`
# MAGIC * Here are the functions related to wide transformations.
# MAGIC   * `df.distinct`
# MAGIC   * `df.union` or any set operation
# MAGIC   * `df.join` or any join operation
# MAGIC   * `df.groupBy`
# MAGIC   * `df.sort` or `df.orderBy`
# MAGIC * Any function that result in shuffling is wide transformation. For all the wide transformations, we have to deal with group of records based on a key.

# COMMAND ----------

