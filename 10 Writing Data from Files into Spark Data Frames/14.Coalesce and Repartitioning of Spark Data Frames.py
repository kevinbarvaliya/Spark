# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * `coalesce` and `repartition` are functions on top of the dataframe. Do not get confused between **coalesce** on Data Frame and the coalesce function available to deal with null values in a given column.
# MAGIC * `coalesce` is typically used to **reduce number of partitions** to deal with as part of downstream processing. 
# MAGIC * `repartition` is used to reshuffle the data to **higher or lower number of partitions** to deal with as part of downstream partitioning.
# MAGIC * Make sure to use a cluster with higher configuration, if you would like to run and experience by your self.
# MAGIC   * 2 to 3 worker nodes using Standard with 14 to 16 GB RAM and 4 cores each.

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True)

# COMMAND ----------

help(df.coalesce)

# COMMAND ----------

help(df.repartition)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * `repartition` incurs **shuffling** and it takes time as data has to be shuffled to newer number of partitions.
# MAGIC * Also you can `repartition` the Data Frame based on specified columns.
# MAGIC * `coalesce` does not incur shuffling.
# MAGIC * We use `coalesce` quite often before writing the data to fewer number of files.

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True, inferSchema=True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/asa/airlines')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# coalescing the Dataframe to 16
df.coalesce(16).rdd.getNumPartitions()

# COMMAND ----------

# not effective as coalesce can be used to reduce the number of partitioning.
# Faster as no shuffling is involved
df.coalesce(186).rdd.getNumPartitions()

# COMMAND ----------

# incurs shuffling
# Watch the execution time and compare with coalesce
df.repartition(16).rdd.getNumPartitions()

# COMMAND ----------

# repartitioned to higher number of partitions
df.repartition(186, 'Year', 'Month').rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC One of the common usage of `coalesce` is to write to lesser number of files.

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------


dbutils.fs.rm(f'/user/{username}/airlines', recurse=True)

# COMMAND ----------

df.write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------

df.repartition(16).write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------

# If you use repartition it will take longer time than this.
df.coalesce(16).write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------

