# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC There are multiple ways to rename Spark Data Frame Columns or Expressions.
# MAGIC * We can rename column or expression using `alias` as part of `select`
# MAGIC * We can add or rename column or expression using `withColumn` on top of Data Frame.
# MAGIC * We can rename one column at a time using `withColumnRenamed` on top of Data Frame.
# MAGIC * We typically use `withColumn` to perform row level transformations and then to provide a name to the result. If we provide the same name as existing column, then the column will be replaced with new one.
# MAGIC * If we want to just rename the column then it is better to use `withColumnRenamed`.
# MAGIC * If we want to apply any transformation, we need to either use `select` or `withColumn`
# MAGIC * We can rename bunch of columns using `toDF`.

# COMMAND ----------

