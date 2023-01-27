# Databricks notebook source
age_list = [21,43,54,76,32,23]

# COMMAND ----------

type(age_list)

# COMMAND ----------

spark

# COMMAND ----------

spark.createDataFrame(age_list, "int")

# COMMAND ----------

from pyspark.sql.types import IntegerType
spark.createDataFrame(age_list, IntegerType())

# COMMAND ----------

name= ["kevin", "vishal", "dharm"]

# COMMAND ----------

display(spark.createDataFrame(name,  "string"))

# COMMAND ----------

from pyspark.sql.types import StringType
display(spark.createDataFrame(name, StringType(),))

# COMMAND ----------

