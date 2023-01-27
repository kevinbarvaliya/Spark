# Databricks notebook source
id = [(32,),(43,),(56,),(34,),(23,)]

# COMMAND ----------

display(spark.createDataFrame(id, 'age int'))

# COMMAND ----------

names = [(1,"kevin"),(2,"vishal"),(3,"dharm")]
display(spark.createDataFrame(names,"id int, name string"))

# COMMAND ----------

