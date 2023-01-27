# Databricks notebook source
names = [(1,"kevin"),(2,"vishal"),(3,"dharm"), (4,"JD")]
df = spark.createDataFrame(names,"id int, name string")
df.show()

# COMMAND ----------

from pyspark.sql import Row


# COMMAND ----------

user_row = [Row(*row) for row in names] 

# COMMAND ----------

user_row

# COMMAND ----------

display(spark.createDataFrame(user_row, "id int, name string"))

# COMMAND ----------

