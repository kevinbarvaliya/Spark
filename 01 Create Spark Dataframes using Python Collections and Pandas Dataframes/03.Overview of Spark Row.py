# Databricks notebook source
names = [(1,"kevin"),(2,"vishal"),(3,"dharm"), (4,"JD")]
df = spark.createDataFrame(names,"id int, name string")
df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

for i, j in enumerate(df.collect()):
    print(i,j)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

Row(id=1, name='kevin', contry='India')

# COMMAND ----------

