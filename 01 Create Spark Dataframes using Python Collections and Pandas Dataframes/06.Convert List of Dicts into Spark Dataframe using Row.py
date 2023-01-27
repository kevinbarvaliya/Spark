# Databricks notebook source
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]

# COMMAND ----------

spark.createDataFrame(users_list).show()

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

for row in users_list:
    print(row.values())

# COMMAND ----------

user_row = [Row(*row.values()) for row in users_list] 

# COMMAND ----------

spark.createDataFrame(user_row,'id int, name string').show()

# COMMAND ----------

user_row1 = [Row(**row) for row in users_list] 
spark.createDataFrame(user_row1,'id int, name string').show()

# COMMAND ----------

