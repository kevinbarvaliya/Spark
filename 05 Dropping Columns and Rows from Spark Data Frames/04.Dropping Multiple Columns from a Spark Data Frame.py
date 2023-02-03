# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

users_df.drop('first_name', 'last_name').printSchema()

# COMMAND ----------

users_df.drop('first_name', 'last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# This will fail as we are passing multiple column objects
# When we want to pass more than one column, we have to pass all column names as strings
users_df.drop(col('first_name'), col('id')).printSchema()

# COMMAND ----------

# If we have column name which does not exist, the column will be ignored
users_df.drop('user_id', 'first_name', 'last_name').printSchema()

# COMMAND ----------

users_df.drop('user_id', 'first_name', 'last_name').show()

# COMMAND ----------

