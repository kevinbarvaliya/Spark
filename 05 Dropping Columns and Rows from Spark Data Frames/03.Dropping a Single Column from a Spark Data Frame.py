# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.drop('last_updated_ts').printSchema()

# COMMAND ----------

users_df.drop(users_df['last_updated_ts']).printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.drop(col('last_updated_ts')).printSchema()

# COMMAND ----------

# If we have column name which does not exist, the column will be ignored
users_df.drop(col('user_id')).printSchema()

# COMMAND ----------

