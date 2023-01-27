# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Rename `id` to `user_id`
# MAGIC * Rename `first_name` to `user_first_name`
# MAGIC * Rename `last_name` to `user_last_name`
# MAGIC * Also add new column by name `user_full_name` which is derived by concatenating `first_name` and `last_name` with `, ` in between.

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

users_df\
    .select(col('id').alias('user_id'), 
            col('first_name').alias('user_first_name'), 
            col('last_name').alias('user_last_name'))\
    .withColumn('user_full_name', concat('user_first_name', lit(' '), 'user_last_name'))\
    .show()

# COMMAND ----------

users_df\
    .select(
            users_df['id'].alias('user_id'), 
            users_df['first_name'].alias('user_first_name'), 
            users_df['last_name'].alias('user_last_name'))\
    .withColumn('user_full_name', concat('user_first_name', lit(' '), 'user_last_name'))\
    .show()

# COMMAND ----------

users_df\
    .select(
            users_df['id'].alias('user_id'), 
            users_df['first_name'].alias('user_first_name'), 
            users_df['last_name'].alias('user_last_name'))\
    .withColumn('user_full_name', concat('user_first_name', lit(' '), 'user_last_name'))\
    .show()