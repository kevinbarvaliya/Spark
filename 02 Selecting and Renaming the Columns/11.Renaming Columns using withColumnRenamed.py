# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Rename `id` to `user_id`
# MAGIC * Rename `first_name` to `user_first_name`
# MAGIC * Rename `last_name` to `user_last_name`

# COMMAND ----------

users_df\
    .select('id', 'first_name', 'last_name')\
    .withColumnRenamed('id','user_id')\
    .withColumnRenamed('first_name', 'user_first_name')\
    .withColumnRenamed('last_name', 'user_last_name')\
    .show()

# COMMAND ----------

