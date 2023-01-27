# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# required columns from original list
c = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the data from required columns and rename the columns to new names as per target column names.
# MAGIC * We should be able to use `select` to get the data from required columns.
# MAGIC * We should be able to rename the columns using `toDF`
# MAGIC * `select` and `toDF` takes variable number of arguments. We can use `*required_columns` while invoking `select` to get the data from required columns. It is applicable for `toDF` as well.

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

users_df\
    .select(required_columns)\
    .toDF(*target_column_names).show()

# COMMAND ----------

