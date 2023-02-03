# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get id and email of users who are not customers or city contain empty string.

# COMMAND ----------

users_df. \
    select('id', 'email', 'current_city', 'is_customer'). \
    show()

# COMMAND ----------

users_df. \
    filter((col('current_city') == '') | (col('is_customer') == False)). \
    select('id', 'email', 'current_city', 'is_customer'). \
    show()

# COMMAND ----------

users_df. \
    filter("current_city = '' OR is_customer = false"). \
    select('id', 'email', 'current_city', 'is_customer'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get id and email of users who are not customers or customers whose last updated time is before 2021-03-01.

# COMMAND ----------

users_df. \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    show()

# COMMAND ----------

users_df. \
    filter((col('is_customer') == False) | (col('last_updated_ts') < '2021-03-01')). \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    show()

# COMMAND ----------

users_df. \
    filter("is_customer = false OR last_updated_ts < '2021-03-01'"). \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    show()

# COMMAND ----------

