# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users who are not living in Dallas.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') != 'Dallas'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') != 'Dallas') | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city != 'Dallas' OR current_city IS NULL"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users whose city name is not empty string. Nulls can be ignored.

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') != ''). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city != ''"). \
    show()

# COMMAND ----------

