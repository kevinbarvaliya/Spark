# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users whose city is not null

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isNotNull()). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter('current_city IS NOT NULL'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users whose city is null

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isNull()). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter('current_city IS NULL'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users where customer_from is null

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    filter(col('customer_from').isNull()). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    filter('customer_from IS NULL'). \
    show()

# COMMAND ----------

