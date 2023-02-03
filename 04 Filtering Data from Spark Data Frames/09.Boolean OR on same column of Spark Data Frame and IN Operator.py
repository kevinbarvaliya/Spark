# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of users whose city is null or empty string (users with no cities associated)

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') == '') | (col('current_city').isNull())). \
    show()

# COMMAND ----------

# this will fail because conditions are not enclosed in circular brackets
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') == '' | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') == '') | col('current_city').isNull()). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city = '' OR current_city IS NULL"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of users whose city is either Houston or Dallas.

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    show()

# COMMAND ----------

# Not recommended, use in instead
users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') == 'Houston') | (col('current_city') == 'Dallas')). \
    show()

# COMMAND ----------

# Not recommended, use in instead
users_df. \
    select('id', 'current_city'). \
    filter("current_city = 'Houston' OR current_city = 'Dallas'"). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isin('Houston', 'Dallas')). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas')"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of users whose city is either Houston or Dallas or empty string. Use in operator.

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isin('Houston', 'Dallas', '')). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas', '')"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of users whose city is either Houston or Dallas or empty string or null.

# COMMAND ----------

# Passing null will not be effective
users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas', '', NULL)"). \
    show()

# COMMAND ----------

# Boolean OR including null check
users_df. \
    select('id', 'current_city'). \
    filter((col('current_city').isin('Houston', 'Dallas', '')) | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas', '') OR current_city IS NULL"). \
    show()

# COMMAND ----------

