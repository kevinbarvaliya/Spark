# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * `>`
# MAGIC * `<`
# MAGIC * `>=` (equivalent to boolean with `col1 > val1 or cal1 = val1`)
# MAGIC * `<=` (equivalent to boolean with `col1 < val1 or cal1 = val1`)

# COMMAND ----------

from pyspark.sql.functions import col, isnan

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get Customers who paid greater than 900

# COMMAND ----------

users_df. \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    filter((col('amount_paid') > 900) & (isnan(col('amount_paid')) == False)). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    filter('amount_paid > 900 AND isnan(amount_paid) = false'). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get Customers who paid less than 900

# COMMAND ----------

users_df. \
    filter((col('amount_paid') < 900) & (isnan(col('amount_paid')) == False)). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    filter('amount_paid < 900 AND isnan(amount_paid) = false'). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get Customers who paid greater than or equals to 900

# COMMAND ----------

users_df. \
    filter((col('amount_paid') >= 900) & (isnan(col('amount_paid')) == False)). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    filter('amount_paid >= 900 AND isnan(amount_paid) = false'). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get Customers who paid less than or equals to 900

# COMMAND ----------

users_df. \
    filter((col('amount_paid') <= 900) & (isnan(col('amount_paid')) == False)). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    filter('amount_paid <= 900 AND isnan(amount_paid) = false'). \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the users who became customers after **2021-01-21**

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    filter(col('customer_from') > '2021-01-21'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    filter('customer_from > "2021-01-21"'). \
    show()

# COMMAND ----------

