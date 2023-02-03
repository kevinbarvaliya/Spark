# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get user id and email whose last updated timestamp is between 2021 Feb 15th and 2021 March 15th.

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

c = col('last_updated_ts')

# COMMAND ----------

help(c.between)

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter(col('last_updated_ts').between('2021-02-15 00:00:00', '2021-03-15 23:59:59')). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter("last_updated_ts BETWEEN '2021-02-15 00:00:00' AND '2021-03-15 23:59:59'"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the users whose payment is in the range of 850 and 900.

# COMMAND ----------

users_df. \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'amount_paid'). \
    filter(col('amount_paid').between(850, 900)). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'amount_paid'). \
    filter('amount_paid BETWEEN "850" AND "900"'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'amount_paid'). \
    filter('amount_paid BETWEEN "850" AND "1000"'). \
    show()

# COMMAND ----------

