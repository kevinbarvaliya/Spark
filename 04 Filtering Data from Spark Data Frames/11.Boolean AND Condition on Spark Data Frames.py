# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get Male Customers (gender is male and is_customer is equals to true)

# COMMAND ----------

users_df. \
    select('id', 'gender', 'is_customer'). \
    show()

# COMMAND ----------

users_df. \
    filter((col('gender') == 'male') & (col('is_customer') == True)). \
    select('id', 'gender', 'is_customer'). \
    show()

# COMMAND ----------

users_df. \
    filter("gender = 'male' AND is_customer = true"). \
    select('id', 'gender', 'is_customer'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the users who become customers between 2021 Jan 20th and 2021 Feb 15th.

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    show()

# COMMAND ----------

users_df. \
    filter((col('customer_from') >= '2021-01-20') & (col('customer_from') <= '2021-02-15')). \
    select('id', 'customer_from'). \
    show()

# COMMAND ----------

users_df. \
    filter("customer_from >= '2021-01-20' AND customer_from  <= '2021-02-15'"). \
    select('id', 'customer_from'). \
    show()

# COMMAND ----------

