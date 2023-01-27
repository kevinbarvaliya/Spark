# Databricks notebook source
# MAGIC  %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Concatenate `first_name` and `last_name` to generate `full_name`

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

users_df.select(col('id'),concat(col('first_name'), lit('_'), col('last_name')).alias("full_name")).show()

# COMMAND ----------

users_df.selectExpr('id',"concat(first_name, '_', last_name) as full_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Convert data type of customer_from date to numeric type

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------


users_df.select(col('id'),
               date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')).show()


# COMMAND ----------


users_df.selectExpr('id',"date_format('customer_from', 'yyyyMMdd') as customer_from").show()


# COMMAND ----------

