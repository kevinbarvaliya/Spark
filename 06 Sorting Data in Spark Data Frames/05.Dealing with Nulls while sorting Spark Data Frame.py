# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in ascending order by **customer_from** (default).

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    orderBy('customer_from'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in ascending order by **customer_from** with null values coming at the end.

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    orderBy(users_df['customer_from'].asc_nulls_last()). \
    show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    orderBy(col('customer_from').asc_nulls_last()). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in descending order by **customer_from** (default).

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    orderBy(users_df['customer_from'].desc()). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in descending order by **customer_from** with null values coming at the beginning.

# COMMAND ----------

users_df. \
    select('id', 'customer_from'). \
    orderBy(users_df['customer_from'].desc_nulls_first()). \
    show()

# COMMAND ----------

