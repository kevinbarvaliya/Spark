# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Filtering"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of customers (is_customer flag is set to true)

# COMMAND ----------

users_df.filter(col('is_customer') == True).show()

# COMMAND ----------

users_df.filter(col('is_customer') == 'true').show()

# COMMAND ----------

users_df.filter('is_customer = "true"').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql('''
    SELECT * FROM users
    WHERE is_customer = "true"
'''). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get list of non customers (is_customer flag is set to false)

# COMMAND ----------

users_df.filter(col('is_customer') == False).show()

# COMMAND ----------

users_df.filter(col('is_customer') == "False").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC * Get users from Dallas

# COMMAND ----------

users_df.filter(col('current_city') == 'Dallas').show()

# COMMAND ----------



# COMMAND ----------

users_df.filter("current_city == 'Dallas'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the customers who paid 900.0

# COMMAND ----------

users_df.filter(col('amount_paid') == '900.0').show()

# COMMAND ----------

users_df.filter('amount_paid == "900.0"').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the customers where paid amount is not a number

# COMMAND ----------

users_df.show()

# COMMAND ----------

from pyspark.sql.functions import isnan

# COMMAND ----------

users_df.select('amount_paid', isnan('amount_paid')).show()

# COMMAND ----------

users_df.filter(isnan('amount_paid') == True).show()

# COMMAND ----------

users_df.filter('isnan(amount_paid) = True').show()

# COMMAND ----------

