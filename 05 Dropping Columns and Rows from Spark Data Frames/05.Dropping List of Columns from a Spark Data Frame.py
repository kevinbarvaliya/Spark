# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

pii_columns = ['first_name', 'last_name', 'email', 'phone_numbers']

# COMMAND ----------

# This will fail
# We need to convert list to varrying arguments to get it working
users_df_nopii = users_df.drop(pii_columns)

# COMMAND ----------

users_df_nopii = users_df.drop(*pii_columns)

# COMMAND ----------

users_df_nopii.printSchema()

# COMMAND ----------

users_df_nopii.show()

# COMMAND ----------

pii_columns = ['first_name', 'last_name', 'email', 'phone_numbers', 'ssn_doesnot_exist']

# COMMAND ----------

users_df_nopii = users_df.drop(*pii_columns)

# COMMAND ----------

users_df_nopii.printSchema()

# COMMAND ----------

users_df_nopii.show()

# COMMAND ----------

