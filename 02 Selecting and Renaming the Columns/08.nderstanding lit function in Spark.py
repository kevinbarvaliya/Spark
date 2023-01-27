# Databricks notebook source
# MAGIC  %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

users_df.select('id',lit(col('amount_paid'))+5).show()

# COMMAND ----------

users_df.select('id',col('amount_paid')+lit(5)).show()

# COMMAND ----------

users_df.selectExpr('id','amount_paid + 10').show()

# COMMAND ----------

users_df.createOrReplaceTempView("users")

# COMMAND ----------

spark.sql("Select id, amount_paid + 25 from users").show()

# COMMAND ----------

