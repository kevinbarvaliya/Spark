# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('id','first_name').show()

# COMMAND ----------

from pyspark.sql.functions import lit, col, concat

# COMMAND ----------



# COMMAND ----------

users_df.select(col('id'),"first_name",'last_name',  concat(col('first_name'), lit("_"), col('last_name')).alias("Full_name")).show()

# COMMAND ----------

users_df.selectExpr('id',"first_name",'last_name', "concat(first_name, '_', last_name) as full_name").show()


# COMMAND ----------

 users_df.createOrReplaceTempView("users")

# COMMAND ----------

spark.sql("""
select * from users
""").show()

# COMMAND ----------

  