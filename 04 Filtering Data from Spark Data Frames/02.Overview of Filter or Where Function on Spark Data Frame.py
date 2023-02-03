# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Filtering"

# COMMAND ----------

help(users_df.filter)

# COMMAND ----------

users_df.where?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * `where` and `filter` are synonyms
# MAGIC * We can pass conditions either by using SQL Style or Non SQL Style.
# MAGIC * For Non SQL Style we can pass columns using `col` function on column name as string or using the notation of`df['column_name']`

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.filter(col('id') == 1).show()

# COMMAND ----------

users_df.where(col('id') == 1).show()

# COMMAND ----------

users_df.filter(users_df['id'] == 1).show()

# COMMAND ----------

users_df.where(users_df['id'] == 1).show()

# COMMAND ----------

# 'id == 1' also works
users_df.filter('id = 1').show()

# COMMAND ----------

users_df.where('id = 1').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    select * from users where id =2
""").show()

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM users
    WHERE id = 1
"""). \
    show()

# COMMAND ----------



# COMMAND ----------

