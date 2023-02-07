# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the **users** data in ascending order by **first_name**

# COMMAND ----------

users_df.sort('first_name').show()

# COMMAND ----------

users_df.sort(users_df.first_name).show()

# COMMAND ----------

users_df.sort(users_df['first_name']).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.sort(col('first_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the **users** data in ascending order by **customer_from**.

# COMMAND ----------

users_df.sort(col('customer_from')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the **data** in ascending order by **number of enrolled courses**.

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df. \
    select('id', 'courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort(size('courses')).show()

# COMMAND ----------

