# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in descending order by **first_name**

# COMMAND ----------

users_df.sort('first_name', ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'], ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'].desc()).show()

# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

users_df.sort(desc('first_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in descending order by **customer_from**.

# COMMAND ----------

users_df.sort(users_df['customer_from'].desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Sort the data in descending order by **number of enrolled courses**.

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df. \
    select('id', 'courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort('no_of_courses', ascending=False). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort(desc('no_of_courses')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df. \
    select('id', 'courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort(col('no_of_courses').desc()). \
    show()

# COMMAND ----------

