# Databricks notebook source
# MAGIC %run "./01.Setup Data Sets to perform joins"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * `crossJoin` results in cartesian product.

# COMMAND ----------

help(courses_df.crossJoin)

# COMMAND ----------

users_df. \
    crossJoin(courses_df). \
    show()

# COMMAND ----------

# Number of records will be equal to 
# number of records in first data frame multipled by number of records in second data frame
users_df. \
    crossJoin(courses_df). \
    count()

# COMMAND ----------

# Even join with out conditions result in cross join or cartesian product
users_df. \
    join(courses_df). \
    show()

# COMMAND ----------

users_df. \
    join(courses_df). \
    count()

# COMMAND ----------

users_df. \
    join(courses_df, how='cross'). \
    show()

# COMMAND ----------

users_df. \
    join(courses_df, how='cross'). \
    count()