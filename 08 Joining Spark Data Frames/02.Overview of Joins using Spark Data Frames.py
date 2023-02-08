# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * Inner Join - **join** or **inner join**
# MAGIC * **Left** or **Right Outer Join**
# MAGIC * Full Outer Join - **a left outer join b** union **a right outer join b**
# MAGIC * Cross Join
# MAGIC * Spark Data Frames have a function called `join`. It can be used to perform inner or outer or full outer join.
# MAGIC * We need to specify **join condition** for Inner or Outer or Full Outer Join. 

# COMMAND ----------

# MAGIC %run "./02 Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

help(courses_df.crossJoin)

# COMMAND ----------

