# Databricks notebook source
# MAGIC %run "./01.Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here are the sorting scenarios which we should be familiar with.
# MAGIC * Sort a data frame using ascending order by a specific column.
# MAGIC * Sort a data frame using descending order by a specific column.
# MAGIC * Dealing with nulls while sorting the data (having the null values at the beginning or at the end).
# MAGIC * Sort a data frame using multiple columns (composite sorting). We also need to be aware of how to sort the data in ascending order by first column and then descending order by second column as well as vice versa.
# MAGIC * We also need to make sure how to perform prioritized sorting. For example, let's say we want to get USA at the top and rest of the countries in ascending order by their respective names.

# COMMAND ----------

help(users_df.sort)

# COMMAND ----------

help(users_df.orderBy)

# COMMAND ----------

