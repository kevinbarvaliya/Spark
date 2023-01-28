# Databricks notebook source
# MAGIC %md
# MAGIC ## Categories of Functions
# MAGIC 
# MAGIC There are approximately 300 functions under `pyspark.sql.functions`. At a higher level they can be grouped into a few categories.

# COMMAND ----------

# MAGIC %md
# MAGIC * String Manipulation Functions
# MAGIC   * Case Conversion - `lower`,  `upper`
# MAGIC   * Getting Length -  `length`
# MAGIC   * Extracting substrings - `substring`, `split`
# MAGIC   * Trimming - `trim`, `ltrim`, `rtrim`
# MAGIC   * Padding - `lpad`, `rpad`
# MAGIC   * Concatenating string - `concat`, `concat_ws`
# MAGIC * Date Manipulation Functions
# MAGIC   * Getting current date and time - `current_date`, `current_timestamp`
# MAGIC   * Date Arithmetic - `date_add`, `date_sub`, `datediff`, `months_between`, `add_months`, `next_day`
# MAGIC   * Beginning and Ending Date or Time - `last_day`, `trunc`, `date_trunc`
# MAGIC   * Formatting Date - `date_format`
# MAGIC   * Extracting Information - `dayofyear`, `dayofmonth`, `dayofweek`, `year`, `month`
# MAGIC * Aggregate Functions
# MAGIC   * `count`, `countDistinct`
# MAGIC   * `sum`, `avg`
# MAGIC   * `min`, `max`
# MAGIC * Other Functions - We will explore depending on the use cases.
# MAGIC   * `CASE` and `WHEN`
# MAGIC   * `CAST` for type casting
# MAGIC   * Functions to manage special types such as `ARRAY`, `MAP`, `STRUCT` type columns
# MAGIC   * Many others

# COMMAND ----------

