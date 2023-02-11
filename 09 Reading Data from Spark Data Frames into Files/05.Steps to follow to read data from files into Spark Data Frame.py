# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * Check if the files are compressed (gz, snappy, bz2, etc). Most common ones are gz and snappy.
# MAGIC * Understand the file format (text, json, avro, parquet, orc, etc). Sometimes files will not have extensions.
# MAGIC * If files does not have extensions, make sure to confirm the details by going through the tech spec or by opening the file.
# MAGIC * We will get tech specs from our leads or architects while working on real world projects.
# MAGIC * If the files are of text file format, check if the data is delimited or separated by a specific character.
# MAGIC * Use appropriate API under `spark.read` to read the data.

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

spark.read.text('/public/retail_db/orders').show(truncate=False)

# COMMAND ----------

# MAGIC %fs ls /public/retail_db_json/orders

# COMMAND ----------

spark.read.json('/public/retail_db_json/orders').show()

# COMMAND ----------

# MAGIC %fs ls /user/root/retail_db_parquet/orders

# COMMAND ----------

