# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * If inferSchema is used entire data need to be read to infer the schema accurately while creating the Data Frame.
# MAGIC * If the data size is too big then additional time will be spent to infer the schema.
# MAGIC * When we explicitly specify the schema, data will not be read while creating the Data Frame.
# MAGIC * As we have seen we should be able to explicitly specify the schema using string or StructType.
# MAGIC * Inferring Schema will come handy to quickly understand the structure of the data as part of proof of concepts as well as design.
# MAGIC * Schema will be inferred by default for files of type JSON, Parquet and ORC. Column names and data types will be inferred using metadata that will be associated with these types of files.
# MAGIC * Inferring the schema on CSV files will create data frames with system generated column names. If inferSchema is used, then the data frame will determine the data types. If the files contain header, then column names can be inherited using it. If not, we need to explicitly pass the columns using `toDF`.

# COMMAND ----------

