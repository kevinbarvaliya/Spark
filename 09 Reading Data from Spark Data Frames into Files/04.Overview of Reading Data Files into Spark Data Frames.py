# Databricks notebook source
spark

# COMMAND ----------

type(spark)

# COMMAND ----------

type(spark.read)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Reading files using direct APIs such as `csv`, `json`, etc under `spark.read`.
# MAGIC * Reading files using `format` and `load` under `spark.read`.
# MAGIC * Specifying options as arguments as well as using functions such as `option` and `options`.
# MAGIC * Supported file formats.
# MAGIC   * `csv`
# MAGIC   * `text`
# MAGIC   * `json`
# MAGIC   * `parquet`
# MAGIC   * `orc`
# MAGIC * Other common file formats.
# MAGIC   * `xml`
# MAGIC   * `avro`
# MAGIC * Important file formats for certification - `csv`, `json`, `parquet`
# MAGIC * Reading compressed files

# COMMAND ----------

