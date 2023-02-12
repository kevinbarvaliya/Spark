# Databricks notebook source
dbutils.fs.ls('/public/retail_db')

# COMMAND ----------

df = spark.read.csv('/public/retail_db/orders')

# COMMAND ----------

df.show()

# COMMAND ----------

# Copy all the data with comma separator to pipe separator

import getpass
username = getpass.getuser()

input_dir = '/public/retail_db'
output_dir = f'/user/{username}/retail_db_pipe'

# COMMAND ----------

help(df.write.csv)

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/orders', sep='|')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep='|').show()

# COMMAND ----------

dbutils.fs.ls(input_dir)

# COMMAND ----------

# Generate CSV files with pipe delimiter
for file_details in dbutils.fs.ls(input_dir):
    print(f'Converting data in {file_details.path} folder from comma separated to pipe separated')
    df = spark.read.csv(file_details.path)
    folder_name = file_details.path.split('/')[-2]
    df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}', sep='|')

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|')

# COMMAND ----------

orders.show()

# COMMAND ----------

