# Databricks notebook source
import getpass
username = getpass.getuser()

# COMMAND ----------

input_dir = '/public/retail_db_json'
output_dir = f'/user/{username}/retail_db_parquet'

# COMMAND ----------

for file_details in dbutils.fs.ls(input_dir):
    if not ('.git' in file_details.path or file_details.path.endswith('sql')):
        print(f'Converting data in {file_details.path} folder from json to parquet')
        data_set_dir = file_details.path.split('/')[-2]
        df = spark.read.json(file_details.path)
        df.coalesce(1).write.parquet(f'{output_dir}/{data_set_dir}', mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.show()

# COMMAND ----------

# Copy all the data with comma separator to pipe separator

import getpass
username = getpass.getuser()

input_dir = '/public/retail_db'
output_dir = f'/user/{username}/retail_db_pipe'

# COMMAND ----------

# Generate CSV files with pipe delimiter
for file_details in dbutils.fs.ls(input_dir):
    if 'git' not in file_details.path and 'sql' not in file_details.path:
        print(f'Converting data in {file_details.path} folder from comma separated to pipe separated')
        df = spark.read.csv(file_details.path)
        folder_name = file_details.path.split('/')[-2]
        df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}', sep='|')