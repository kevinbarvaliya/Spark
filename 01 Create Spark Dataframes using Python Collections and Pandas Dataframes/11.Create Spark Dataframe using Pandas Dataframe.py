# Databricks notebook source
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

df = spark.createDataFrame([Row(**row) for row in users])

# COMMAND ----------

df.printSchema

# COMMAND ----------

df.show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.DataFrame(users)

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(users)).show()

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

fields = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])

# COMMAND ----------

spark.createDataFrame(users, fields).show()

# COMMAND ----------

