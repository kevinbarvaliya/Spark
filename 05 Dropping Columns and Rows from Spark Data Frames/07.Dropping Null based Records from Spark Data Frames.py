# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Here are the features that are available with respect to dropping records based on null values.
# MAGIC * Drop records when all column values are nulls.
# MAGIC * Drop records any of the column value is null.
# MAGIC * Drop records that have less than `thresh` non-null values.
# MAGIC * Drop records when any of the column value or all column values are nulls for provided subset of columns.
# MAGIC * We can use `df.na.drop` or `df.dropna` to take care of dealing with records having columns with null values.

# COMMAND ----------

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
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
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
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 5,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.count()

# COMMAND ----------

# Attribute which exposes functions dealing with null records
# drop => users_df.dropna
# fill => users_df.fillna
# replace => users_df.replacena
users_df.na

# COMMAND ----------

help(users_df.dropna)

# COMMAND ----------

help(users_df.na.drop)

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.na.drop('all').show()

# COMMAND ----------

# Drop if any column value is null
users_df.na.drop('any').show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.na.drop(thresh=2).show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.na.drop(how='all', subset=['id', 'email']).show()

# COMMAND ----------

users_df.na.drop(how='any', subset=['id', 'email']).show()

# COMMAND ----------

