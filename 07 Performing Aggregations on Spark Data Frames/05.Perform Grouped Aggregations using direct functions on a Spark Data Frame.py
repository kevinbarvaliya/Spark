# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items_grouped = order_items. \
    groupBy('order_item_order_id')

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------

order_items_grouped. \
    count(). \
    show()

# COMMAND ----------

order_items_grouped. \
    count(). \
    withColumnRenamed('count', 'order_count'). \
    show()

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

# Get sum of all numeric fields
order_items_grouped. \
    sum(). \
    show()

# COMMAND ----------

orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# Get sum of all numeric fields
# Ignored order_date and order_status as they are not numeric fields
orders. \
    groupBy('order_date'). \
    sum(). \
    show()

# COMMAND ----------

order_items_grouped = order_items. \
    select('order_item_order_id', 'order_item_quantity', 'order_item_subtotal'). \
    groupBy('order_item_order_id')

# COMMAND ----------

help(order_items_grouped.sum)

# COMMAND ----------

# Gets sum on order_item_order_id as well
# It is not relevant and better to discard aggregation on key fields such as order_item_order_id
order_items_grouped. \
    sum(). \
    show()

# COMMAND ----------

# Consider only order_item_quantity and order_item_subtotal
order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    show()

# COMMAND ----------

order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    printSchema()

# COMMAND ----------

order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    toDF('order_item_order_id', 'order_quantity', 'order_revenue'). \
    printSchema()

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

# We can specify custom names to derived fields using toDF
# withColumn can be used to apply functions such as round on aggregated results
order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    toDF('order_item_order_id', 'order_quantity', 'order_revenue'). \
    withColumn('order_revenue', round('order_revenue', 2)). \
    show()

# COMMAND ----------

