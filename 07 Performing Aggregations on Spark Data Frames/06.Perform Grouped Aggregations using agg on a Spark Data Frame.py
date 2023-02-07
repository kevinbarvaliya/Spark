# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items_grouped = order_items. \
    groupBy('order_item_order_id')

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------

order_items_grouped. \
    sum('order_item_quantity', 'order_item_subtotal'). \
    show()

# COMMAND ----------

help(order_items_grouped.agg)

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

order_items_grouped. \
    agg(sum('order_item_quantity'), sum('order_item_subtotal')). \
    printSchema()

# COMMAND ----------

order_items_grouped. \
    agg(sum('order_item_quantity'), sum('order_item_subtotal')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

order_items_grouped. \
    agg(sum('order_item_quantity').alias('order_quanity'), round(sum('order_item_subtotal'), 2).alias('order_revenue')). \
    printSchema()

# COMMAND ----------

order_items_grouped. \
    agg(sum('order_item_quantity').alias('order_quanity'), round(sum('order_item_subtotal'), 2).alias('order_revenue')). \
    show()

# COMMAND ----------

order_items_grouped. \
    agg({'order_item_quantity': 'sum', 'order_item_subtotal': 'sum'}). \
    printSchema()

# COMMAND ----------

order_items_grouped. \
    agg({'order_item_quantity': 'sum', 'order_item_subtotal': 'sum'}). \
    toDF('order_item_order_id', 'order_revenue', 'order_quantity'). \
    withColumn('order_revenue', round('order_revenue', 2)). \
    printSchema()

# COMMAND ----------

order_items_grouped. \
    agg({'order_item_quantity': 'sum', 'order_item_subtotal': 'sum'}). \
    toDF('order_item_order_id', 'order_revenue', 'order_quantity'). \
    withColumn('order_revenue', round('order_revenue', 2)). \
    show()

# COMMAND ----------

order_items_grouped. \
    agg({'order_item_quantity': 'sum', 'order_item_quantity': 'min'}). \
    show()

# COMMAND ----------

from pyspark.sql.functions import min

# COMMAND ----------

order_items_grouped. \
    agg(
        sum('order_item_quantity').alias('order_quanity'), 
        min('order_item_quantity').alias('min_order_quantity'),
        round(sum('order_item_subtotal'), 2).alias('order_revenue'),
        min('order_item_subtotal').alias('min_order_item_subtotal')
    ). \
    show()

# COMMAND ----------

