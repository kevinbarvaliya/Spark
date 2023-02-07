# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC To perform total aggregations we can also leverage `agg` function.

# COMMAND ----------

order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   * Get revenue using `order_item_subtotal` for a given `order_item_order_id` (eg: 2)

# COMMAND ----------

order_items.filter('order_item_order_id = 2')

# COMMAND ----------

order_items.filter('order_item_order_id = 2').show()

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

help(sum)

# COMMAND ----------

help(order_items.agg)

# COMMAND ----------

help(order_items.groupBy('order_item_order_id').agg)

# COMMAND ----------

order_items.filter('order_item_order_id = 2').agg(sum('order_item_subtotal')).show()

# COMMAND ----------

order_items.filter('order_item_order_id = 2').agg(sum('order_item_subtotal').alias('order_revenue')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get number of items, total quantity as well as revenue for a given order item order id (eg: 2)
# MAGIC   * Number of items can be computed using `count` on `order_item_quantity`.
# MAGIC   * Total quantity can be computed using `sum` on `order_item_quantity`.
# MAGIC   * Total Revenue can be computed using `sum` on `order_item_subtotal`.

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

order_items. \
    filter('order_item_order_id = 2'). \
    show()

# COMMAND ----------

order_items. \
    filter('order_item_order_id = 2'). \
    agg(
        count('order_item_quantity').alias('order_item_count'),
        sum('order_item_quantity').alias('order_quantity'),
        sum('order_item_subtotal').alias('order_revenue')
    ). \
    show()

# COMMAND ----------

# We can only perform one aggregation per one column using this approach
order_items. \
    filter('order_item_order_id = 2'). \
    agg(
        {'order_item_quantity': 'count', 'order_item_subtotal': 'sum'}
    ). \
    toDF('order_item_count', 'order_revenue'). \
    show()

# COMMAND ----------

