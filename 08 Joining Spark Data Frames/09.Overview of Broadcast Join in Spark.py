# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * It is also known as map side as well as replicated join.
# MAGIC * The smaller data set will be broadcasted to all the executors in the cluster.
# MAGIC * The size of the smaller data set is driven by `spark.sql.autoBroadcastJoinThreshold`.
# MAGIC * We can even perform broadcast join when the smaller data set size is greater than `spark.sql.autoBroadcastJoinThreshold` by using `broadcast` function from `pyspark.sql.functions`.
# MAGIC * We can disable broadcast join by setting `spark.sql.autoBroadcastJoinThreshold` value to 0.
# MAGIC * If broadcast join is disabled then it will result in reduce side join.
# MAGIC * Make sure to setup multinode cluster using 28 GB Memory, 4 Cores each. Configure scaling between 2 and 4 nodes. Driver can be of minimum configuration.

# COMMAND ----------

# Default size is 10 MB.
spark.conf.get('spark.sql.autoBroadcastJoinThreshold')

# COMMAND ----------

# We can disable broadcast join using this approach
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '0')

# COMMAND ----------

# Resetting to original value
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760b')

# COMMAND ----------

# 1+ GB Data Set
clickstream = spark.read.csv('dbfs:/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed/', sep='\t', header=True)

# COMMAND ----------

# 10+ GB Data Set
articles = spark.read.parquet('dbfs:/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet/')

# COMMAND ----------

# MAGIC %%time
# MAGIC 
# MAGIC # Default will be reduce side join as the size of smaller data set is more than 10 MB (default broadcast size)
# MAGIC clickstream.join(articles, articles.id == clickstream.curr_id).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Review SQL Plan visualization to confirm the previous query have used sort merge join.

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

# MAGIC %%time
# MAGIC # We can use broadcast function to override existing broadcast join threshold
# MAGIC # We can also override by using this code spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '1500m')
# MAGIC broadcast(clickstream).join(articles, articles.id == clickstream.curr_id).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Review SQL Plan visualization to confirm the previous query have used broadcast based join.

# COMMAND ----------

