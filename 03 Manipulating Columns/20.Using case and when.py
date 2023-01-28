# Databricks notebook source
# MAGIC %md
# MAGIC ## Using CASE and WHEN
# MAGIC 
# MAGIC Let us understand how to perform conditional operations using `CASE` and `WHEN` in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC * `CASE` and `WHEN` is typically used to apply transformations based up on conditions. We can use `CASE` and `WHEN` similar to SQL using `expr` or `selectExpr`.
# MAGIC * If we want to use APIs, Spark provides functions such as `when` and `otherwise`. `when` is available as part of `pyspark.sql.functions`. On top of column type that is generated using `when` we should be able to invoke `otherwise`.

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 10,
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, None,
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, '',
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 10,
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us transform bonus to 0 in case of null or empty, otherwise return the bonus amount.

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# COMMAND ----------

employeesDF. \
    withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
    show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

employeesDF. \
    withColumn(
        'bonus', 
        expr("""
            CASE WHEN bonus IS NULL OR bonus = '' THEN 0
            ELSE bonus
            END
            """)
    ). \
    show()

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

when?

# COMMAND ----------

employeesDF. \
    withColumn(
        'bonus',
        when((col('bonus').isNull()) | (col('bonus') == lit('')), 0).otherwise(col('bonus'))
    ). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Create a dataframe using list called as persons and categorize them based up on following rules.
# MAGIC 
# MAGIC |Age range|Category|
# MAGIC |---------|--------|
# MAGIC |0 to 2 Months|New Born|
# MAGIC |2+ Months to 12 Months|Infant|
# MAGIC |12+ Months to 48 Months|Toddler|
# MAGIC |48+ Months to 144 Months|Kids|
# MAGIC |144+ Months|Teenager or Adult|

# COMMAND ----------

persons = [
    (1, 1),
    (2, 13),
    (3, 18),
    (4, 60),
    (5, 120),
    (6, 0),
    (7, 12),
    (8, 160)
]

# COMMAND ----------

personsDF = spark.createDataFrame(persons, schema='id INT, age INT')

# COMMAND ----------

personsDF.show()

# COMMAND ----------

personsDF. \
    withColumn(
        'category',
        expr("""
            CASE
            WHEN age BETWEEN 0 AND 2 THEN 'New Born'
            WHEN age > 2 AND age <= 12 THEN 'Infant'
            WHEN age > 12 AND age <= 48 THEN 'Toddler'
            WHEN age > 48 AND age <= 144 THEN 'Kid'
            ELSE 'Teenager or Adult'
            END
        """)
    ). \
    show()

# COMMAND ----------

personsDF. \
    withColumn(
        'category',
        when(col('age').between(0, 2), 'New Born').
        when((col('age') > 2) & (col('age') <= 12), 'Infant').
        when((col('age') > 12) & (col('age') <= 48), 'Toddler').
        when((col('age') > 48) & (col('age') <= 144), 'Kid').
        otherwise('Teenager or Adult')
    ). \
    show()

# COMMAND ----------

