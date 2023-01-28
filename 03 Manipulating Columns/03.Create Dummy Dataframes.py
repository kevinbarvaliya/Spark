# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Dummy Data Frame
# MAGIC Let us go ahead and create data frame using dummy data to explore Spark functions.

# COMMAND ----------

l = [('X', )]

# COMMAND ----------

# Oracle dual (view)
# dual - dummy CHAR(1)
# "X" - One record

# COMMAND ----------

df = spark.createDataFrame(l, "dummy STRING")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Once Data Frame is created, we can use to understand how to use functions. For example, to get current date, we can run `df.select(current_date()).show()`.
# MAGIC 
# MAGIC It is similar to Oracle Query `SELECT sysdate FROM dual`

# COMMAND ----------

l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")

from pyspark.sql.functions import current_date
df.select(current_date()). \
    show()

# COMMAND ----------

df.select(current_date().alias("current_date")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC Here is another example of creating Data Frame using collection of employees. We will be using this Data Frame to explore all the important functions to process column data in detail.

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, 
      "united states", "+1 123 456 7890", "123 45 6789"
    ),
     (2, "Henry", "Ford", 1250.0, 
      "India", "+91 234 567 8901", "456 78 9123"
     ),
     (3, "Nick", "Junior", 750.0, 
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
     ),
     (4, "Bill", "Gomes", 1500.0, 
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
     )
]

# COMMAND ----------

len(employees)

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.printSchema()

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, 
      "united states", "+1 123 456 7890", "123 45 6789"
    ),
     (2, "Henry", "Ford", 1250.0, 
      "India", "+91 234 567 8901", "456 78 9123"
     ),
     (3, "Nick", "Junior", 750.0, 
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
     ),
     (4, "Bill", "Gomes", 1500.0, 
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
     )
]

# COMMAND ----------



# COMMAND ----------

