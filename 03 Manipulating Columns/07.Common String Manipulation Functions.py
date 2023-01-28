# Databricks notebook source
# MAGIC %md
# MAGIC ## Common String Manipulation Functions
# MAGIC Let us go through some of the common string manipulation functions using pyspark as part of this topic.

# COMMAND ----------

# MAGIC %md
# MAGIC * Concatenating strings
# MAGIC   * We can pass a variable number of strings to `concat` function.
# MAGIC   * It will return one string concatenating all the strings.
# MAGIC   * If we have to concatenate literal in between then we have to use `lit` function.
# MAGIC * Case Conversion and Length
# MAGIC   * Convert all the alphabetic characters in a string to **uppercase** - `upper`
# MAGIC   * Convert all the alphabetic characters in a string to **lowercase** - `lower`
# MAGIC   * Convert first character in a string to **uppercase** - `initcap`
# MAGIC   * Get **number of characters in a string** - `length`
# MAGIC   * All the 4 functions take column type argument.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - Concatenating Strings
# MAGIC 
# MAGIC Let us perform few tasks to understand more about 
# MAGIC `concat` function.
# MAGIC * Let’s create a Data Frame and explore `concat` function.

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
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

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Create a new column by name **full_name** concatenating **first_name** and **last_name**.

# COMMAND ----------

from pyspark.sql.functions import concat
employeesDF. \
    withColumn("full_name", concat("first_name", "last_name")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Improvise by adding a **comma followed by a space** in between **first_name** and **last_name**.

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# COMMAND ----------

employeesDF. \
    withColumn("full_name", concat("first_name", lit(", "), "last_name")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - Case Conversion and length
# MAGIC 
# MAGIC Let us perform tasks to understand the behavior of case conversion functions and length.
# MAGIC 
# MAGIC * Use employees data and create a Data Frame.
# MAGIC * Apply all 4 functions on **nationality** and see the results.

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
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

employeesDF = spark.createDataFrame(employees). \
    toDF("employee_id", "first_name",
         "last_name", "salary",
         "nationality", "phone_number",
         "ssn"
        )

# COMMAND ----------

from pyspark.sql.functions import col, lower, upper, initcap, length

# COMMAND ----------

employeesDF. \
  select("employee_id", "nationality"). \
  withColumn("nationality_upper", upper(col("nationality"))). \
  withColumn("nationality_lower", lower(col("nationality"))). \
  withColumn("nationality_initcap", initcap(col("nationality"))). \
  withColumn("nationality_length", length(col("nationality"))). \
  show()

# COMMAND ----------

