# Databricks notebook source
# MAGIC %md
# MAGIC ## Padding Characters around Strings
# MAGIC Let us go through how to pad characters to strings using Spark Functions.

# COMMAND ----------

# MAGIC %md
# MAGIC * We typically pad characters to build fixed length values or records.
# MAGIC * Fixed length values or records are extensively used in Mainframes based systems.
# MAGIC * Length of each and every field in fixed length records is predetermined and if the value of the field is less than the predetermined length then we pad with a standard character.
# MAGIC * In terms of numeric fields we pad with zero on the leading or left side. For non numeric fields, we pad with some standard character on leading or trailing side.
# MAGIC * We use `lpad` to pad a string with a specific character on leading or left side and `rpad` to pad on trailing or right side.
# MAGIC * Both lpad and rpad, take 3 arguments - column or expression, desired length and the character need to be padded.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - Padding Strings
# MAGIC 
# MAGIC Let us perform simple tasks to understand the syntax of `lpad` or `rpad`.
# MAGIC * Create a Dataframe with single value and single column.
# MAGIC * Apply `lpad` to pad with - to Hello to make it 10 characters.

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

from pyspark.sql.functions import lit, lpad

# COMMAND ----------

df.select(lpad(lit("Hello"), 10, "-").alias("dummy")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Let’s create the **employees** Dataframe

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

employeesDF.show()

# COMMAND ----------

employeesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Use **pad** functions to convert each of the field into fixed length and concatenate. Here are the details for each of the fields.
# MAGIC   * Length of the employee_id should be 5 characters and should be padded with zero.
# MAGIC   * Length of first_name and last_name should be 10 characters and should be padded with - on the right side.
# MAGIC   * Length of salary should be 10 characters and should be padded with zero.
# MAGIC   * Length of the nationality should be 15 characters and should be padded with - on the right side.
# MAGIC   * Length of the phone_number should be 17 characters and should be padded with - on the right side.
# MAGIC   * Length of the ssn can be left as is. It is 11 characters. 
# MAGIC * Create a new Dataframe **empFixedDF** with column name **employee**. Preview the data by disabling truncate.

# COMMAND ----------

from pyspark.sql.functions import lpad, rpad, concat

# COMMAND ----------

empFixedDF = employeesDF.select(
    concat(
        lpad("employee_id", 5, "0"), 
        rpad("first_name", 10, "-"), 
        rpad("last_name", 10, "-"),
        lpad("salary", 10, "0"), 
        rpad("nationality", 15, "-"), 
        rpad("phone_number", 17, "-"), 
        "ssn"
    ).alias("employee")
)

# COMMAND ----------

empFixedDF.show(truncate=False)

# COMMAND ----------

