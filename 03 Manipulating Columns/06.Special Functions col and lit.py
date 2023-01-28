# Databricks notebook source
# MAGIC %md
# MAGIC ## Special Functions - col and lit
# MAGIC 
# MAGIC Let us understand special functions such as col and lit. These functions are typically used to convert the strings to column type.

# COMMAND ----------

# MAGIC %md
# MAGIC * First let us create Data Frame for demo purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC Let us start spark context for this Notebook so that we can execute the code provided. You can sign up for our [10 node state of the art cluster/labs](https://labs.itversity.com/plans) to learn Spark SQL using our unique integrated LMS.

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

# MAGIC %md
# MAGIC * For Data Frame APIs such as `select`, `groupBy`, `orderBy` etc we can pass column names as strings.

# COMMAND ----------

employeesDF. \
    select("first_name", "last_name"). \
    show()

# COMMAND ----------

employeesDF. \
    groupBy("nationality"). \
    count(). \
    show()

# COMMAND ----------

employeesDF. \
    orderBy("employee_id"). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * If there are no transformations on any column in any function then we should be able to pass all column names as strings.
# MAGIC * If not we need to pass all columns as type column by using col function.
# MAGIC * If we want to apply transformations using some of the functions then passing column names as strings will not suffice. We have to pass them as column type.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

employeesDF. \
    select(col("first_name"), col("last_name")). \
    show()

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

employeesDF. \
    select(upper("first_name"), upper("last_name")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * `col` is the function which will convert column name from string type to **Column** type. We can also refer column names as **Column** type using Data Frame name.

# COMMAND ----------

from pyspark.sql.functions import col, upper

# COMMAND ----------

employeesDF. \
    select(upper(col("first_name")), upper(col("last_name"))). \
    show()

# COMMAND ----------

employeesDF. \
    groupBy(upper(col("nationality"))). \
    count(). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Also, if we want to use functions such as `alias`, `desc` etc on columns then we have to pass the column names as column type (not as strings).

# COMMAND ----------

# This will fail as the function desc is available only on column type.
employeesDF. \
    orderBy("employee_id".desc()). \
    show()

# COMMAND ----------

# We can invoke desc on columns which are of type column
employeesDF. \
    orderBy(col("employee_id").desc()). \
    show()

# COMMAND ----------

employeesDF. \
    orderBy(col("first_name").desc()). \
    show()

# COMMAND ----------

# Alternative - we can also refer column names using Data Frame like this
employeesDF. \
    orderBy(upper(employeesDF['first_name']).alias('first_name')). \
    show()

# COMMAND ----------

# Alternative - we can also refer column names using Data Frame like this
employeesDF. \
    orderBy(upper(employeesDF.first_name).alias('first_name')). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Sometimes, we want to add a literal to the column values. For example, we might want to concatenate first_name and last_name separated by comma and space in between.

# COMMAND ----------

from pyspark.sql.functions import concat

# COMMAND ----------

# Same as above
employeesDF. \
    select(concat(col("first_name"), ", ", col("last_name"))). \
    show()

# COMMAND ----------

# Referring columns using Data Frame
employeesDF. \
    select(concat(employeesDF["first_name"], ", ", employeesDF["last_name"])). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * If we pass the literals directly in the form of string or numeric type, then it will fail. It will search for column by name using the string passed. In this example, it will check for column by name `, ` (comma followed by space).
# MAGIC * We have to convert literals to column type by using `lit` function.

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

employeesDF. \
    select(concat(col("first_name"), 
                  lit(", "), 
                  col("last_name")
                 ).alias("full_name")
          ). \
    show(truncate=False)

# COMMAND ----------

