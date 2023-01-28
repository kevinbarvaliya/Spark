# Databricks notebook source
# MAGIC %md
# MAGIC ## Extracting Strings using substring
# MAGIC Let us understand how to extract strings from main string using `substring` function in Pyspark.

# COMMAND ----------

# MAGIC %md
# MAGIC * If we are processing **fixed length columns** then we use `substring` to extract the information.
# MAGIC * Here are some of the examples for **fixed length columns** and the use cases for which we typically extract information..
# MAGIC * 9 Digit Social Security Number. We typically extract last 4 digits and provide it to the tele verification applications..
# MAGIC * 16 Digit Credit Card Number. We typically use first 4 digit number to identify Credit Card Provider and last 4 digits for the purpose of tele verification.
# MAGIC * Data coming from MainFrames systems are quite often fixed length. We might have to extract the information and store in multiple columns.
# MAGIC * `substring` function takes 3 arguments, **column**, **position**, **length**. We can also provide position from the end by passing negative value.

# COMMAND ----------

# MAGIC %md
# MAGIC * Here is how we typically take care of getting substring from the main string using Python. We pass index and length to extract the substring.

# COMMAND ----------

s = "Hello World"

# COMMAND ----------

# Extracts first 5 characters from the string
s[:5]

# COMMAND ----------

# Extracts characters from 2nd to 4th (3 characters). 
# Second argument is length of the string that need to be considered.
s[1:4]

# COMMAND ----------

l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l, "dummy STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use `substring` function to extract substring from main string using Pyspark.

# COMMAND ----------

from pyspark.sql.functions import substring, lit

# COMMAND ----------

# Function takes 3 arguments
# First argument is a column from which we want to extract substring.
# Second argument is the character from which string is supposed to be extracted.
# Third argument is number of characters from the first argument.
df.select(substring(lit("Hello World"), 7, 5)). \
  show()

# COMMAND ----------

df.select(substring(lit("Hello World"), -5, 5)). \
  show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - substring
# MAGIC 
# MAGIC Let us perform few tasks to extract information from fixed length strings.
# MAGIC * Create a list for employees with name, ssn and phone_number.
# MAGIC * SSN Format **3 2 4** - Fixed Length with 11 characters
# MAGIC * Phone Number Format - Country Code is variable and remaining phone number have 10 digits:
# MAGIC  * Country Code - one to 3 digits
# MAGIC  * Area Code - 3 digits
# MAGIC  * Phone Number Prefix - 3 digits
# MAGIC  * Phone Number Remaining - 4 digits
# MAGIC  * All the 4 parts are separated by spaces
# MAGIC * Create a Dataframe with column names name, ssn and phone_number
# MAGIC * Extract last 4 digits from the phone number.
# MAGIC * Extract last 4 digits from SSN.

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

employeesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import substring, col

# COMMAND ----------

employeesDF. \
    select("employee_id", "phone_number", "ssn"). \
    withColumn("phone_last4", substring(col("phone_number"), -4, 4).cast("int")). \
    withColumn("ssn_last4", substring(col("ssn"), 8, 4).cast("int")). \
    show()

# COMMAND ----------

employeesDF. \
    select("employee_id", "phone_number", "ssn"). \
    withColumn("phone_last4", substring(col("phone_number"), -4, 4).cast("int")). \
    withColumn("ssn_last4", substring(col("ssn"), 8, 4).cast("int")). \
    printSchema()

# COMMAND ----------

