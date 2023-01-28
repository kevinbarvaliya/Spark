# Databricks notebook source
# MAGIC %md
# MAGIC ## Extracting Strings using split
# MAGIC Let us understand how to extract substrings from main string using `split` function.

# COMMAND ----------

# MAGIC %md
# MAGIC * If we are processing **variable length columns** with **delimiter** then we use `split` to extract the information.
# MAGIC * Here are some of the examples for **variable length columns** and the use cases for which we typically extract information.
# MAGIC * Address where we store House Number, Street Name, City, State and Zip Code comma separated. We might want to extract City and State for demographics reports.
# MAGIC * `split` takes 2 arguments, **column** and **delimiter**.
# MAGIC * `split` convert each string into array and we can access the elements using index.
# MAGIC * We can also use `explode` in conjunction with `split` to explode the list or array into records in Data Frame. It can be used in cases such as word count, phone count etc. 

# COMMAND ----------

l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l, "dummy STRING")

# COMMAND ----------

from pyspark.sql.functions import split, explode, lit

# COMMAND ----------

df.select(split(lit("Hello World, how are you"), " ")). \
    show(truncate=False)

# COMMAND ----------

df.select(split(lit("Hello World, how are you"), " ")[2]). \
    show(truncate=False)

# COMMAND ----------

df.select(explode(split(lit("Hello World, how are you"), " ")).alias('word')). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Most of the problems can be solved either by using `substring` or `split`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - split
# MAGIC Let us perform few tasks to extract information from fixed length strings as well as delimited variable length strings.
# MAGIC * Create a list for employees with name, ssn and phone_numbers.
# MAGIC * SSN Format **3 2 4** - Fixed Length with 11 characters
# MAGIC * Phone Number Format - Country Code is variable and remaining phone number have 10 digits. One can have multiple phone numbers where they are separated by `,`:
# MAGIC  * Country Code - one to 3 digits
# MAGIC  * Area Code - 3 digits
# MAGIC  * Phone Number Prefix - 3 digits
# MAGIC  * Phone Number Remaining - 4 digits
# MAGIC  * All the 4 parts are separated by spaces
# MAGIC * Create a Dataframe with column names name, ssn and phone_number
# MAGIC * Extract area code and last 4 digits from the phone number.
# MAGIC * Extract last 4 digits from SSN.

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_numbers STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF. \
    select('employee_id', 'phone_numbers'). \
    show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, explode

# COMMAND ----------

employeesDF = employeesDF. \
    select('employee_id', 'phone_numbers', 'ssn'). \
    withColumn('phone_number', explode(split('phone_numbers', ',')))

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employeesDF. \
    select("employee_id", "phone_number", "ssn"). \
    withColumn("area_code", split("phone_number", " ")[1].cast("int")). \
    withColumn("phone_last4", split("phone_number", " ")[3].cast("int")). \
    withColumn("ssn_last4", split("ssn", " ")[2].cast("int")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Getting Phone Count

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employeesDF. \
    groupBy('employee_id'). \
    count(). \
    show()

# COMMAND ----------

