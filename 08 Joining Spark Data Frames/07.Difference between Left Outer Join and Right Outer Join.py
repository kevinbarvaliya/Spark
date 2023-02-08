# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * Left or Right is used based on the side of the driving Data Frame.
# MAGIC * Between users and course_enrolments, users is typically the driving Data Frame as there is one to many relationship between users and course_enrolments.
# MAGIC * Here is how we typically perform outer join between **users** and **course_enrolments**.
# MAGIC   * **users** left outer join **course_enrolments**.
# MAGIC   * **course_enrolments** right outer join **users**.
# MAGIC * Also here is how we typically perform outer join between **courses** and **courses_enrolments**.
# MAGIC   * **courses** left outer join **course_enrolments**
# MAGIC   * **course_enrolments** right outer join **courses**.

# COMMAND ----------

