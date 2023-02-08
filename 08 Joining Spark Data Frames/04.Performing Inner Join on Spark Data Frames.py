# Databricks notebook source
# MAGIC %run "./01.Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get the user details who have enrolled for the courses.
# MAGIC   * Need to join **users_df** and **course_enrolments_df**.
# MAGIC   * Here are the fields that needs to be displayed.
# MAGIC     * All fields from `users_df`
# MAGIC     * `course_id` and `course_enrolment_id` from `course_enrolments`

# COMMAND ----------

users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id). \
    show()

# COMMAND ----------

# as both data frames have user id using same name, we can pass column name as string as well
users_df. \
    join(course_enrolments_df, 'user_id'). \
    show()

# COMMAND ----------

# Get all columns from users_df and course_id as well as course_enrolment_id from course_enrolments
users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id). \
    select(users_df['*'], course_enrolments_df['course_id'], course_enrolments_df['course_enrolment_id']). \
    show()

# COMMAND ----------

# using alias
users_df.alias('u'). \
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

# Get number of courses enroled by each user
# Fails as user_id is part of both the data frames
users_df.alias('u'). \
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id). \
    groupBy('user_id'). \
    count(). \
    show()

# COMMAND ----------

# Succeeds as we are picking up user_id from users_df
users_df. \
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id). \
    groupBy(users_df['user_id']). \
    count(). \
    show()

# COMMAND ----------

# Using alias
users_df.alias('u'). \
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id). \
    groupBy('u.user_id'). \
    count(). \
    show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, 'user_id'). \
    groupBy('user_id'). \
    count(). \
    show()

# COMMAND ----------

