# Databricks notebook source
# MAGIC %run "./01.Setup Data Sets to perform joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get all the user details along with course enrolment details (if the user have any course enrolments).
# MAGIC * If the users does not have any course enrolments, we need to get all user details. Course details will be substituted with null values.
# MAGIC   * Need to perform left or right outer join between **users_df** and **course_enrolments_df**.
# MAGIC   * We will use right for this lecture. As `users_df` is from parent table and as we are going to use **right outer join**, we need to invoke `join` on top of `users_df`.
# MAGIC   * Here are the fields that needs to be displayed.
# MAGIC     * All fields from `users_df`
# MAGIC     * `course_id` and `course_enrolment_id` from `course_enrolments_df`
# MAGIC * For this example using these data frames, using just `outer` also give same results. But it is not correct to use `outer`.
# MAGIC * `how='outer'` means **full outer join**.

# COMMAND ----------

course_enrolments_df. \
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right'). \
    show()

# COMMAND ----------

# right or right_outer or rightouter are same.

course_enrolments_df. \
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right_outer'). \
    show()

# COMMAND ----------

course_enrolments_df. \
    join(users_df, 'user_id', 'right'). \
    show()

# COMMAND ----------

course_enrolments_df. \
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right'). \
    select(users_df['*'], course_enrolments_df['course_id'], course_enrolments_df['course_enrolment_id']). \
    show()

# COMMAND ----------

# using alias
course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

# Get all the users who have not enroled for any courses
# Recommended to use primary key in the child table when comparing with null values

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    filter('ce.course_enrolment_id IS NULL'). \
    select('u.*', 'course_id', 'course_enrolment_id'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Get number of courses enroled by each user
# MAGIC * If there are no enrolments, then count should return 0

# COMMAND ----------

# count will give incorrect results
# Even though users 1, 2, 6 are not enrolled for any courses, it returns 1
course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    count(). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------

from pyspark.sql.functions import sum, when

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    agg(sum(when(course_enrolments_df['course_enrolment_id'].isNull(), 0).otherwise(1)).alias('course_count')). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

course_enrolments_df.alias('ce'). \
    join(users_df.alias('u'), users_df.user_id == course_enrolments_df.user_id, 'right'). \
    groupBy('u.user_id'). \
    agg(sum(expr('''
        CASE WHEN ce.course_enrolment_id IS NULL
            THEN 0
        ELSE 1
        END
    ''')).alias('course_count')). \
    orderBy('u.user_id'). \
    show()

# COMMAND ----------

