# Databricks notebook source
courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972}]

from pyspark.sql import Row
courses_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

# Review Supported modes
help(courses_df.write.mode)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will perform tasks to understand the behavior of error/errorIfExists, overwrite, append. You can try out ignore and see what happens.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Different ways mode can be specified while writing data frame into files. `file_format` can be any valid out of the box format such as `text`, `csv`, `json`, `parquet`, `orc`.
# MAGIC * `courses_df.write.mode(saveMode).file_format(path_to_folder)`
# MAGIC * `courses_df.write.file_format(path_to_folder, mode=saveMode)`
# MAGIC * `courses_df.write.mode(saveMode).format('file_format').save(path_to_folder)`
# MAGIC * `courses_df.write.format('file_format').save(path_to_folder, mode=saveMode)`

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Understand default behavior.
# MAGIC   * Fails if folder exists.
# MAGIC   * Creates folder and then adds files to it.

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/courses', recurse=True)

# COMMAND ----------

# Will create folder and add files with data using parquet format
courses_df. \
    coalesce(1). \
    write. \
    parquet(f'/user/{username}/courses')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

# Fails as mode is error or errorIfExists by default
courses_df.write.parquet(f'/user/{username}/courses')

# COMMAND ----------

spark.read.parquet(f'/user/{username}/courses').count()

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    parquet(f'/user/{username}/courses')

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    parquet(f'/user/{username}/courses', mode='overwrite')

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    format('parquet'). \
    save(f'/user/{username}/courses', mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

spark.read.parquet(f'/user/{username}/courses').count()

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    mode('append'). \
    parquet(f'/user/{username}/courses')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

spark.read.parquet(f'/user/{username}/courses').count()

# COMMAND ----------

