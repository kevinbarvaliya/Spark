# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

import datetime
courses = [
    {
        'course_id': 1,
        'course_title': 'Mastering Python',
        'course_published_dt': datetime.date(2021, 1, 14),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 2, 18, 16, 57, 25)
    },
    {
        'course_id': 2,
        'course_title': 'Data Engineering Essentials',
        'course_published_dt': datetime.date(2021, 2, 10),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 5, 12, 7, 33)
    },
    {
        'course_id': 3,
        'course_title': 'Mastering Pyspark',
        'course_published_dt': datetime.date(2021, 1, 7),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 4, 6, 10, 5, 42)
    },
    {
        'course_id': 4,
        'course_title': 'AWS Essentials',
        'course_published_dt': datetime.date(2021, 3, 19),
        'is_active': False,
        'last_updated_ts': datetime.datetime(2021, 4, 10, 2, 25, 36)
    },
    {
        'course_id': 5,
        'course_title': 'Docker 101',
        'course_published_dt': datetime.date(2021, 2, 28),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 21, 7, 18, 52)
    }
]

courses_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

users = [{
  "user_id": 1,
  "user_first_name": "Sandra",
  "user_last_name": "Karpov",
  "user_email": "skarpov0@ovh.net"
}, {
  "user_id": 2,
  "user_first_name": "Kari",
  "user_last_name": "Dearth",
  "user_email": "kdearth1@so-net.ne.jp"
}, {
  "user_id": 3,
  "user_first_name": "Joanna",
  "user_last_name": "Spennock",
  "user_email": "jspennock2@redcross.org"
}, {
  "user_id": 4,
  "user_first_name": "Hirsch",
  "user_last_name": "Conaboy",
  "user_email": "hconaboy3@barnesandnoble.com"
}, {
  "user_id": 5,
  "user_first_name": "Loreen",
  "user_last_name": "Malin",
  "user_email": "lmalin4@independent.co.uk"
}, {
  "user_id": 6,
  "user_first_name": "Augy",
  "user_last_name": "Christon",
  "user_email": "achriston5@mlb.com"
}, {
  "user_id": 7,
  "user_first_name": "Trudey",
  "user_last_name": "Choupin",
  "user_email": "tchoupin6@de.vu"
}, {
  "user_id": 8,
  "user_first_name": "Nadine",
  "user_last_name": "Grimsdell",
  "user_email": "ngrimsdell7@sohu.com"
}, {
  "user_id": 9,
  "user_first_name": "Vassily",
  "user_last_name": "Tamas",
  "user_email": "vtamas8@businessweek.com"
}, {
  "user_id": 10,
  "user_first_name": "Wells",
  "user_last_name": "Simpkins",
  "user_email": "wsimpkins9@amazon.co.uk"
}]

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

course_enrolments = [{
  "course_enrolment_id": 1,
  "user_id": 10,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 2,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 3,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 4,
  "user_id": 9,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 5,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 6,
  "user_id": 5,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 7,
  "user_id": 4,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 8,
  "user_id": 7,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 9,
  "user_id": 8,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 10,
  "user_id": 3,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 11,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 12,
  "user_id": 3,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 13,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 14,
  "user_id": 4,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 15,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}]

course_enrolments_df = spark.createDataFrame([Row(**ce) for ce in course_enrolments])

# COMMAND ----------

courses_df.show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

course_enrolments_df.show()

# COMMAND ----------

