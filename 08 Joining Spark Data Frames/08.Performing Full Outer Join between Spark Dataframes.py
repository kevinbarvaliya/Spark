# Databricks notebook source
users1 = [
    {
        "email":"alovett0@nsw.gov.au",
        "first_name":"Aundrea",
        "last_name":"Lovett",
        "gender":"Male",
        "ip_address":"62.72.1.143"
    },
    {
        "email":"bjowling1@spiegel.de",
        "first_name":"Bettine",
        "last_name":"Jowling",
        "gender":"Female",
        "ip_address":"26.250.197.47"
    },
    {
        "email":"rablitt2@technorati.com",
        "first_name":"Reggie",
        "last_name":"Ablitt",
        "gender":"Male",
        "ip_address":"104.181.218.238"
    },
    {
        "email":"tgavahan3@printfriendly.com",
        "first_name":"Ted",
        "last_name":"Gavahan",
        "gender":"Female",
        "ip_address":"216.80.86.100"
    },
    {
        "email":"ccastellan4@bloglovin.com",
        "first_name":"Chantal",
        "last_name":"Castellan",
        "gender":"Female",
        "ip_address":"178.93.82.145"
    },
    {
        "email":"hcurrier5@hexun.com",
        "first_name":"Herrick",
        "last_name":"Currier",
        "gender":"Male",
        "ip_address":"98.120.5.78"
    },
    {
        "email":"zlendrem6@columbia.edu",
        "first_name":"Zorina",
        "last_name":"Lendrem",
        "gender":"Female",
        "ip_address":"219.128.213.53"
    },
    {
        "email":"lbutland7@time.com",
        "first_name":"Lilas",
        "last_name":"Butland",
        "gender":"Female",
        "ip_address":"109.110.131.151"
    },
    {
        "email":"palfonsetti8@ask.com",
        "first_name":"Putnam",
        "last_name":"Alfonsetti",
        "gender":"Female",
        "ip_address":"167.97.48.246"
    },
    {
        "email":"hunitt9@bizjournals.com",
        "first_name":"Holden",
        "last_name":"Unitt",
        "gender":"Female",
        "ip_address":"142.228.161.192"
    },
    {
        "email":"dmcmorrana@reference.com",
        "first_name":"Dorice",
        "last_name":"McMorran",
        "gender":"Female",
        "ip_address":"233.1.28.220"
    },
    {
        "email":"afaulconerb@barnesandnoble.com",
        "first_name":"Andris",
        "last_name":"Faulconer",
        "gender":"Female",
        "ip_address":"109.40.175.103"
    },
    {
        "email":"kupexc@sun.com",
        "first_name":"Krispin",
        "last_name":"Upex",
        "gender":"Male",
        "ip_address":"154.110.22.75"
    },
    {
        "email":"fmancktelowd@youku.com",
        "first_name":"Farand",
        "last_name":"Mancktelow",
        "gender":"Genderqueer",
        "ip_address":"190.20.187.10"
    },
    {
        "email":"kdodgshune@google.com",
        "first_name":"Kellyann",
        "last_name":"Dodgshun",
        "gender":"Female",
        "ip_address":"80.247.105.228"
    }
]

from pyspark.sql import Row
users1_df = spark.createDataFrame([Row(**user) for user in users1])

# COMMAND ----------

users2 = [{
        "email":"lbutland7@time.com",
        "first_name":"Lilas",
        "last_name":"Butland",
        "gender":"Female",
        "ip_address":"109.110.131.151"
    },
    {
        "email":"palfonsetti8@ask.com",
        "first_name":"Putnam",
        "last_name":"Alfonsetti",
        "gender":"Female",
        "ip_address":"167.97.48.246"
    },
    {
        "email":"hunitt9@bizjournals.com",
        "first_name":"Holden",
        "last_name":"Unitt",
        "gender":"Female",
        "ip_address":"142.228.161.192"
    },
    {
        "email":"dmcmorrana@reference.com",
        "first_name":"Dorice",
        "last_name":"McMorran",
        "gender":"Female",
        "ip_address":"233.1.28.220"
    },
    {
        "email":"afaulconerb@barnesandnoble.com",
        "first_name":"Andris",
        "last_name":"Faulconer",
        "gender":"Female",
        "ip_address":"109.40.175.103"
    },
    {
        "email":"kupexc@sun.com",
        "first_name":"Krispin",
        "last_name":"Upex",
        "gender":"Male",
        "ip_address":"154.110.22.75"
    },
    {
        "email":"fmancktelowd@youku.com",
        "first_name":"Farand",
        "last_name":"Mancktelow",
        "gender":"Genderqueer",
        "ip_address":"190.20.187.10"
    },
    {
        "email":"kdodgshune@google.com",
        "first_name":"Kellyann",
        "last_name":"Dodgshun",
        "gender":"Female",
        "ip_address":"80.247.105.228"
    },
    {
        "email":"kbaressf@geocities.jp",
        "first_name":"Karly",
        "last_name":"Baress",
        "gender":"Female",
        "ip_address":"145.232.153.145"
    },
    {
        "email":"amillinsg@com.com",
        "first_name":"Adelaide",
        "last_name":"Millins",
        "gender":"Female",
        "ip_address":"75.160.220.182"
    },
    {
        "email":"skemsleyh@quantcast.com",
        "first_name":"Shir",
        "last_name":"Kemsley",
        "gender":"Male",
        "ip_address":"234.195.73.177"
    },
    {
        "email":"kchomiszewskii@simplemachines.org",
        "first_name":"Kristo",
        "last_name":"Chomiszewski",
        "gender":"Female",
        "ip_address":"60.91.73.198"
    },
    {
        "email":"rkelwickj@baidu.com",
        "first_name":"Rosemonde",
        "last_name":"Kelwick",
        "gender":"Genderfluid",
        "ip_address":"42.50.134.65"
    }
]

from pyspark.sql import Row
users2_df = spark.createDataFrame([Row(**user) for user in users2])

# COMMAND ----------

users1_df.count()

# COMMAND ----------

users2_df.count()

# COMMAND ----------

# outer or full or fullouter or full_outer are same

users1_df. \
    join(users2_df, users1_df.email == users2_df.email, 'full'). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, users1_df.email == users2_df.email, 'fullouter'). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, users1_df.email == users2_df.email, 'full_outer'). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'full_outer'). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'full_outer'). \
    count()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'left'). \
    union(
        users1_df. \
            join(users2_df, 'email', 'right')
    ). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'left'). \
    union(
        users1_df. \
            join(users2_df, 'email', 'right')
    ). \
    count()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'left'). \
    union(
        users1_df. \
            join(users2_df, 'email', 'right')
    ). \
    distinct(). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, 'email', 'left'). \
    union(
        users1_df. \
            join(users2_df, 'email', 'right')
    ). \
    distinct(). \
    count()

# COMMAND ----------

# Projecting data after full outer join
# Get the details from the users1_df, if missing get details from users2_df

from pyspark.sql.functions import coalesce
users1_df. \
    join(users2_df, users1_df.email == users2_df.email, 'full'). \
    select(
        coalesce(users1_df['email'], users2_df['email']).alias('email'),
        coalesce(users1_df['first_name'], users2_df['first_name']).alias('first_name'),
        coalesce(users1_df['last_name'], users2_df['last_name']).alias('last_name'),
        coalesce(users1_df['gender'], users2_df['gender']).alias('gender'),
        coalesce(users1_df['ip_address'], users2_df['ip_address']).alias('ip_address'),
    ). \
    show()

# COMMAND ----------

# Using alias
users1_df.alias('u1'). \
    join(users2_df.alias('u2'), users1_df.email == users2_df.email, 'full'). \
    select(
        coalesce('u1.email', 'u2.email').alias('email'),
        coalesce('u1.first_name', 'u2.first_name').alias('first_name'),
        coalesce('u1.last_name', 'u2.last_name').alias('last_name'),
        coalesce('u1.gender', 'u2.gender').alias('gender'),
        coalesce('u1.ip_address', 'u2.ip_address').alias('ip_address'),
    ). \
    show()

# COMMAND ----------

