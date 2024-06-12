# Databricks notebook source
# script to ingest user data from API
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Fetch JSON data from an API
api_url = "https://jsonplaceholder.typicode.com/users"

# Fetch data rom API
response = requests.get(api_url)
if response.status_code == 200:
    json_data = response.json()
else:
    print("Failed to fetch data from API")



# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read user JSON from API") \
    .getOrCreate()

# Convert JSON to RDD and then to DataFrame
# Assuming json_data is a list of dictionaries
payload_rdd = spark.sparkContext.parallelize(json_data)
df = spark.read.json(payload_rdd)




# COMMAND ----------

# Defining the structure of user JSON data 
user_df = df.select(
    "id",
    "name",
    "username",
    "email",
    "address.street",
    "address.suite",
    "address.city",
    "address.zipcode",
    "address.geo.lat",
    "address.geo.lng",
    "phone",
    "website",
    col("company.name").alias("company_name"),
    "company.catchPhrase",
    "company.bs"
)


# COMMAND ----------

# Display the DataFrame
display(user_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Sales_Analytics;

# COMMAND ----------

user_df.write.mode("overwrite").format("parquet").saveAsTable("sales_analytics.user")