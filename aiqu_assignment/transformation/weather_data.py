# Databricks notebook source
# script to read weather data from OpenWeatherMap API on the basis of latitude and longitude
import requests
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

API_KEY = "bbad11cd2ab65c6e12013ef62476e283"

# COMMAND ----------

# function to read data using API URL
def fetch_weather(lat, lng):
    weather_url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lng}&appid={API_KEY}&units=metric"
    response = requests.get(weather_url)
    if response.status_code == 200:
        data = response.json()
        temperature = data['main']['temp']
        weather_condition = data['weather'][0]['description']
        return temperature, weather_condition
    else:
        return None, None

# COMMAND ----------

merged_sales_user_df = spark.sql("select * from sales_analytics.user_sales")

# COMMAND ----------

# fetching weather data for a particular lat and long
weather_data = []
for row in merged_sales_user_df.collect():
    lat = row['lat']
    lng = row['lng']
    temperature, weather_condition = fetch_weather(lat, lng)
    weather_data.append((row['order_id'], temperature, weather_condition))





# COMMAND ----------

# Convert weather data to DataFrame and then to PySpark DataFrame
weather_df = pd.DataFrame(weather_data, columns=['order_id', 'temperature', 'weather_condition'])
weather_spark_df = spark.createDataFrame(weather_df)



# COMMAND ----------

#Merge Weather Data with sales-user Data
final_df = merged_sales_user_df.join(weather_spark_df, on="order_id", how="left")



# COMMAND ----------

#Display Final Dataset with Weather Data
display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').format('Parquet').saveAsTable('sales_analytics.user_sales_weather')