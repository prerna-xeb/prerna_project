# Databricks notebook source
# scripts to do extra analysis on the data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("OtherAnalysis").getOrCreate()

# COMMAND ----------

sales_df = spark.sql("select quantity,price,city from sales_analytics.user_sales")

# COMMAND ----------

# Calculate 'total_amount' as the product of 'quantity' and 'price'
sales_df = sales_df.withColumn("total_amount", col("quantity") * col("price"))

# COMMAND ----------

display(sales_df)

# COMMAND ----------


#Average Order Value
average_order_value_df = sales_df.agg(
    round(avg("total_amount"),2).alias("average_order_value")
)

# COMMAND ----------

display(average_order_value_df)

# COMMAND ----------

# Sales Volume Distribution (e.g., ranges of quantities)
sales_volume_distribution_df = sales_df.groupBy("quantity").agg(
    sum("total_amount").alias("total_revenue")
).orderBy("quantity")

# COMMAND ----------

display(sales_volume_distribution_df)

# COMMAND ----------

# revenue generated across each city
city_revenue_df = sales_df\
                .groupBy("city")\
                .agg(round(sum("total_amount"),2).alias("total_revenue"))\
                .orderBy(desc("total_revenue"))

# COMMAND ----------

display(city_revenue_df)

# COMMAND ----------

import plotly.express as px

#Revenue generated across each city
fig1 = px.bar(city_revenue_df.toPandas(),
              x='city', y='total_revenue',
              title='Revenue generated across each city')
fig1.show()