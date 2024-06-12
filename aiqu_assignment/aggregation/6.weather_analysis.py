# Databricks notebook source
# script to calculate total sales amount and avg sales amount for each weather condition.
from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.sql("select product_id,quantity,price, weather_condition from sales_analytics.user_sales_weather")

# COMMAND ----------

sales_df = sales_df.withColumn("total_amount", col("quantity") * col("price"))

# COMMAND ----------

# Analyze Average Sales Amount Per Weather Condition
average_sales_per_weather_df= sales_df.groupBy("weather_condition").agg(
    round(avg("total_amount"),2).alias("average_sales_amount"),
    round(sum("total_amount"),2).alias("total_sales_amount")
).orderBy(desc("total_sales_amount"))

# COMMAND ----------

display(average_sales_per_weather_df)

# COMMAND ----------

import plotly.express as px

# Sales per weather condition
fig1 = px.bar(average_sales_per_weather_df.toPandas(),
              x='weather_condition', y='total_sales_amount',
              title='Sales per weather condition')
fig1.show()

# COMMAND ----------

# script to get the product with maximum sale revenue for each weather condition
product_df = sales_df.groupBy("weather_condition","product_id").agg(
    round(sum("total_amount"),2).alias("total_sales_amount")
).orderBy(desc("weather_condition"))

# COMMAND ----------

display(product_df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# top selling product for each weather condition
row_number_spec = Window.partitionBy("weather_condition").orderBy(desc("total_sales_amount"))
final_df = product_df.withColumn("rn",row_number().over(row_number_spec))

# COMMAND ----------

result_df = final_df\
            .where("rn =1")\
            .select("weather_condition","product_id","total_sales_amount")


# COMMAND ----------

display(result_df)

# COMMAND ----------

import plotly.express as px

# Sales per weather condition
fig1 = px.bar(result_df.toPandas(),
              x='weather_condition', y='product_id',
              title='Highest Revenue Generating Product per weather condition')
fig1.show()