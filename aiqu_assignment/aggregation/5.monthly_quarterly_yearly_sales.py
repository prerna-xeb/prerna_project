# Databricks notebook source
# script to give sales report yearly, quarterly and monthly
sales_df = spark.sql("select * from sales_analytics.sales")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("yearly_monthly_quarterly_sales").getOrCreate()

# COMMAND ----------

# Calculate 'total_amount' as the product of 'quantity' and 'price'
sales_df = sales_df.withColumn("total_amount", col("quantity") * col("price"))

# COMMAND ----------

# Convert 'order_date' to date type and extract year, month, and quarter
sales_df = sales_df.withColumn("year", year(col("order_date"))) \
                    .withColumn("month", month(col("order_date"))) \
                    .withColumn("quarter", quarter(col("order_date")))\
                    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))

# COMMAND ----------

sales_df.persist()

# COMMAND ----------

display(sales_df)

# COMMAND ----------

# Aggregate Sales Data - Monthly Sales
monthly_sales_df = sales_df.groupBy("year", "month","year_month").agg(
                round(sum("total_amount"),2).alias("total_sales_amount"),
                sum("quantity").alias("total_quantity_sold")
).orderBy("year_month")

# COMMAND ----------

display(monthly_sales_df)

# COMMAND ----------

import plotly.express as px

# Monthly Sales
fig1 = px.bar(monthly_sales_df.toPandas(),
              x='year_month', y='total_sales_amount',
              title='Monthly Sales')
fig1.show()

# COMMAND ----------

#Aggregate Sales Data - Quarterly Sales
quarterly_sales_df = sales_df.groupBy("year", "quarter").agg(
    round(sum("total_amount"),2).alias("total_sales_amount"),
    sum("quantity").alias("total_quantity_sold")
).orderBy("year", "quarter")

# COMMAND ----------

quarterly_sales_df = quarterly_sales_df\
                    .withColumn("year_quarter", concat_ws("-", "year", "quarter"))\
                    .orderBy("year_quarter")

# COMMAND ----------

display(quarterly_sales_df)

# COMMAND ----------

import plotly.express as px

# Quarterly Sales
fig2 = px.bar(quarterly_sales_df.toPandas(),
              x='year_quarter', y='total_sales_amount',
              title='Quarterly Sales')
fig2.show()

# COMMAND ----------

#Aggregate Sales Data - yearly Sales
yearly_sales_df = sales_df.groupBy("year").agg(
    round(sum("total_amount"),2).alias("total_sales_amount"),
    sum("quantity").alias("total_quantity_sold")
).orderBy("year")

# COMMAND ----------

display(yearly_sales_df)

# COMMAND ----------

import plotly.express as px

# Quarterly Sales
fig3 = px.bar(yearly_sales_df.toPandas(),
              x='year', y='total_sales_amount',
              title='Yearly Sales')
fig3.show()

# COMMAND ----------

sales_df.unpersist()