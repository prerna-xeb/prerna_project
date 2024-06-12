# Databricks notebook source
# script to calculate average order quantity per product
from pyspark.sql.functions import *

# COMMAND ----------

user_sales_df = spark.sql("select product_id, quantity, order_id from sales_analytics.user_sales")

# COMMAND ----------

avg_order_qty_df = user_sales_df\
                .groupBy("product_id")\
                .agg(round((sum("quantity")/count("order_id")),2).alias("avg_order_qty"))\
                .orderBy("product_id")


# COMMAND ----------

display(avg_order_qty_df)

# COMMAND ----------

import plotly.express as px

#Avg Order Quantity Product
fig1 = px.bar(avg_order_qty_df.toPandas(),
              x='product_id', y='avg_order_qty',
              title='Average Order Quantity')
fig1.show()