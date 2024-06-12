# Databricks notebook source
# script to calculate total sales for every customer
user_sales_df = spark.sql("select customer_id, name, price from sales_analytics.user_sales")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

total_sales_df = user_sales_df\
                .groupBy("customer_id")\
                .agg(max("name").alias("customer_name"),round(sum("price"),2).alias("total_amount"))\
                .orderBy("customer_id")



# COMMAND ----------

display(total_sales_df)

# COMMAND ----------

total_sales_df.write.mode('overwrite').format('parquet').saveAsTable("sales_analytics.total_sales")

# COMMAND ----------

final_df = total_sales_df.select("customer_name","total_amount")

# COMMAND ----------

display(final_df)

# COMMAND ----------

import plotly.express as px

#Total Sales per Customer
fig1 = px.bar(final_df.toPandas(),
              x='customer_name', y='total_amount',
              title='Total Sales per Customer')
fig1.show()