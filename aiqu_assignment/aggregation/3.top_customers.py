# Databricks notebook source
# script to give top 5 customers
from pyspark.sql.functions import *

# COMMAND ----------

total_sales_df = spark.sql("select * from sales_analytics.total_sales")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

row_num_spec = Window.orderBy(desc("total_amount"))
final_df = total_sales_df.withColumn("rn",row_number().over(row_num_spec))


# COMMAND ----------

final_top_five_df = final_df\
                    .where("rn <=5")\
                    .select("customer_name","total_amount")

# COMMAND ----------

display(final_top_five_df)

# COMMAND ----------

import plotly.express as px

display(final_top_five_df)

#Top 5 customers
fig1 = px.bar(final_top_five_df.toPandas(),
              x='customer_name', y='total_amount',
              title='Top Five Customers')
fig1.show()