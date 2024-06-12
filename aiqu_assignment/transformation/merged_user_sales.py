# Databricks notebook source
# script to merge sales and user data

# COMMAND ----------

# read user data and put it in dataframe
user_df = spark.sql("select id,name,email,lat,lng,city from sales_analytics.user")

# COMMAND ----------

# read sales data and put it in dataframe
sales_df = spark.sql("select * from sales_analytics.sales")

# COMMAND ----------

# use broadcast join as user table size is less and we have selected only few columns
from pyspark.sql.functions import broadcast

result_df = sales_df.join(broadcast(user_df), user_df.id==sales_df.customer_id, "left")

# COMMAND ----------

from pyspark.sql.functions import col

final_df = result_df.drop(col("customer_id"))

# COMMAND ----------

final_df = final_df.withColumnRenamed("id","customer_id")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# create table user_saes
final_df.write.mode("overwrite").format("parquet").saveAsTable("sales_analytics.user_sales")