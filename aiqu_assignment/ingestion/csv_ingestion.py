# Databricks notebook source
# script to ingest sales data from csv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# defining schema for sales data
sales_schema = StructType(fields=[StructField("order_id", IntegerType(), True),
                                     StructField("customer_id", IntegerType(), True),
                                     StructField("product_id", IntegerType(), True),
                                     StructField("quantity", IntegerType(), True),
                                     StructField("price", FloatType(), True),
                                     StructField("order_date", DateType(), True)
])

# COMMAND ----------

# reading data from csv and put it in dataframe
sales_df=spark.read\
        .option("Header",True)\
        .format("csv")\
        .schema(sales_schema)\
        .load("/mnt/analytics/bronze/sales_data.csv")

# COMMAND ----------

display(sales_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Sales_Analytics;

# COMMAND ----------

sales_df.write.mode("overwrite").format("parquet").saveAsTable("Sales_Analytics.sales")