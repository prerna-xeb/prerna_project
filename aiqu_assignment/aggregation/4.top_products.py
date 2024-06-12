# Databricks notebook source
# script to give top 5 products
product_df = spark.sql("select product_id,quantity from sales_analytics.sales")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

total_quantity_df = product_df\
                    .groupBy("product_id")\
                    .agg(sum("quantity").alias("total_quantity"))

# COMMAND ----------

display(total_quantity_df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

rownum_spec = Window.orderBy(desc("total_quantity"))
final_df = total_quantity_df.withColumn("rn",row_number().over(rownum_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

result_df = final_df\
            .where("rn <=5")\
            .select("product_id","total_quantity")

# COMMAND ----------

display(result_df)

# COMMAND ----------

import plotly.express as px

#Top 5 Products
fig1 = px.bar(result_df.toPandas(),
              x='product_id', y='total_quantity',
              title='Top Five Products')
fig1.show()