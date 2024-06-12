# Databricks notebook source
def execute_notebooks(nb):
    try:
        # Use exec to run the %run command within a try-except block
        dbutils.notebook.run(nb, 0)
    except Exception as e:
        # Handle the exception
        print(f"An error occurred while running the notebook: {e}")


# COMMAND ----------

execute_notebooks("./configuration/mount_filepath")

# COMMAND ----------

execute_notebooks("./ingestion/api_ingestion")

# COMMAND ----------

execute_notebooks("./ingestion/csv_ingestion")

# COMMAND ----------

execute_notebooks("./transformation/merged_user_sales")

# COMMAND ----------

execute_notebooks("./transformation/weather_data")

# COMMAND ----------

execute_notebooks("./aggregation/1.total_sales_per_customer")

# COMMAND ----------

execute_notebooks("./aggregation/2.average_order_quantity_per_product")

# COMMAND ----------

execute_notebooks("./aggregation/3.top_customers")

# COMMAND ----------

execute_notebooks("./aggregation/4.top_products")

# COMMAND ----------

execute_notebooks("./aggregation/5.monthly_quarterly_yearly_sales")

# COMMAND ----------

execute_notebooks("./aggregation/6.weather_analysis")

# COMMAND ----------

execute_notebooks("./aggregation/7.other_analysis")