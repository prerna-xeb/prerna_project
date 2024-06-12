# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table of Contents
# MAGIC - [Introduction](#introduction)
# MAGIC - [Components](#components)
# MAGIC - [Setup Instructions](#setup-instructions)
# MAGIC - [Usage Instructions](#usage-instructions)
# MAGIC - [Data Pipeline Description](#data-pipeline-description)
# MAGIC - [Aggregations and Data Manipulations](#aggregations-and-data-manipulations)
# MAGIC - [Dependencies](#dependencies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC This project provides a Dockerized data pipeline that runs Databricks notebooks to perform various data processing tasks. The pipeline fetches user and sales data, integrates weather information, and performs several data aggregations and transformations.
# MAGIC
# MAGIC ## Components
# MAGIC 1. **Dockerfile**: Defines the Docker image for the pipeline.
# MAGIC 2. **requirements.txt**: Lists the required Python packages.
# MAGIC 3. **main.py**: Entry point script to run the Databricks notebooks with exception handling.
# MAGIC 4. **weather_data.py**: Notebook script to fetch and integrate weather data with sales data.
# MAGIC 5. **merged_user_sales.py**: Notebook script to merge user and sales data.
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC 1. **Install Docker**: Ensure Docker is installed on your machine. You can download and install Docker from [here](https://www.docker.com/products/docker-desktop).
# MAGIC
# MAGIC 2. **Project Directory Structure**:
# MAGIC    Create a project directory with the following structure:
# MAGIC    /project
# MAGIC   |-- Dockerfile
# MAGIC   |-- requirements.txt
# MAGIC   |-- main.py
# MAGIC   |-- weather_data.py
# MAGIC   |-- merged_user_sales.py

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Dockerfile**:
# MAGIC Create a `Dockerfile` in the project directory:

# COMMAND ----------

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Databricks CLI
RUN pip install databricks-cli

# Set environment variables for Databricks CLI
ENV DATABRICKS_HOST=<your-databricks-host>
ENV DATABRICKS_TOKEN=<your-databricks-token>

# Run main.py when the container launches
CMD ["python", "main.py"]


# COMMAND ----------

# MAGIC %md
# MAGIC 4. **requirements.txt**:
# MAGIC Create a *'requirements.txt'* file in the project directory:

# COMMAND ----------

requests
pandas
pyspark


# COMMAND ----------

# MAGIC %md
# MAGIC 5. **main.py**:
# MAGIC Create a main.py script in the project directory:

# COMMAND ----------

# MAGIC %md
# MAGIC 6. **weather_data.py**:
# MAGIC Create a weather_data.py script in the project directory:

# COMMAND ----------

# MAGIC %md
# MAGIC 7. **merged_user_sales.py**:
# MAGIC Create a merged_user_sales.py script in the project directory:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC **1. Build the Docker Image:**

# COMMAND ----------

docker build -t my-databricks-app .


# COMMAND ----------

# MAGIC %md
# MAGIC **2. Run the Docker Container:**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Pipeline Description
# MAGIC The data pipeline consists of the following components:
# MAGIC
# MAGIC **1. weather_data.py:** Fetches weather data from the OpenWeatherMap API based on the latitude and longitude of each sale, and integrates this data with the sales data.
# MAGIC **2. merged_user_sales.py:** Merges user and sales data to create a combined dataset, which is then saved to a table for further analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations and Data Manipulations
# MAGIC The following aggregations and data manipulations are performed in the pipeline:
# MAGIC
# MAGIC **1. Total Sales Amount per Customer**: Calculated by summing the sales amounts for each customer.
# MAGIC
# MAGIC **2. Average Order Quantity per Product**: Determined by averaging the order quantities for each product.
# MAGIC
# MAGIC **3. Top-Selling Products**: Identified by sorting the products based on the total sales amount.
# MAGIC
# MAGIC **4. Top Customers**: Identified by sorting the customers based on the total sales amount.
# MAGIC
# MAGIC **5. Monthly Sales Trends**: Analyzed by aggregating sales data on a monthly basis.
# MAGIC
# MAGIC **6. Average Sales per Weather Condition**: Analyzed by calculating the average sales amount for each weather condition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies
# MAGIC - Python 3.9
# MAGIC - requests
# MAGIC - pandas
# MAGIC - pyspark
# MAGIC - Databricks CLI

# COMMAND ----------

# MAGIC %md
# MAGIC ![Monthly Sales](aggregation/top_5_customers.png)