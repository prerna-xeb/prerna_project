# Databricks Data Pipeline

## Table of Contents
- [Introduction](#introduction)
- [Components](#components)
- [Setup Instructions](#setup-instructions)
- [Usage Instructions](#usage-instructions)
- [Data Pipeline Description](#data-pipeline-description)
- [Aggregations and Data Manipulations](#aggregations-and-data-manipulations)
- [Dependencies](#dependencies)
- [Visualizations](#visualizations)

## Data Pipeline Description
The data pipeline consists of the following components:

**1. Data Ingestion:** Two pipelines have been created for ingesting user data and sales data.
**User Data**
**Sales Data**
  

**1. weather_data.py:** Fetches weather data from the OpenWeatherMap API based on the latitude and longitude of each sale, and integrates this data with the sales data.

**2. merged_user_sales.py:** Merges user and sales data to create a combined dataset, which is then saved to a table for further analysis.
