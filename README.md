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

## 1.Data Ingestion: 
Two pipelines have been created for ingesting user data and sales data.

**1. User Data Pipeline:** This pipeline has been created to extract the information of user from API. A table with name 'User' has been created with the data fetched from API. One can refer the script here
*/aiqu_assignment/ingestion/api_ingestion*

**2. Sales Data Pipeline:** This pipeline has been created to extract data from csv file of sales data. 'Sales' table has been created with it. One can refer the script here
*/aiqu_assignment/ingestion/csv_ingestion*

## 2.Data Transformation: 
Two pipelines have been created for merging data.

**1. Merge User and Sales:** This pipeline has been created to join sales and user data. Only few columns have been selected from 'User' table. Since the size of User table is very small, Broadcast join has been used. A table with name 'user_sales' has been created with it. One can refer the script here */aiqu_assignment/transformation/merged_user_sales*.

**2. Merge User,Sales & Weather:** This pipeline has been created to merge data from 'user_sales' table with weather data. Temperature and weather condition information have been fetched from OpenWatherMapAPI for each latitude and longituse. A table with name 'user_sales_weather' has been created with it. One can refer the script here */aiqu_assignment/transformation/weather_data*.

## 3.Data Aggregation & Visualization: 
Various reports have been created from the user, sales and weather information which can help business.

**1. Total Sales Per Customer:** This report gives information of total sales for each customer. 

Script:- *aiqu_assignment/aggregation/1.total_sales_per_customer*

Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/5071182b-e3cd-4c27-b26d-6f4d3eaaaae8)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/48403b8c-87e4-4a0d-8226-10de09a1a4e9)






**2. Average Order Quantity Per Product:** This report gives information of average order quantity per product. 
Script:- *aiqu_assignment/aggregation/2.average_order_quantity_per_product*

Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/0c79364c-123b-4584-9b3d-93b9bf14326e)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/e956ad37-f17b-4e65-904a-7278a979315a)




**3. Top Customers:** This report gives information of top five customers. 
Script:- *aiqu_assignment/aggregation/3.top_customers*

Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/6b9b6943-c3a9-4e5d-b63a-1a9549fd6e20)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/075e87af-12a7-4c78-8130-a7e011592b1f)


**4. Top Products:** This report gives information of top five products. 
Script:- *aiqu_assignment/aggregation/4.top_products*

Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/df66b1f8-28a7-490b-9de3-ce323a593e54)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/a5dcfd0e-9b2c-47d8-b57f-e9ab0d1dd0b7)



**5. Top Customers:** This report gives information of top five customers. 
