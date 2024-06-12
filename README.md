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



**5. Monthly Quarterly Yearly Sales:** This report gives total sales for every month, quarter and year.
Script:- *aiqu_assignment/aggregation/5.monthly_quarterly_yearly_sales*

Monthly Sales:
Data Snippet:-

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/f0a222bf-6ecb-4209-98fa-14d80e4e0b41)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/30b7e13e-4ed3-4636-8402-b36cdbe54f95)

Quarterly Sales:
Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/bb3adbe7-e485-4899-97dd-fdce04fdc269)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/703b37a3-3e35-4374-b9b5-4c149765ef02)

Yearly Sales:
Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/65002536-df10-45a8-9cf3-d83152ab407a)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/dff96f23-e04c-43a8-bd7b-edfe0b2d2141)


**6. Weather Analysis:** There are two reports in this script. First report gives total sales amount for every weather condition. Second report shows the product which had maximum revenue in each city.

Script:*aiqu_assignment/aggregation/6.weather_analysis*

Data Snippet:


![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/64fe9ac6-71e4-487a-a0e4-c6fa3f63bcf4)


Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/6f9f710a-9b84-4fd7-970f-f97aea4af2c2)


Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/0a03c856-a12d-4c93-892a-ac7d87a22aff)

Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/8b460e57-0bf4-4603-96fd-2d9ac08de0ad)

**7. Other Analysis:** This script has reports of Average Order Value, Sales Volume Distribution and total revenue generated for each city.

Script:

Average Order Value Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/ea28358a-365d-46f9-9e71-55823aa4785b)

Sales Volume Distribution Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/b7f36fd1-bc3f-4d74-aa30-186a256125c8)


Total Revenue Per City Data Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/80777e0d-eaf9-4c5f-bc9d-509cdb08e834)


Report Snippet:

![image](https://github.com/prerna-xeb/prerna_project/assets/171050743/fb0cbecb-3796-4c71-bf00-f3c6cc001bba)
