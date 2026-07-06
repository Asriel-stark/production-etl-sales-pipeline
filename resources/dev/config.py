import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

# ---------------------------------------------------------------------------
# Pipeline Audit Metadata
# ---------------------------------------------------------------------------
# pipeline_version: Semantic version of this codebase. Increment on each
#   deployment so audit records can be correlated with specific code changes.
# environment: Deployment environment. Prevents DEV noise from polluting PROD
#   dashboards when multiple environments share a database host.
# trigger_type: How the job is initiated. Change to 'AIRFLOW', 'CRON', etc.
#   in the appropriate environment's config file.
# ---------------------------------------------------------------------------
pipeline_version = "2.0.0"
environment = "DEV"
trigger_type = "MANUAL"

#AWS Access And Secret key
aws_access_key = "access_key"
aws_secret_key = "secret_key"
bucket_name = "project-storageaws"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "deekshith"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "name",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = r"C:\Users\DEEKSHITH POOJARY\OneDrive\Desktop\project-1\file_from_s3"

customer_data_mart_local_file = r"C:\Users\DEEKSHITH POOJARY\OneDrive\Desktop\project-1\customer_data_mart"

sales_team_data_mart_local_file = r"C:\Users\DEEKSHITH POOJARY\OneDrive\Desktop\project-1\sales_team_data_mart"

sales_team_data_mart_partitioned_local_file = r"C:\Users\DEEKSHITH POOJARY\OneDrive\Desktop\project-1\sales_partition_data"

error_folder_path_local = r"C:\Users\DEEKSHITH POOJARY\OneDrive\Desktop\project-1\error_files"