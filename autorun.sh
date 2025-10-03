#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="data-engineering.json"
export gcp_bucket_name="raw_data_sales_etl"
export gcs_object="raw_data_sales_etl/sales_data.csv"
export project_id="data-engineering-472313"
export dataset_id="regional_sales"
export table_id="sales_data"

python_script="/app/main.py"

echo "Running The scripts: $python_script" 
python3 "$python_script"