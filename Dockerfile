FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x autorun.sh

ENV GOOGLE_APPLICATION_CREDENTIALS="/app/data-engineering.json"
ENV gcp_bucket_name="raw_data_sales_etl"
ENV gcs_object="raw_data_sales_etl/sales_data.csv"
ENV project_id="data-engineering-472313"
ENV dataset_id="regional_sales"
ENV table_id="sales_data"

CMD ["./autorun.sh"]