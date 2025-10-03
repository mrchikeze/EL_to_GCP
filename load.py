
import pandas as pd
import glob
from google.cloud import storage
from google.cloud import bigquery

def load(data_to_load, gcp_bucket_name, gcs_object, project_id, dataset_id, table_id):
    if data_to_load is None:
        print("Data Unavailable to load to cloud")
        return
    try:
        print(f"Uploading to Google Bucket '{gcp_bucket_name}' at '{gcs_object}'")
        client=storage.Client()
        bucket= client.bucket(gcp_bucket_name)
        blob=bucket.blob(gcs_object)
        dt=pd.DataFrame(data_to_load)
        csv_data=dt.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')

        #Verify Upload
        if blob.exists():
            gcs_url=f"gs://{gcp_bucket_name}/{gcs_object}"
            print("Uploaded successfully to :",gcs_url)

        else:
            raise Exception("Uploading verification failed")
        

        #Loading to Bigquery from GCS
        print(f"Loading to Bigquery fromGCS ",gcs_url)
        bg=bigquery.Client(project=project_id)

        #Verify if dataset(table) exists 
        id=bg.dataset(dataset_id)
        if id is None:
            dataset=bigquery.Dataset(dataset_id)
            dataset=bg.create_dataset(dataset)
            print("Dataset Created Successfully")


        table_ref=id.table(table_id)
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header
            write_disposition="WRITE_TRUNCATE",  # Replace table if exists
            autodetect=True,  # Auto-detect schema
        )

        load_job = bg.load_table_from_uri(
        [gcs_url], table_ref, job_config=job_config
        )
        load_job.result()  # Wait for job to complete

            # Post-load validation
        table = bg.get_table(table_ref)
        expected_rows = len(data_to_load)
        if table.num_rows == expected_rows:
            print(f"Load successful! Loaded {table.num_rows} rows to '{project_id}.{dataset_id}.{table_id}'.")
        else:
            print(f"Warning: Loaded {table.num_rows} rows, expected {expected_rows}.")
            
    except Exception as e:
        print(f"Error during loading: {e}")
        if 'bigquery' in str(type(e)).lower():
            print("Tip: Check BigQuery permissions and dataset existence.")
        if 'storage' in str(type(e)).lower():
            print("Tip: Check GCS bucket name and permissions.")







