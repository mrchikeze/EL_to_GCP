from extract import extract
from load import load
import os

bucket_name=os.getenv("gcp_bucket_name")
gcs=os.getenv("gcs_object")
p_id=os.getenv("project_id")
d_id=os.getenv("dataset_id")
t_id=os.getenv("table_id")


def extract_load_cloud(source_file=None, gcp_bucket_name=bucket_name, gcs_object=gcs, project_id=p_id, dataset_id=d_id, table_id=t_id):


    extracted=extract()
    load(extracted, gcp_bucket_name, gcs_object, project_id, dataset_id, table_id)



if __name__=="__main__":
    extract_load_cloud()
