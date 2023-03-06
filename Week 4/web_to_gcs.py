import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
from google.cloud import storage

# Set Google Cloud Storage bucket name
BUCKET_NAME = "bucket_name"  # Enter your bucket name here
 
# Set local directory for downloaded files
LOCAL_DIR = "data"

# Create the local directory if it doesn't exist
if not os.path.exists(LOCAL_DIR):
    os.makedirs(LOCAL_DIR)

# Define function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, blob_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"File {file_path} uploaded to gs://{bucket_name}/{blob_name}")


# Define function to get file from github to local and then call the upload_to_gcs function
def web_to_gcs(service, year):

    for month in range(1, 13):
        base_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/"
        file_name = f"{service}_tripdata_{year}-{month:02d}.csv.gz"
        url = base_url + file_name
        file_path = os.path.join(LOCAL_DIR, file_name)
        r = requests.get(url, allow_redirects=True)
        open(file_path, "wb").write(r.content)
        
        # Check if the downloaded file is a valid gzip file
        try:
            with gzip.open(file_path) as f:
                pass
        except Exception as e:
            print(f"Error: {e}. {file_name} may be corrupted.")
            os.remove(file_path)
            continue
        
        # Convert CSV file to Parquet
        csv = pd.read_csv(file_path)
        parquet_file_name = file_name.replace(".csv.gz", ".parquet")
        parquet_file_path = os.path.join(LOCAL_DIR, parquet_file_name)
        table = pa.Table.from_pandas(csv)
        pq.write_table(table, parquet_file_path)
            
        # Upload Parquet file to Google Cloud Storage
        upload_to_gcs(BUCKET_NAME, f"{service}/{parquet_file_name}", parquet_file_path)
        
        # Delete local CSV and Parquet files
        os.remove(file_path)
        os.remove(parquet_file_path)


web_to_gcs("fhv", 2019)   
web_to_gcs("green", 2019)   
web_to_gcs("green", 2020)   
web_to_gcs("yellow", 2019)   
web_to_gcs("yellow", 2020)   
        
