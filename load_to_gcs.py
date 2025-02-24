import os
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
import gzip


#Change this to your bucket name
BUCKET_NAME = "dez-jimmyh-hw4"  


CREDENTIALS_FILE = os.getenv("GCP_CREDENTIALS") 
if CREDENTIALS_FILE:
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    # if the ENV variable hasn't set, use default way (gcloud auth)
    client = storage.Client()


BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)] 
DOWNLOAD_DIR = "./fhv_tripdata_2019"

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    """
    Download a .gz file, extract it to .csv, and return the extracted CSV file path.
    """
    gz_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")
    csv_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv")
    url = f"{BASE_URL}{month}.csv.gz"

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, gz_file_path)
        print(f"Downloaded: {gz_file_path}")
        # Extract .gz to .csv
        with gzip.open(gz_file_path, 'rb') as gz_file, open(csv_file_path, 'wb') as csv_file:
            shutil.copyfileobj(gz_file, csv_file)
        
        print(f"Extracted: {csv_file_path}")

        # Remove the .gz file after extraction
        os.remove(gz_file_path)
        print(f"Removed: {gz_file_path}")

        return csv_file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    '''
    Check whether the object is exist in GCS or not
    '''
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")