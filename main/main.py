from first_stage.create_bucket import GcpBucket
from first_stage.upload_files_to_bucket import GcpObjets
from google.cloud import storage
from google.cloud.storage import Client, Blob

import os

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'./main/sandbox_data_key.json'

    BUCKET_NAME = "yt_statistics"
    SOURCE_FILE = r"../data/comments.csv"

    storage_client = storage.Client()
    bucket_client = storage_client.bucket(BUCKET_NAME)

    bucket = GcpBucket(storage_client, bucket_client, BUCKET_NAME)
    bucket.create_bucket()

    blob = GcpObjets(storage_client, bucket_client)
    blob.upload_file(SOURCE_FILE)