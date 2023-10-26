from google.cloud.storage import Blob


class GcpObjets:
    def __init__(self, storage_client, bucket):
        self.storage_client = storage_client
        self.bucket = bucket


    def check_file_existence(self, blob: Blob) -> bool:
        if blob.exists():
            print(f"File {blob.name} already exists in bucket")   
            return True
        else:
            return False 

    def upload_file(self, source_file_name):

        destination_blob_name = "comments.csv"
        blob = self.bucket.blob(destination_blob_name)
        generation_match_precondition = 0

        if not self.check_file_existence(blob):
            blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
            print(f"File {source_file_name} uploaded to {destination_blob_name}.")