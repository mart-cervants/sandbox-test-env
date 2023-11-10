
class GcpBucket:
    def __init__(self, storage_client, bucket,  bucket_name: str):
        self.bucket_name = bucket_name
        self.storage_client = storage_client
        self.bucket = bucket


    def check_bucket_existence(self) -> bool:
        if self.bucket.exists():
            print(f"Bucket {self.bucket.name} already exists")
            return True
        else:
            return False

    def create_bucket(self, storage_class: str ='STANDARD', location: str ='us-central1'): 
        self.bucket.storage_class = storage_class

        if not self.check_bucket_existence():
            bucket = self.storage_client.create_bucket(self.bucket, location=location) 
            print(f'Bucket {bucket.name} successfully created.')




