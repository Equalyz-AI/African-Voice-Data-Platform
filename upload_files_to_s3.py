import os
from src.download.s3_config import s3, BUCKET

s3 = s3

bucket_name = BUCKET
local_directory = os.path.join(os.getcwd(), 'samples')
s3_prefix = 'data'

for root, _, files in os.walk(local_directory):
    for file in files:
        local_path = os.path.join(root, file)
        s3_key = os.path.join(s3_prefix, os.path.relpath(local_path, local_directory)).replace('\\', '/')
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")



