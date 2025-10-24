import boto3

# ========= CONFIG ==========
OBS_BUCKET = "dsn"
OBS_PREFIX = ""    
# ===========================


session = boto3.session.Session()

obs_client = session.client(
    service_name="s3",
    region_name="cn-global-1",
    aws_access_key_id="TIEQAWBJ6ZJLTLOUIJPJ",
    aws_secret_access_key="TybqPKKpzrwQxcUl5aQaxU1CqHQTKWL3Exy63iDH",
    endpoint_url="https://obsv3.cn-global-1.gbbcloud.com"
)

def list_all_obs_folders(bucket_name, prefix='', indent=0):
    """Recursively list folders/files in OBS bucket (2 samples per level)."""
    paginator = obs_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        # === Files (only 2 samples) ===
        files = [obj['Key'] for obj in page.get('Contents', []) if not obj['Key'].endswith('/')]
        if files:
            for f in files[:2]:
                print('  ' * indent + f"📄 {f.split('/')[-1]}")
            if len(files) > 2:
                print('  ' * indent + f"... ({len(files) - 2} more files)")

        # === Folders (only 2 samples) ===
        folders = [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
        for folder in folders[:2]:
            folder_name = folder.rstrip('/').split('/')[-1]
            print('  ' * indent + f"📁 {folder_name}/")
            list_all_obs_folders(bucket_name, prefix=folder, indent=indent + 1)

        if len(folders) > 2:
            print('  ' * indent + f"... ({len(folders) - 2} more folders)")


# Run
list_all_obs_folders(OBS_BUCKET, OBS_PREFIX)







# def get_sample_files_from_prefix(bucket_name, prefix, sample_size=4):
#     import random

#     paginator = s3.get_paginator('list_objects_v2')
#     files = []

#     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#         for obj in page.get('Contents', []):
#             key = obj['Key']
#             if not key.endswith('/'):
#                 files.append(key)
#                 # Early stop if we have enough to sample from
#                 if len(files) >= sample_size * 2:
#                     break
#         if len(files) >= sample_size * 2:
#             break

#     if not files:
#         print(f"No files found under prefix: {prefix}")
#         return []

#     return random.sample(files, min(sample_size, len(files)))





# samples = get_sample_files_from_prefix('african-voice-audio-bucket', 'naija/spontaneous_without_transcript/')
# for s in samples:
#     print(f"🔹 {s}")