from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER")
ENDPOINT = os.getenv("AZURE_BLOB_ENDPOINT") 
# API_VERSION = "2012-02-12"

API_VERSION = "2025-11-05"

if not ACCOUNT_NAME or not ACCOUNT_KEY or not CONTAINER or not ENDPOINT:
    raise RuntimeError("Missing env vars")

print("Using endpoint:", ENDPOINT)

credential = {
    "account_name": ACCOUNT_NAME,
    "account_key": ACCOUNT_KEY
}

blob_service = BlobServiceClient(
    account_url=ENDPOINT,
    credential=credential,
    api_version=API_VERSION
)



container_client = blob_service.get_container_client(CONTAINER)










# import os
# from azure.identity import ManagedIdentityCredential
# from azure.storage.blob import BlobServiceClient
# from azure.core.pipeline.policies import UserAgentPolicy


# # Create your credential you want to use
# mi_credential = ManagedIdentityCredential()

# account_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# # Set up user-agent override
# class NoUserAgentPolicy(UserAgentPolicy):
#     def on_request(self, request):
#         pass

# # Create the BlobServiceClient object
# blob_service_client = BlobServiceClient(account_url, credential=mi_credential, user_agent_policy=NoUserAgentPolicy())

# container_client = blob_service_client.get_container_client(container=CONTAINER) 