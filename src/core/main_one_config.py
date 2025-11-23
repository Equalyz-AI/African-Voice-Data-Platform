from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER")
ENDPOINT = os.getenv("AZURE_BLOB_ENDPOINT") 

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
    api_version="2021-04-10"
)

container_client = blob_service.get_container_client(CONTAINER)