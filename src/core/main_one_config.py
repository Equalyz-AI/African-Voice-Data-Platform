from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER")
ENDPOINT = os.getenv("AZURE_BLOB_ENDPOINT") 
API_VERSION = "2025-11-05"

# API_VERSION = "2025-11-05"

# if not ACCOUNT_NAME or not ACCOUNT_KEY or not CONTAINER or not ENDPOINT:
#     raise RuntimeError("Missing env vars")

# print("Using endpoint:", ENDPOINT)

# credential = {
#     "account_name": ACCOUNT_NAME,
#     "account_key": ACCOUNT_KEY
# }

# blob_service = BlobServiceClient(
#     account_url=ENDPOINT,
#     credential=credential,
#     api_version=API_VERSION
# )



# container_client = blob_service.get_container_client(CONTAINER)










# import os
# from azure.identity import ManagedIdentityCredential
# from azure.storage.blob import BlobServiceClient
# from azure.core.pipeline.policies import UserAgentPolicy


# # Create your credential you want to use
# mi_credential = ManagedIdentityCredential()

# account_url = ENDPOINT

# # Set up user-agent override
# class NoUserAgentPolicy(UserAgentPolicy):
#     def on_request(self, request):
#         pass

# # Create the BlobServiceClient object
# blob_service_client = BlobServiceClient(account_url, credential=mi_credential, user_agent_policy=NoUserAgentPolicy())

# container_client = blob_service_client.get_container_client(container=CONTAINER) 




import os
import logging
from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# ------------------ CONFIG ------------------
ENDPOINT = os.environ.get("AZURE_BLOB_ENDPOINT", "https://dsnvoiceaisa.blob.ng2.stack01.mdx-i.com")
CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "dsnvoiceaisa")
ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")  # Only needed locally
USE_MANAGED_IDENTITY = os.environ.get("USE_MANAGED_IDENTITY", "False").lower() == "true"
# --------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_blob_service_client():
    """
    Returns a BlobServiceClient using:
    - ManagedIdentityCredential if on Azure VM/Service and USE_MANAGED_IDENTITY=True
    - Else falls back to account key (for local dev)
    """
    if USE_MANAGED_IDENTITY:
        try:
            logger.info("Trying ManagedIdentityCredential...")
            credential = ManagedIdentityCredential()
            client = BlobServiceClient(account_url=ENDPOINT, credential=credential)
            # Test connection
            _ = client.get_service_properties()
            logger.info("ManagedIdentityCredential successful!")
            return client
        except Exception as e:
            logger.warning(f"ManagedIdentityCredential failed: {e}. Falling back to account key.")

    if not ACCOUNT_KEY:
        raise ValueError(
            "No ACCOUNT_KEY found in environment. Set AZURE_ACCOUNT_KEY for local testing."
        )
    logger.info("Using account key credential for BlobServiceClient.")
    credential = {
        "account_name": ACCOUNT_NAME,
        "account_key": ACCOUNT_KEY
    }
    return BlobServiceClient(
        account_url=ENDPOINT, 
        credential=credential,
        api_version="2019-07-07"
    )


# Create container client
blob_service_client = get_blob_service_client()
container_client = blob_service_client.get_container_client(CONTAINER)
