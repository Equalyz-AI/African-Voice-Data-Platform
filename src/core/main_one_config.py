from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential
from dotenv import load_dotenv
import os, logging


load_dotenv()


# ------------------ CONFIG ------------------
ACCOUNT_NAME =  os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
ENDPOINT = os.environ.get("AZURE_BLOB_ENDPOINT", "https://dsnvoiceaisa.blob.ng2.stack01.mdx-i.com")
CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "dsnvoiceaisa")
ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
USE_MANAGED_IDENTITY = os.environ.get("USE_MANAGED_IDENTITY", "False").lower() == "true"
API_VERSION = "2019-07-07"

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
        api_version=API_VERSION
    )


# Create container client
blob_service_client = get_blob_service_client()
container_client = blob_service_client.get_container_client(CONTAINER)
