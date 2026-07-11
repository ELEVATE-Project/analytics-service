import json
import logging
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account

from app.config import settings

logger = logging.getLogger("analytics_service.services.gcp_storage")

def get_gcp_credentials() -> service_account.Credentials:
    """
    Constructs the GCP credentials object from settings.
    """
    # Build dictionary from settings
    cred_dict = {
        "type": settings.TYPE,
        "project_id": settings.PROJECT_ID,
        "private_key_id": settings.PRIVATE_KEY_ID,
        "private_key": settings.PRIVATE_KEY.replace('\\n', '\n'), # Ensure newlines are correct
        "client_email": settings.CLIENT_EMAIL,
        "client_id": settings.CLIENT_ID,
        "auth_uri": settings.AUTH_URI,
        "token_uri": settings.TOKEN_URI,
        "auth_provider_x509_cert_url": settings.AUTH_PROVIDER_X509_CERT_URL,
        "client_x509_cert_url": settings.CLIENT_X509_CERT_URL,
        "universe_domain": settings.UNIVERSE_DOMAIN
    }
    
    # Optional: Log missing keys if any (excluding private_key for security)
    missing = [k for k, v in cred_dict.items() if not v and k != "private_key"]
    if missing:
        logger.warning(f"Missing GCP credentials fields in settings: {missing}")
        
    return service_account.Credentials.from_service_account_info(cred_dict)

def upload_to_gcp(local_file_path: str, blob_name: str) -> str:
    """
    Uploads a local file to the configured GCP bucket and returns its public URL.
    """
    if not settings.BUCKET_NAME:
        raise ValueError("BUCKET_NAME is not configured in settings.")
        
    path = Path(local_file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {local_file_path}")
        
    try:
        credentials = get_gcp_credentials()
        client = storage.Client(credentials=credentials, project=settings.PROJECT_ID)
        bucket = client.bucket(settings.BUCKET_NAME)
        
        # Upload
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path)
        
        logger.info(f"Successfully uploaded {local_file_path} to {settings.BUCKET_NAME}/{blob_name}")
        
        # Return the relative path
        # e.g., /bucket-name/blob-name
        return f"/{settings.BUCKET_NAME}/{blob_name}"
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to GCP bucket {settings.BUCKET_NAME}: {e}")
        raise
