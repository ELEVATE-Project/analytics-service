"""
Cloud bucket storage for raw CSV files.

Uses Google Cloud Storage with service account credentials loaded
from environment variables via app.config.settings.
"""

import io
import logging
import uuid

from google.cloud import storage
from google.oauth2 import service_account

from app.config import settings

logger = logging.getLogger("analytics_service.storage.gcs")

_client: storage.Client | None = None


def _get_client() -> storage.Client:
    """
    Lazily creates a GCS client authenticated via the service account.
    """
    global _client
    if _client is not None:
        return _client

    creds_dict = settings.get_gcs_credentials_dict()
    if creds_dict:
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        _client = storage.Client(
            credentials=credentials,
            project=settings.PROJECT_ID,
        )
        logger.info("GCS client initialised with service account: %s", settings.CLIENT_EMAIL)
    else:
        # Fallback: Application Default Credentials
        _client = storage.Client()
        logger.info("GCS client initialised with Application Default Credentials")

    return _client


def upload_csv(file_bytes: bytes, report_type: str, original_filename: str) -> str:
    """
    Upload raw CSV bytes to the configured GCS bucket.

    Returns the object key (cloud_storage_path) that gets stored in
    csv_upload.cloud_storage_path so we can fetch it later.

    Object key format: <csv_uploads_prefix>/<uuid>_<original_filename>
    """
    bucket_name = settings.BUCKET_NAME
    if not bucket_name:
        raise ValueError("BUCKET_NAME is not configured in settings.")

    prefix = settings.CSV_BLOB_UPLOADS.strip("/")
    object_key = f"{prefix}/{uuid.uuid4()}_{original_filename}"

    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    blob.upload_from_string(file_bytes, content_type="text/csv")

    logger.info("Uploaded CSV to gs://%s/%s", bucket_name, object_key)
    return object_key


def fetch_csv(bucket_path: str) -> io.BytesIO:
    """
    Fetch a CSV back from the GCS bucket as an in-memory file-like object.
    """
    bucket_name = settings.BUCKET_NAME
    if not bucket_name:
        raise ValueError("BUCKET_NAME is not configured in settings.")

    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(bucket_path)
    data = blob.download_as_bytes()

    logger.info("Fetched CSV from gs://%s/%s (%d bytes)", bucket_name, bucket_path, len(data))
    return io.BytesIO(data)
