from app.config import settings
from .base import ObjectStorage
from .gcp import GcpStorage
from .aws_s3 import AwsS3Storage
from .azure import AzureStorage
from .oci import OciStorage
import threading

_storage_adapter: ObjectStorage | None = None
_storage_lock = threading.Lock()


def validate_storage_config(provider: str, settings_obj) -> None:
    # Both buckets must always be configured — the code routes objects to the
    # correct bucket based on AccessMode, so both must exist at startup.
    if not settings_obj.STORAGE_PUBLIC_BUCKET:
        raise ValueError("STORAGE_PUBLIC_BUCKET required")
    if not settings_obj.STORAGE_PRIVATE_BUCKET:
        raise ValueError("STORAGE_PRIVATE_BUCKET required")

    if provider == "aws":
        if not settings_obj.STORAGE_REGION:
            raise ValueError("STORAGE_REGION required for AWS")
    elif provider == "gcp":
        pass  # credentials come from the GCP service-account env vars
    elif provider == "oci":
        if not settings_obj.OCI_NAMESPACE:
            raise ValueError("OCI_NAMESPACE required for OCI")
    elif provider == "azure":
        if not (
            settings_obj.AZURE_STORAGE_ACCOUNT_NAME
            or settings_obj.AZURE_STORAGE_CONNECTION_STRING
        ):
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING required")


def clear_storage_cache() -> None:
    global _storage_adapter
    _storage_adapter = None


def get_object_storage() -> ObjectStorage:
    global _storage_adapter
    if _storage_adapter is not None:
        return _storage_adapter

    with _storage_lock:
        if _storage_adapter is not None:
            return _storage_adapter

    provider = settings.STORAGE_PROVIDER.lower()
    validate_storage_config(provider, settings)

    if provider == "gcp":
        _storage_adapter = GcpStorage(
            public_bucket  = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket = settings.STORAGE_PRIVATE_BUCKET,
        )
    elif provider == "aws":
        _storage_adapter = AwsS3Storage(
            public_bucket   = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket  = settings.STORAGE_PRIVATE_BUCKET,
            region          = settings.STORAGE_REGION,
            connect_timeout = settings.STORAGE_CONNECT_TIMEOUT_SECONDS,
            read_timeout    = settings.STORAGE_READ_TIMEOUT_SECONDS,
            max_retries     = settings.STORAGE_MAX_RETRIES,
            aws_access_key_id = settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token = settings.AWS_SESSION_TOKEN,
        )
    elif provider == "azure":
        _storage_adapter = AzureStorage(
            public_bucket     = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket    = settings.STORAGE_PRIVATE_BUCKET,
            connection_string = settings.AZURE_STORAGE_CONNECTION_STRING,
            account_name      = settings.AZURE_STORAGE_ACCOUNT_NAME,
            client_id         = settings.AZURE_CLIENT_ID,
            client_secret     = settings.AZURE_CLIENT_SECRET,
            tenant_id         = settings.AZURE_TENANT_ID,
        )
    elif provider == "oci":
        _storage_adapter = OciStorage(
            public_bucket  = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket = settings.STORAGE_PRIVATE_BUCKET,
            namespace      = settings.OCI_NAMESPACE,
            config_file    = settings.OCI_CONFIG_FILE,
            profile        = settings.OCI_CONFIG_PROFILE,
            region         = settings.OCI_REGION,
        )
    else:
        raise ValueError(f"Unsupported storage provider: {provider!r}. Must be one of: gcp, aws, azure, oci")

    return _storage_adapter
