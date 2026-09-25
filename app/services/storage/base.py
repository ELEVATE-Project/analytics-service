from abc import ABC, abstractmethod
from .models import StoredObject, AccessMode
import urllib.parse


class ObjectStorage(ABC):
    """
    Provider-neutral storage interface.

    Every operation that touches a bucket accepts an ``access_mode`` so the
    adapter can route the call to the correct bucket:

      AccessMode.PUBLIC  → public bucket  (blurred images; publicly readable)
      AccessMode.PRIVATE → private bucket (internal CSVs; not publicly readable)

    Call sites MUST always pass ``access_mode`` explicitly — never rely on the
    default — so the bucket routing is obvious during code review.
    """

    public_bucket: str
    private_bucket: str

    def _bucket_for(self, access_mode: AccessMode) -> str:
        return self.public_bucket if access_mode == AccessMode.PUBLIC else self.private_bucket

    def generate_public_url(self, object_key: str) -> str:
        """
        Returns a relative public URL (e.g., /{bucket}/{key}) for the given object key.
        """
        encoded_key = urllib.parse.quote(object_key, safe="/")
        return f"/{self.public_bucket}/{encoded_key}"

    @abstractmethod
    def upload_file(
        self,
        local_file_path: str,
        object_key: str,
        content_type: str | None = None,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject: ...

    @abstractmethod
    def upload_bytes(
        self,
        data: bytes,
        object_key: str,
        content_type: str | None = None,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject: ...

    @abstractmethod
    def download_bytes(
        self,
        object_key: str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes: ...

    @abstractmethod
    def delete_object(
        self,
        object_key: str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> None: ...

    @abstractmethod
    def generate_access_url(
        self,
        object_key: str,
        expires_in_seconds: int,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> str: ...

# URL helpers
def resolve_url(
    stored_object: StoredObject,
    storage: ObjectStorage,
    expires_in_seconds: int = 3600,
) -> str:
    """
    Return the best URL for accessing ``stored_object``.

    PUBLIC  → direct public URL (no expiry, no auth required).
    PRIVATE → pre-signed / signed URL valid for ``expires_in_seconds`` seconds.
    """
    if stored_object.access_mode == AccessMode.PUBLIC:
        return storage.generate_public_url(stored_object.key)
    return storage.generate_access_url(
        stored_object.key,
        expires_in_seconds = expires_in_seconds,
        access_mode = AccessMode.PRIVATE,
    )
