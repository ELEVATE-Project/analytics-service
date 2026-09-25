# Multicloud Object Storage

The analytics service ships with a provider-agnostic storage abstraction (`app/services/storage/`). A single `STORAGE_PROVIDER` environment variable switches the entire service — CSV uploads, blurred image uploads, and signed URL generation — between **GCP**, **AWS S3**, **OCI Object Storage**, and **Azure Blob Storage**.

No application code changes are needed when switching providers. All routing happens inside the factory.

---

## Architecture overview

```
app/services/storage/
├── __init__.py          # re-exports: get_object_storage, StoredObject, AccessMode, errors, resolve_url
├── base.py              # ObjectStorage ABC + resolve_url() helper
├── models.py            # StoredObject dataclass, AccessMode enum (PUBLIC | PRIVATE)
├── errors.py            # StorageError, StorageNotFoundError, StoragePermissionError, StorageTransientError
├── factory.py           # get_object_storage() — reads STORAGE_PROVIDER, returns the right adapter
├── gcp.py               # GcpStorage   — google-cloud-storage
├── aws_s3.py            # AwsS3Storage — boto3
├── azure.py             # AzureStorage — azure-storage-blob
└── oci.py               # OciStorage   — oci SDK
```

### Two-bucket model

Every provider uses **two separate buckets**:

| Setting | Purpose | Access |
|---|---|---|
| `STORAGE_PUBLIC_BUCKET` | Blurred images served to end-users | Public read (bucket-level policy) |
| `STORAGE_PRIVATE_BUCKET` | Uploaded CSVs, internal data | Private only — access via signed URL |

`AccessMode.PUBLIC` → public bucket · `AccessMode.PRIVATE` → private bucket. The factory enforces this at every upload and download call automatically.

### Factory wiring

```python
from app.services.storage.factory import get_object_storage
from app.services.storage.models import AccessMode

storage = get_object_storage()                                     # reads STORAGE_PROVIDER from settings
obj     = storage.upload_bytes(data, "path/file.csv",
                               content_type="text/csv",
                               access_mode=AccessMode.PRIVATE)    # → STORAGE_PRIVATE_BUCKET
url     = storage.generate_access_url(obj.key, expires_in_seconds=3600,
                                      access_mode=AccessMode.PRIVATE)
```

---

## Quick-start: switch providers

1. Set `STORAGE_PROVIDER` in your `.env` (or environment).
2. Populate the credentials block for that provider (see each section below).
3. Set `STORAGE_PUBLIC_BUCKET` and `STORAGE_PRIVATE_BUCKET` to your bucket names.
4. Restart the service. No code changes required.

```
STORAGE_PROVIDER=gcp   # gcp | aws | azure | oci
STORAGE_PUBLIC_BUCKET=my-public-images-bucket
STORAGE_PRIVATE_BUCKET=my-private-csv-bucket
```

---

## Provider setup guides

---

### GCP Cloud Storage (default)

**Required env vars**

```dotenv
STORAGE_PROVIDER=gcp
STORAGE_PUBLIC_BUCKET=your-public-bucket
STORAGE_PRIVATE_BUCKET=your-private-bucket

# GCP service account credentials (same fields as the JSON key file)
TYPE=service_account
PROJECT_ID=your-gcp-project-id
PRIVATE_KEY_ID=abc123...
PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
CLIENT_EMAIL=your-sa@your-project.iam.gserviceaccount.com
CLIENT_ID=1234567890
AUTH_URI=https://accounts.google.com/o/oauth2/auth
TOKEN_URI=https://oauth2.googleapis.com/token
AUTH_PROVIDER_X509_CERT_URL=https://www.googleapis.com/oauth2/v1/certs
CLIENT_X509_CERT_URL=https://www.googleapis.com/robot/v1/metadata/x509/...
UNIVERSE_DOMAIN=googleapis.com
```

**How to obtain credentials**

1. Open [Google Cloud Console](https://console.cloud.google.com) → **IAM & Admin → Service Accounts**.
2. Click **Create Service Account** → give it a name (e.g. `analytics-storage`).
3. Assign the following roles:
   - `Storage Object Admin` on the private bucket (for CSV read/write).
   - `Storage Object Viewer` or `Storage Object Admin` on the public bucket (for image uploads).
4. Click the service account → **Keys → Add Key → Create new key → JSON**.
5. Download the JSON file. Copy each field into your `.env`:
   - `private_key` → `PRIVATE_KEY` (keep the `\n` newlines as-is, wrap the whole value in double quotes)
   - `client_email` → `CLIENT_EMAIL`, etc.

**Bucket setup**

```bash
# Create buckets (replace PROJECT and REGION)
gsutil mb -p PROJECT -l REGION gs://your-public-bucket
gsutil mb -p PROJECT -l REGION gs://your-private-bucket

# Make the public bucket world-readable
gsutil iam ch allUsers:objectViewer gs://your-public-bucket
```

**Signed URLs** are generated using the service account key embedded in settings — no extra setup needed.

---

### AWS S3

**Required env vars**

```dotenv
STORAGE_PROVIDER=aws
STORAGE_PUBLIC_BUCKET=your-public-s3-bucket
STORAGE_PRIVATE_BUCKET=your-private-s3-bucket
STORAGE_REGION=us-east-1

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_SESSION_TOKEN=               # leave blank unless using temporary credentials (STS/IAM role)
AWS_DEFAULT_REGION=us-east-1
```

**How to obtain credentials**

1. Open [AWS IAM Console](https://console.aws.amazon.com/iam) → **Users → Create user**.
2. Attach the **AmazonS3FullAccess** policy (or a custom policy — see below).
3. Click the user → **Security credentials → Create access key → Application running outside AWS**.
4. Copy the **Access key ID** and **Secret access key** into your `.env`.

**Minimal IAM policy (least-privilege)**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::your-public-s3-bucket/*",
        "arn:aws:s3:::your-private-s3-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "s3:GeneratePresignedUrl",
      "Resource": "arn:aws:s3:::your-private-s3-bucket/*"
    }
  ]
}
```

**Bucket setup**

```bash
# Create buckets
aws s3 mb s3://your-public-s3-bucket --region us-east-1
aws s3 mb s3://your-private-s3-bucket --region us-east-1

# Make the public bucket world-readable (add a bucket policy)
aws s3api put-bucket-policy --bucket your-public-s3-bucket --policy '{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::your-public-s3-bucket/*"
  }]
}'
```

> **Note**: If your account has **Block Public Access** enabled at the account level, you must disable it for the public bucket in the S3 console under **Block public access (bucket settings)** before the policy above takes effect.

**Signed URLs** are generated via `boto3.generate_presigned_url` — no extra setup needed beyond the IAM policy above.

---

### OCI Object Storage

**Required env vars**

```dotenv
STORAGE_PROVIDER=oci
STORAGE_PUBLIC_BUCKET=your-public-oci-bucket
STORAGE_PRIVATE_BUCKET=your-private-oci-bucket

OCI_NAMESPACE=your-tenancy-namespace
OCI_CONFIG_FILE=~/.oci/config
OCI_CONFIG_PROFILE=DEFAULT
OCI_REGION=us-ashburn-1
```

**How to obtain credentials**

1. Install the OCI CLI: `pip install oci-cli` then run `oci setup config`.
2. This creates `~/.oci/config` and generates an API signing key pair automatically. The wizard will also guide you to upload the public key to OCI Console.
3. Alternatively, do it manually:
   - Open [OCI Console](https://cloud.oracle.com) → top-right avatar → **User Settings → API Keys → Add API Key**.
   - Generate a new key pair (or upload your existing public key).
   - Copy the **Config file snippet** shown after uploading — paste it into `~/.oci/config`.

Your `~/.oci/config` will look like:
```ini
[DEFAULT]
user=ocid1.user.oc1..aaa...
fingerprint=aa:bb:cc:dd:...
tenancy=ocid1.tenancy.oc1..aaa...
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
```

4. Find your **Object Storage Namespace**:
   ```bash
   oci os ns get
   # Returns: {"data": "your-namespace"}
   ```
   Set this as `OCI_NAMESPACE`.

**Bucket setup**

```bash
# Create the private bucket (no public access)
oci os bucket create \
  --compartment-id <compartment-ocid> \
  --name your-private-oci-bucket \
  --region us-ashburn-1

# Create the public bucket and enable public access
oci os bucket create \
  --compartment-id <compartment-ocid> \
  --name your-public-oci-bucket \
  --public-access-type ObjectRead \
  --region us-ashburn-1
```

**IAM policy** (add in OCI Console → Identity → Policies):
```
Allow group analytics-group to manage objects in compartment <compartment-name>
  where target.bucket.name = 'your-public-oci-bucket'
Allow group analytics-group to manage objects in compartment <compartment-name>
  where target.bucket.name = 'your-private-oci-bucket'
```

**Signed URLs (Pre-Authenticated Requests)** are created using the OCI SDK automatically when `generate_access_url` is called.

---

### Azure Blob Storage

**Required env vars**

```dotenv
STORAGE_PROVIDER=azure
STORAGE_PUBLIC_BUCKET=your-public-container     # Azure "containers" are equivalent to buckets
STORAGE_PRIVATE_BUCKET=your-private-container

AZURE_STORAGE_ACCOUNT_NAME=yourstorageaccount

# Option A — connection string (easiest for dev/test)
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net

# Option B — service principal (recommended for production)
AZURE_CLIENT_ID=your-app-client-id
AZURE_CLIENT_SECRET=your-app-client-secret
AZURE_TENANT_ID=your-azure-tenant-id
```

If both are set, the **connection string takes priority**.

**How to obtain credentials**

**Option A — Connection String (dev/test)**

1. Open [Azure Portal](https://portal.azure.com) → **Storage Accounts → your account → Security + Networking → Access keys**.
2. Copy **Connection string** under `key1` or `key2` → paste as `AZURE_STORAGE_CONNECTION_STRING`.

**Option B — Service Principal (production)**

1. In Azure Portal → **Azure Active Directory → App registrations → New registration**.
2. Name the app (e.g. `analytics-storage`) → Register.
3. Note the **Application (client) ID** → `AZURE_CLIENT_ID`.
4. Note the **Directory (tenant) ID** → `AZURE_TENANT_ID`.
5. Go to **Certificates & secrets → New client secret** → copy the secret value → `AZURE_CLIENT_SECRET`.
6. Assign the **Storage Blob Data Contributor** role to this app on each container:
   - Storage Account → **Containers → your-container → Access Control (IAM) → Add role assignment**.
   - Role: `Storage Blob Data Contributor` · Assign to: the app registered above.

**Container setup**

```bash
# Using Azure CLI
az storage container create \
  --account-name yourstorageaccount \
  --name your-private-container \
  --public-access off

# Public container — blobs are readable by anyone with the URL
az storage container create \
  --account-name yourstorageaccount \
  --name your-public-container \
  --public-access blob
```

**Signed URLs** (SAS tokens) are generated via `BlobClient.generate_sas()` automatically — no extra configuration needed.

---

---

## Environment variable reference

| Variable | Required for | Default | Description |
|---|---|---|---|
| `STORAGE_PROVIDER` | all | `gcp` | `gcp \| aws \| azure \| oci` |
| `STORAGE_PUBLIC_BUCKET` | all | — | Bucket for blurred images (public read) |
| `STORAGE_PRIVATE_BUCKET` | all | — | Bucket for uploaded CSVs (private) |
| `STORAGE_REGION` | aws, oci | — | Cloud region for bucket operations |
| `STORAGE_STORY_PREFIX` | all | `story_blurred_image` | Key prefix for story images |
| `STORAGE_DISCUSSION_PREFIX` | all | `discussion_blurred_image` | Key prefix for discussion images |
| `STORAGE_CSV_PREFIX` | all | `mitra_dashboard_api_output` | Key prefix for CSV uploads |
| `STORAGE_SIGNED_URL_TTL_SECONDS` | all | `3600` | Expiry for generated private URLs |
| `STORAGE_CONNECT_TIMEOUT_SECONDS` | all | `10` | Connection timeout |
| `STORAGE_READ_TIMEOUT_SECONDS` | all | `60` | Read timeout |
| `STORAGE_MAX_RETRIES` | all | `3` | Retry attempts on transient errors |
| `AWS_ACCESS_KEY_ID` | aws | — | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | aws | — | AWS secret key |
| `AWS_SESSION_TOKEN` | aws (STS only) | — | Temporary session token |
| `AWS_DEFAULT_REGION` | aws | — | AWS region fallback |
| `OCI_NAMESPACE` | oci | — | Object Storage tenancy namespace |
| `OCI_CONFIG_FILE` | oci | `~/.oci/config` | Path to OCI config file |
| `OCI_CONFIG_PROFILE` | oci | `DEFAULT` | Profile name in config file |
| `OCI_REGION` | oci | — | OCI region identifier |
| `AZURE_STORAGE_ACCOUNT_NAME` | azure | — | Azure storage account name |
| `AZURE_STORAGE_CONNECTION_STRING` | azure (Option A) | — | Full Azure connection string |
| `AZURE_STORAGE_CONTAINER` | azure | — | Default container name |
| `AZURE_CLIENT_ID` | azure (Option B) | — | Service principal client ID |
| `AZURE_CLIENT_SECRET` | azure (Option B) | — | Service principal secret |
| `AZURE_TENANT_ID` | azure (Option B) | — | Azure AD tenant ID |

---

## Error handling

All provider exceptions are normalised into a consistent hierarchy in `app/services/storage/errors.py`:

| Exception | When raised |
|---|---|
| `StorageNotFoundError` | Object or bucket does not exist |
| `StoragePermissionError` | Access denied / insufficient IAM permissions |
| `StorageTransientError` | Network timeout, throttle, retryable server error |
| `StorageError` | All other storage-layer failures |

Callers catch these provider-agnostic exceptions — no boto3 / GCS / OCI / Azure SDK exceptions leak out.

---

## Adding a new provider

1. Create `app/services/storage/<provider>.py` implementing the `ObjectStorage` ABC from `base.py`.  
   Required methods: `upload_file`, `upload_bytes`, `download_bytes`, `delete_object`, `generate_access_url`.
2. Register it in `factory.py` under a new `STORAGE_PROVIDER` string.
3. Add the provider's credentials to `config.py` and `.env.example`.
4. Add unit tests in `tests/unit_testing.py` following the `test_storage_*` pattern.

---

*analytics_service — `app/services/storage/` · `app/config.py` · `.env.example`*
