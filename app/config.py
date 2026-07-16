import json
from typing import Dict, Any, List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_TOPIC_INGESTION: str = Field(default="analytics.ingestion.raw")
    KAFKA_GROUP_ID: str = Field(default="analytics-ingestion-group")

    # Database Configuration
    DATABASE_URL: str = Field(default="postgresql://postgres:postgres@localhost:5432/temporal")

    # Orchestration Mode: 'real-time' or 'batch'
    PROCESSING_MODE: str = Field(default="real-time")
    BATCH_SCHEDULE_CRON: str = Field(default="0 20 * * *")

    # Optional full schema reset on startup
    RESET_DB: bool = Field(default=False)

    # Temporal Configuration
    TEMPORAL_HOST: str = Field(default="localhost:7233")
    TEMPORAL_QUEUE: str = Field(default="analytics-processing-queue")

    # LLM / OpenRouter Configuration
    OPENROUTER_API_KEY: str = Field(default="")
    OPENROUTER_MODEL: str = Field(default="nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free")
    OPENROUTER_BASE_URL: str = Field(default="https://openrouter.ai/api/v1")
    LLM_TEMPERATURE: float = Field(default=0.0)
    LLM_MAX_TOKENS: int = Field(default=2048)
    LLM_TIMEOUT_SECONDS: int = Field(default=60)

    # Story Rating Configuration
    MAX_PDF_TEXT_CHARS: int = Field(default=40000)

    # Scoped Ingestion Pipeline Configuration (JSON strings loaded from env)
    PROCESS_CONFIG_STORY: str = Field(
        default='[{"name": "pii_and_abusive_language_detection", "columns": ["objective"]}, {"name": "thematic_classification", "columns": ["objective"]}, {"name": "story_rating"}]'
    )
    PROCESS_CONFIG_DISCUSSION: str = Field(
        default='[{"name": "pii_and_abusive_language_detection", "columns": ["challenges"]}, {"name": "thematic_classification", "columns": ["challenges"]}]'
    )

    # Thematic Classification Configuration
    MINIMUM_THEME_WORD_COUNT: int = Field(default=5)
    THEMATIC_STATEMENT_DELIMITER: str = Field(default="|")
    EMBEDDING_MODEL_NAME: str = Field(default="all-MiniLM-L6-v2")
    SIMILARITY_SCORE_THRESHOLD: float = Field(default=0.65)
    LLM_CONFIDENCE_SCORE_THRESHOLD: float = Field(default=0.8)

    # Auth Token Configuration
    AUTH_TOKEN: str = Field(description="Bearer token for API authentication. Must be set via environment variable.")
    MAX_CSV_UPLOAD_BYTES: int = Field(default=10485760) # 10MB
    CSV_SCHEDULE_CRON_TIME: str = Field(default="0 20 * * *")
    KAFKA_TOPIC_CSV_ROWS: str = Field(default="analytics.ingestion.raw")

    # CSV Column Schemas (stored as JSON arrays of column names)
    STORY_CSV_COLUMN: str = Field(
        default='["id","Title","User name","Designation","Location","District","Organization","Report Created At","Objective","Challenges","Action Steps","Impact","Duration","Blurb","masked_blurb","Content","masked_content","Images","Pdf","Transcript Link"]'
    )
    DISCUSSION_CSV_COLUMN: str = Field(
        default='["id","Title","User name","User Location","District","Participant Count","Men","Women","Children","Date of Discussion","Organization","Challenges","Solutions","Author","Language","Report Created At","Transcript Link","Image Urls","PDF Urls"]'
    )

    # GCP Credentials & Cloud Storage Config
    TYPE: str = Field(default="service_account")
    PROJECT_ID: str = Field(default="")
    PRIVATE_KEY_ID: str = Field(default="")
    PRIVATE_KEY: str = Field(default="")
    CLIENT_EMAIL: str = Field(default="")
    CLIENT_ID: str = Field(default="")
    AUTH_URI: str = Field(default="https://accounts.google.com/o/oauth2/auth")
    TOKEN_URI: str = Field(default="https://oauth2.googleapis.com/token")
    AUTH_PROVIDER_X509_CERT_URL: str = Field(default="https://www.googleapis.com/oauth2/v1/certs")
    CLIENT_X509_CERT_URL: str = Field(default="")
    UNIVERSE_DOMAIN: str = Field(default="googleapis.com")
    BUCKET_NAME: str = Field(description="GCS bucket name for CSV uploads. Must be set via environment variable.")
    STORY_BLOB: str = Field(default="story_blurred_image")
    DISCUSSION_BLOB: str = Field(default="dicussion_blurred_image")
    MEDIA_BASE_URL: str = Field(default="https://mohini-static.shikshalokam.org")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @field_validator("DATABASE_URL")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        # asyncpg requires postgresql:// or postgres:// scheme
        if v.startswith("postgresql+asyncpg://"):
            return v.replace("postgresql+asyncpg://", "postgresql://")
        return v

    def get_gcs_credentials_dict(self) -> Optional[Dict[str, str]]:
        """
        Assembles the individual credentials into the JSON dict that
        google.cloud.storage.Client.from_service_account_info() expects.
        Returns None if required fields are missing.
        """
        if not self.CLIENT_EMAIL or not self.PRIVATE_KEY:
            return None
        return {
            "type": self.TYPE,
            "project_id": self.PROJECT_ID,
            "private_key_id": self.PRIVATE_KEY_ID,
            "private_key": self.PRIVATE_KEY.replace("\\n", "\n").replace('"', ''),
            "client_email": self.CLIENT_EMAIL,
            "client_id": self.CLIENT_ID,
            "auth_uri": self.AUTH_URI,
            "token_uri": self.TOKEN_URI,
            "auth_provider_x509_cert_url": self.AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": self.CLIENT_X509_CERT_URL,
            "universe_domain": self.UNIVERSE_DOMAIN,
        }

    def get_process_config(self, submission_type: str) -> List[Dict[str, Any]]:
        """
        Dynamically returns the process list configuration based on submission type.
        """
        raw_config = ""
        normalized_type = submission_type.lower().strip()
        if "story" in normalized_type:
            raw_config = self.PROCESS_CONFIG_STORY
        elif "discussion" in normalized_type:
            raw_config = self.PROCESS_CONFIG_DISCUSSION
        else:
            # Fallback/Default config
            return []

        try:
            return json.loads(raw_config)
        except (json.JSONDecodeError, TypeError):
            return []

# Singleton instance
settings = Settings()

