import json
from typing import Dict, Any, List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_TOPIC_INGESTION: str = Field(default="analytics.ingestion.raw")
    KAFKA_TOPIC_INGESTION_DLQ: str = Field(default="analytics.ingestion.raw.dlq")
    KAFKA_GROUP_ID: str = Field(default="analytics-ingestion-group")

    # Database Configuration
    DATABASE_URL: str = Field(default="postgresql://postgres:postgres@localhost:5432/temporal")

    # Orchestration Mode: 'real-time' or 'batch'
    PROCESSING_MODE: str = Field(default="real-time")
    BATCH_SCHEDULE_CRON: str = Field(default="0 20 * * *")
    # Max pending submissions fetched/fanned-out per chunk in BatchProcessingWorkflow —
    # keeps memory and concurrent child-workflow count bounded regardless of queue size.
    BATCH_SIZE: int = Field(default=100)

    # Deployment environment — defaults to the safe option ('production') so a
    # missing/unset value never accidentally allows destructive operations like
    # RESET_DB. Must be explicitly set to 'development' to opt into those.
    ENVIRONMENT: str = Field(default="production")

    # Optional full schema reset on startup — only takes effect when
    # ENVIRONMENT=development (see db.py's initialize_schema), so a stray
    # RESET_DB=true in a production config can't wipe a live database.
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

    # GCP Credentials
    TYPE: str = Field(default="service_account")
    PROJECT_ID: str = Field(default="")
    PRIVATE_KEY_ID: str = Field(default="")
    PRIVATE_KEY: str = Field(default="")
    CLIENT_EMAIL: str = Field(default="")
    CLIENT_ID: str = Field(default="")
    AUTH_URI: str = Field(default="")
    TOKEN_URI: str = Field(default="")
    AUTH_PROVIDER_X509_CERT_URL: str = Field(default="")
    CLIENT_X509_CERT_URL: str = Field(default="")
    UNIVERSE_DOMAIN: str = Field(default="googleapis.com")
    BUCKET_NAME: str = Field(default="")
    STORY_BLOB: str = Field(default="")
    DISCUSSION_BLOB: str = Field(default="")
    MEDIA_BASE_URL: str = Field(default="")

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
