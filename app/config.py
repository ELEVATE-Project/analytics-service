import json
from typing import Dict, Any, List, Optional
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
    BATCH_SIZE: int = Field(default=100, gt=0)

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

    # Kafka ingestion-time field validation schema (JSON strings loaded from env).
    # One entry per (create/update/delete) event type: "required" is a list of
    # dot-notation paths (e.g. "tags.state", "data.pdfUrls.original") that must be
    # present and non-empty; "optional" is informational only. "update" additionally
    # sets newValuesNoEmpty so every key actually present in newValues must be non-empty.
    STORY_KAFKA_SCHEMA: str = Field(
        default='{"create": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt", "tags.state", "tags.district", "tags.organization", "tags.programId", "tags.programName", "tags.leaderCategoryId", "tags.leaderCategoryName", "data.title", "data.designation", "data.submissionDate", "data.pdfUrls.original", "data.pdfUrls.masked", "data.transcriptLink", "data.challenges", "data.objective", "data.actionSteps", "data.impact", "data.duration", "data.blurb", "data.content"], "optional": ["data.imageUrls"]}, "update": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"], "newValuesNoEmpty": true}, "delete": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"]}}'
    )
    DISCUSSION_KAFKA_SCHEMA: str = Field(
        default='{"create": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt", "tags.state", "tags.district", "tags.organization", "tags.programId", "tags.programName", "tags.leaderCategoryId", "tags.leaderCategoryName", "data.title", "data.designation", "data.submissionDate", "data.pdfUrls.original", "data.pdfUrls.masked", "data.transcriptLink", "data.challenges", "data.solutions", "data.participantsData"], "optional": ["data.author", "data.language", "data.imageUrls"]}, "update": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"], "newValuesNoEmpty": true}, "delete": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"]}}'
    )

    # Thematic Classification Configuration
    MINIMUM_THEME_WORD_COUNT: int = Field(default=5)
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

    @field_validator("PROCESS_CONFIG_STORY", "PROCESS_CONFIG_DISCUSSION")
    @classmethod
    def validate_process_config_json(cls, v: str, info) -> str:
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, list):
                raise ValueError(f"{info.field_name} must be a valid JSON array of process steps.")
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for {info.field_name}: {e}") from e
        return v

    @field_validator("STORY_KAFKA_SCHEMA", "DISCUSSION_KAFKA_SCHEMA")
    @classmethod
    def validate_kafka_ingestion_schema_json(cls, v: str, info) -> str:
        try:
            parsed = json.loads(v)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for {info.field_name}: {e}") from e

        if not isinstance(parsed, dict) or not {"create", "update", "delete"} <= parsed.keys():
            raise ValueError(
                f"{info.field_name} must be a JSON object with 'create', 'update', and 'delete' keys."
            )

        # consumer.py's _validate_ingestion_schema unconditionally does
        # event_schema.get("required", []) on each section, so a malformed section
        # (e.g. a string instead of an object) must be rejected here rather than
        # crashing the consumer at message-processing time.
        for section_name in ("create", "update", "delete"):
            section = parsed[section_name]
            if not isinstance(section, dict):
                raise ValueError(
                    f"{info.field_name}.{section_name} must be a JSON object, got {type(section).__name__}."
                )
            for list_field in ("required", "optional"):
                if list_field in section:
                    field_value = section[list_field]
                    if not isinstance(field_value, list) or not all(isinstance(item, str) for item in field_value):
                        raise ValueError(
                            f"{info.field_name}.{section_name}.{list_field} must be a list of strings."
                        )
            if "newValuesNoEmpty" in section and not isinstance(section["newValuesNoEmpty"], bool):
                raise ValueError(
                    f"{info.field_name}.{section_name}.newValuesNoEmpty must be a boolean."
                )

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
        Raises ValueError if JSON parsing fails or if config is not a list.
        """
        normalized_type = submission_type.lower().strip()
        if "story" in normalized_type:
            raw_config = self.PROCESS_CONFIG_STORY
        elif "discussion" in normalized_type:
            raw_config = self.PROCESS_CONFIG_DISCUSSION
        else:
            # Fallback/Default config for unknown submission type
            return []

        try:
            parsed = json.loads(raw_config)
            if not isinstance(parsed, list):
                raise ValueError(
                    f"Process configuration for submission type {submission_type!r} must be a list, got {type(parsed).__name__}"
                )
            return parsed
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(
                f"Failed to parse process configuration JSON for submission type {submission_type!r}: {e}"
            ) from e

    def get_kafka_ingestion_schema(self, submission_type: str) -> Dict[str, Any]:
        """
        Dynamically returns the ingestion-time required-fields schema for the given
        submission type. Raises ValueError if JSON parsing fails, if the config is
        not a dict, or if the submission type doesn't match 'story' or 'discussion'.
        """
        normalized_type = submission_type.lower().strip() if isinstance(submission_type, str) else ""
        if "story" in normalized_type:
            raw_schema = self.STORY_KAFKA_SCHEMA
        elif "discussion" in normalized_type:
            raw_schema = self.DISCUSSION_KAFKA_SCHEMA
        else:
            raise ValueError(f"No Kafka ingestion schema defined for submission type {submission_type!r}")

        try:
            parsed = json.loads(raw_schema)
            if not isinstance(parsed, dict):
                raise ValueError(
                    f"Kafka ingestion schema for submission type {submission_type!r} must be a dict, got {type(parsed).__name__}"
                )
            return parsed
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(
                f"Failed to parse Kafka ingestion schema JSON for submission type {submission_type!r}: {e}"
            ) from e

# Singleton instance
settings = Settings()

