# CSV Ingestion & Processing Pipeline

This module manages the bulk uploading, validation, splitting, and ingestion of Story and Discussion CSV reports.

---

## 🏗️ Architecture & Data Flow

You can process CSV files in either **Real-Time** or **Batch** mode depending on your server resources and requirements.

### 1. Batch Mode Flowchart

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 1 — UPLOAD                                                       │
│                                                                         │
│  Client  ──POST /v1/upload/──▶  FastAPI  ──upload──▶  GCS Bucket        │
│                                   │                                     │
│                                   └──insert status='pending'──▶  csv_uploads DB │
└─────────────────────────────────────────────────────────────────────────┘
                    ↓  (9:10 PM IST cron)
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 2 — INGESTION  [CsvBatchProcessingWorkflow]                      │
│                                                                         │
│  csv_fetch_and_validate_activity                                        │
│    ├── Fetch CSV from GCS                                               │
│    ├── Validate CSV column headers                                      │
│    ├── INVALID ──▶ status='on_hold' (stop)                              │
│    └── VALID   ──▶ status='in_progress'                                 │
│                                                                         │
│  csv_push_to_kafka_activity                                             │
│    └── Split CSV into rows ──▶ Kafka (analytics.ingestion.raw)          │
│                                    │                                    │
│                             IngestionConsumer                           │
│                                    └── Insert rows ──▶ submissions DB   │
│                                              status='pending'           │
│  csv_update_status_activity                                             │
│    └── status='success' ──▶ csv_uploads DB                              │
└─────────────────────────────────────────────────────────────────────────┘
                    ↓  (9:15 PM IST cron)
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 3 — ANALYSIS  [BatchProcessingWorkflow]                          │
│                                                                         │
│  Fetch all pending submissions                                          │
│    └──▶ ConfigDrivenProcessingWorkflow (per submission)                 │
│              ├── Activity: PII & Abusive Language Masking               │
│              ├── Activity: Thematic Classification                      │
│              └── status='success' ──▶ submissions DB                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2. Real-Time Mode Flowchart

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 1 — UPLOAD & DISPATCH                                            │
│                                                                         │
│  Client  ──POST /v1/upload/──▶  FastAPI  ──upload──▶  GCS Bucket        │
│                                   │                                     │
│                                   ├── insert status='pending' ──▶ csv_uploads DB │
│                                   └── start ──▶ CsvProcessingWorkflow   │
└─────────────────────────────────────────────────────────────────────────┘
                    ↓  (immediately)
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 2 — INGESTION  [CsvProcessingWorkflow]                           │
│                                                                         │
│  csv_fetch_and_validate_activity                                        │
│    ├── Fetch CSV from GCS                                               │
│    ├── Validate CSV column headers                                      │
│    ├── INVALID ──▶ status='on_hold' (stop)                              │
│    └── VALID   ──▶ status='in_progress'                                 │
│                                                                         │
│  csv_push_to_kafka_activity                                             │
│    └── Split CSV into rows ──▶ Kafka (analytics.ingestion.raw)          │
│                                    │                                    │
│                             IngestionConsumer                           │
│                                    ├── Insert rows ──▶ submissions DB   │
│                                    │         status='processing'        │
│                                    └── trigger ──▶ ConfigDrivenProcessingWorkflow │
│                                                        │                │
│                                          ┌─────────────┴──────────────┐ │
│                                          │  Per Submission             │ │
│                                          │  ├── PII Masking Activity   │ │
│                                          │  ├── Thematic Analysis      │ │
│                                          │  └── status='success'       │ │
│                                          └─────────────────────────────┘ │
│  csv_update_status_activity                                             │
│    └── status='success' ──▶ csv_uploads DB                              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Orchestration Modes

The pipeline supports two execution modes governed by the `PROCESSING_MODE` environment variable in your `.env` file:

### 1. Real-Time Mode (`PROCESSING_MODE=real-time`)
*   **Trigger**: A successful API upload immediately starts a `CsvProcessingWorkflow` run for the upload record.
*   **Ingestion**: The CSV is immediately downloaded from GCS, split into rows, and pushed to Kafka.
*   **Consumer**: The Kafka consumer reads the row events, inserts them into PostgreSQL, and immediately kicks off a `ConfigDrivenProcessingWorkflow` for each individual row.
*   **Result**: Submissions are masked, analyzed, and completed within seconds of uploading.

### 2. Batch Mode (`PROCESSING_MODE=batch`)
*   **Trigger**: A successful API upload simply saves the file in GCS and registers it as `pending` in the `csv_uploads` table. No workflows are run immediately.
*   **Ingestion Schedule (`CSV_SCHEDULE_CRON_TIME`, default 9:10 PM IST)**: `CsvBatchProcessingWorkflow` triggers, fetches all pending CSV uploads, validates columns, and pushes each row to Kafka. Submissions land in the DB as `pending`.
*   **Analysis Schedule (`BATCH_SCHEDULE_CRON`, default 9:15 PM IST)**: `BatchProcessingWorkflow` triggers, queries all `pending` submissions, and fans out `ConfigDrivenProcessingWorkflow` for each one in parallel.

---

## 🔌 API Endpoints

### 1. Upload CSV Report
*   **Endpoint**: `POST /v1/upload/`
*   **Headers**: `Authorization: Bearer <AUTH_TOKEN>`
*   **Body (Form-Data)**:
    *   `report_type`: Either `story` or `discussion` (case-insensitive)
    *   `program_name`: The name of the target program
    *   `leader_category`: The category of the target leaders
    *   `tenant_code`: The identifier for the tenant (defaults to `mitra`)
    *   `file`: The `.csv` file upload
*   **Behavior**: Validates columns, runs a duplicate file check, saves to GCS, and inserts a pending row into the tracking table.
*   **Example curl Request**:
    ```bash
    curl -i -X POST http://localhost:8000/v1/upload/ \
      -H "Authorization: Bearer dummy-analytics-auth-token-2026" \
      -F "report_type=discussion" \
      -F "program_name=My Program" \
      -F "leader_category=District Leader" \
      -F "tenant_code=mitra" \
      -F "file=@sample.csv"
    ```

### 2. Manual Process Override
*   **Endpoint**: `POST /v1/process/csv/{record_id}`
*   **Behavior**: Instantly triggers the `CsvProcessingWorkflow` for a specific record ID, bypassing the scheduled cron time. Useful for retrying `on_hold` files or running testing immediately.

---

## ⚙️ Configuration Variables

The following parameters in `.env` govern this pipeline:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PROCESSING_MODE` | Ingestion mode (`real-time` or `batch`) | `real-time` |
| `CSV_SCHEDULE_CRON_TIME` | **Batch only.** UTC cron when `CsvBatchProcessingWorkflow` runs — fetches & pushes pending CSV uploads to Kafka. Default is 9:10 PM IST. | `40 15 * * *` |
| `BATCH_SCHEDULE_CRON` | **Batch only.** UTC cron when `BatchProcessingWorkflow` runs — processes pending submissions (PII + thematic). Default is 9:15 PM IST. | `45 15 * * *` |
| `BATCH_SIZE` | Max pending submissions fetched per chunk in `BatchProcessingWorkflow` | `100` |
| `STORY_CSV_COLUMN` | JSON Array of columns expected for Story reports | `["id","Title", ...]` |
| `DISCUSSION_CSV_COLUMN` | JSON Array of columns expected for Discussion reports | `["id","Title", ...]` |
| `BUCKET_NAME` | Target GCS bucket for CSV uploads | `dev-sg-dashboard` |

---

## 📅 Staggered Schedule Coordination

In batch mode, two separate Temporal schedules are registered at startup. They are staggered by 5 minutes to guarantee that CSV ingestion always completes before the analysis phase begins:

| Variable | Cron | IST Time | Purpose | Workflow |
| :--- | :--- | :--- | :--- | :--- |
| `CSV_SCHEDULE_CRON_TIME` | `40 15 * * *` | **9:10 PM** | Download pending CSVs from GCS, split rows, push to Kafka → `pending` submissions in DB | `CsvBatchProcessingWorkflow` |
| `BATCH_SCHEDULE_CRON` | `45 15 * * *` | **9:15 PM** | Pick up all `pending` submissions and run PII masking + thematic classification | `BatchProcessingWorkflow` |

> **Why staggered?** If both ran at the same time, the analysis workflow would find zero pending submissions because the Kafka consumer hasn't had time to insert rows yet. The 5-minute gap ensures all CSV rows are committed to the database before analysis starts.

---

## 📊 Database Schema

### `csv_uploads` Table
Tracks uploaded raw files and their validation status:
```sql
CREATE TABLE csv_uploads (
    id                     SERIAL PRIMARY KEY,
    report_type            VARCHAR(100) NOT NULL,
    program_name           VARCHAR(255),
    leader_category        VARCHAR(255),
    file_name              VARCHAR(500),
    file_size              BIGINT,
    cloud_storage_path     TEXT NOT NULL,
    meta_data              JSONB DEFAULT '{}'::jsonb,
    status                 VARCHAR(20) NOT NULL DEFAULT 'pending' 
                             CHECK (status IN ('pending', 'in_progress', 'success', 'on_hold')),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
*   `pending`: File is valid and queued for scheduled batch processing.
*   `on_hold`: File failed validation (check `meta_data.validation_errors` for details) or encountered fetching errors.
*   `in_progress`: The batch script is currently processing and pushing CSV rows.
*   `success`: Ingestion succeeded, rows are written to `submissions`.
