# CSV Ingestion & Processing Pipeline

This module manages the bulk uploading, validation, splitting, and ingestion of Story and Discussion CSV reports.

---

## 🏗️ Architecture & Data Flow

You can process CSV files in either **Real-Time** or **Batch** mode depending on your server resources and requirements.

### 1. Batch Mode Flowchart

```mermaid
graph TD
    %% Upload Phase
    A[Client Uploads CSV] -->|POST /v1/upload/| B[FastAPI Web Server]
    B -->|Upload CSV to Google Cloud Storage| C[Upload to GCS]
    C -->|Create row status='pending'| D[csv_uploads Table]

    %% Ingestion Phase (Staggered Run 1)
    G[Temporal Schedule: 9:10 PM IST] -->|Triggers| H[CsvBatchProcessingWorkflow]
    H -->|csv_fetch_and_validate_activity| I[Download CSV from GCS & Validate Columns]
    I -->|Is CSV Valid?| J{Valid?}
    J -->|No| K[Mark status='on_hold' in csv_uploads]
    J -->|Yes| L[csv_push_to_kafka_activity: Split CSV into rows & push to Kafka]
    L -->|Kafka Topic: analytics.ingestion.raw| M[IngestionConsumer]
    M -->|Insert row to DB| N[submissions table status='pending']
    H -->|Update status='success'| O[csv_uploads Table]

    %% Analysis Phase (Staggered Run 2)
    P[Temporal Schedule: 9:15 PM IST] -->|Triggers| Q[BatchProcessingWorkflow]
    Q -->|Fetch pending submissions| R[ConfigDrivenProcessingWorkflow]
    R -->|Activity 1: PII Masking| S[Mask Abusive & Sensitive content]
    R -->|Activity 2: Thematic Analysis| T[Map Statements to Taxonomy]
    R -->|Update status='success'| U[submissions table]
```

### 2. Real-Time Mode Flowchart

```mermaid
graph TD
    %% Upload Phase
    A[Client Uploads CSV] -->|POST /v1/upload/| B[FastAPI Web Server]
    B -->|Upload CSV to Google Cloud Storage| C[Upload to GCS]
    C -->|Create row status='pending'| D[csv_uploads Table]
    B -->|Start Workflow| E[CsvProcessingWorkflow]

    %% Processing Phase
    E -->|csv_fetch_and_validate_activity| F[Download CSV from GCS & Validate Columns]
    F -->|Is CSV Valid?| G{Valid?}
    G -->|No| H[Mark status='on_hold' in csv_uploads]
    G -->|Yes| I[csv_push_to_kafka_activity: Split CSV into rows & push to Kafka]
    I -->|Kafka Topic: analytics.ingestion.raw| J[IngestionConsumer]
    J -->|Insert row to DB| K[submissions table status='processing']
    J -->|Trigger| L[ConfigDrivenProcessingWorkflow]
    L -->|Activity 1: PII Masking| M[Mask Abusive & Sensitive content]
    L -->|Activity 2: Thematic Analysis| N[Map Statements to Taxonomy]
    M & N -->|Update status='success'| O[submissions table]
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
*   **Trigger**: A successful API upload simply saves the file in GCS and registers it as `pending` in the `csv_uploads` table. No workflows are run.
*   **Ingestion Schedule (9:10 PM IST)**: `CsvBatchProcessingWorkflow` triggers, processes the CSVs, and pushes them to Kafka. Submissions are saved in the DB as `pending`.
*   **Analysis Schedule (9:15 PM IST)**: `BatchProcessingWorkflow` triggers, queries all `pending` submissions, and runs analysis workflows in parallel.

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
*   **Endpoint**: `POST /v1/push/{record_id}`
*   **Behavior**: Instantly triggers the `CsvProcessingWorkflow` for a specific record ID, bypassing the scheduled cron time. Useful for retrying `on_hold` files or running testing immediately.

---

## ⚙️ Configuration Variables

The following parameters in `.env` govern this pipeline:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PROCESSING_MODE` | Ingestion mode (`real-time` or `batch`) | `real-time` |
| `CSV_SCHEDULE_CRON_TIME` | UTC Cron schedule to process pending CSV files (runs 9:10 PM IST) | `40 15 * * *` |
| `BATCH_SCHEDULE_CRON` | UTC Cron schedule to analyze pending submissions (runs 9:15 PM IST) | `45 15 * * *` |
| `STORY_CSV_COLUMN` | JSON Array of columns expected for Story reports | `["id","Title", ...]` |
| `DISCUSSION_CSV_COLUMN` | JSON Array of columns expected for Discussion reports | `["id","Title", ...]` |
| `BUCKET_NAME` | Target GCS bucket for CSV uploads | `dev-sg-dashboard` |

---

## 📅 Staggered Schedule Coordination

To prevent race conditions where the analysis runs before ingestion has pushed records to the database, the schedules must be staggered:
1. **CSV Ingestion runs at 9:10 PM IST (`40 15 * * *` UTC)**. It reads the CSV files and pushes them to Kafka, which places the raw rows into PostgreSQL in the `pending` state.
2. **Submissions Analysis runs at 9:15 PM IST (`45 15 * * *` UTC)**. It queries all `pending` submissions in the database and runs PII masking and thematic classification.

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
