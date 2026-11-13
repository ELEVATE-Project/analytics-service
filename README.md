# Analytics Service

An orchestration and ingestion service built with FastAPI, Kafka, and Temporal. It manages dynamic data ingestion pipelines, rule-based content moderation, local NLP vector similarity matching, and fallback LLM classification.

---

## Setup & Running Guide

### Prerequisites

Ensure the following dependencies are installed and running locally:
1. **Python**: Version 3.9+
2. **PostgreSQL**: Running and initialized with the database schema
3. **Apache Kafka & Zookeeper**: Running on `localhost:9092`
4. **Temporal Server**: Running on `localhost:7233` (e.g. via `temporal server start-dev` or Docker)

### Installation

1. Clone the repository and navigate to the project root.
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your environment variables:
   Copy `.env.example` to `.env` and fill in the required configurations (such as database credentials and your OpenRouter API key):
   ```bash
   cp .env.example .env
   ```

### Temporal Setup via Docker

A [docker-compose.yaml](file:///Users/user/Documents/AI/analytics-arch/analytics_service/docker-compose.yaml) file is provided to start the local Temporal server and its UI dashboard.

To start Temporal:
```bash
docker-compose up -d
```
This launches:
* **Temporal Server** on port `7233`
* **Temporal Web UI** on port `8233`

The configuration automatically points to your local host's PostgreSQL instance (`host.docker.internal`). Ensure PostgreSQL is running and has the `temporal` and `temporal_visibility` databases created prior to starting the containers.

#### Configuration ([docker-compose.yaml](file:///Users/user/Documents/AI/analytics-arch/analytics_service/docker-compose.yaml)):
```yaml
version: '3.8'

services:
  temporal:
    image: temporalio/auto-setup:1.24
    container_name: temporal
    ports:
      - "7233:7233"
    environment:
      - DB=postgres12
      - POSTGRES_SEEDS=host.docker.internal
      - POSTGRES_USER=postgres
      - POSTGRES_PWD=postgres
      - DB_PORT=5432
      - TEMPORAL_DB=temporal
      - TEMPORAL_VISIBILITY_DB=temporal_visibility
    extra_hosts:
      - "host.docker.internal:host-gateway"

  temporal-ui:
    image: temporalio/ui:2.34.0
    container_name: temporal-ui
    ports:
      - "8233:8080"
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
      - TEMPORAL_UI_PORT=8080
    depends_on:
      - temporal
```

### Running the Application

The **`main.py`** entry point supports starting different components using the `--mode` flag:

```bash
# Default – launches web server, Kafka consumer, and Temporal worker concurrently
python main.py

# Launch specific services separately
python main.py --mode web        # Starts only the FastAPI web server
python main.py --mode consumer   # Starts only the Kafka consumer
python main.py --mode worker     # Starts only the Temporal worker
```

---

## Health Endpoint

Verify that all systems are running by calling the health check API:
```bash
curl http://localhost:8000/health
```
Example response:
```json
{
  "status": "healthy",
  "consumer_running": true,
  "worker_running": true
}
```

---

## Orchestration Modes

The service supports two processing modes configured via the `PROCESSING_MODE` environment variable in your `.env` file:

### 1. Real-Time Processing (`PROCESSING_MODE=real-time`)
*   **Ingestion Flow**: When a new Kafka event is received, the consumer automatically attempts to trigger a Temporal workflow (`ConfigDrivenProcessingWorkflow`) immediately.
*   **Temporal Connection Self-Healing**: 
    *   On startup, the Kafka consumer initializes the Temporal client connection.
    *   If the Temporal server is offline or unreachable during consumer startup or during execution, the consumer **does not crash**.
    *   Instead, the consumer gracefully logs the connection error and leaves the submission in the PostgreSQL database with a `'pending'` status.
    *   When new Kafka events arrive, the consumer automatically checks if the Temporal client is disconnected and attempts to reconnect/heal the connection. Once the Temporal server is back online, real-time workflow triggering resumes automatically.

### 2. Batch Processing (`PROCESSING_MODE=batch`)
*   **Ingestion Flow**: When a new Kafka event is received, the consumer simply stores the record in PostgreSQL with a `'pending'` status. The consumer **completely bypasses all Temporal calls** during ingestion. Even if the Temporal backend is entirely down, Kafka event ingestion works flawlessly.
*   **Automatic Daily Schedule Registration**: 
    *   When the Temporal worker process starts, it inspects `PROCESSING_MODE`. If set to `batch`, the worker automatically registers a daily batch Schedule in the Temporal Server named `daily-batch-processing`.
    *   The run time is configured via the `BATCH_SCHEDULE_CRON` environment variable (default: `0 20 * * *` - 8:00 PM UTC).
    *   If the schedule is already registered, the worker logs this and skips registration gracefully.
*   **Batch Execution**: 
    *   At the scheduled time, the Temporal Server triggers the `BatchProcessingWorkflow`.
    *   This workflow calls the `fetch_pending_submissions_activity` to retrieve all submissions in PostgreSQL with a `'pending'` status.
    *   It then spawns child workflows (`ConfigDrivenProcessingWorkflow`) to process all retrieved submissions in parallel.
    *   If the Temporal Server backend is offline at the scheduled run time, its *catch-up* policy will trigger the missed batch immediately upon starting up again.


---

## Feedback (1 Million Voices) uploads: preparing the export and managing questionnaire versions

`POST /v1/upload/` with `report_type=feedback` takes **one persona per upload** (the persona is the file's
`session_type` column: `Youth_Voice`, `General_Public_Voice`, `Parent_Voice`, `Educator_Voice`; the `leader_category`
form field must be the same persona) and rejects a body above the persona config's `max_csv_upload_bytes` (100 MB
when seeded; see step 4). The Mitra
export holds all personas in one file, so it is prepared before upload. These are operator tools in `tools/`; the
service never calls them.

**1. Split the export, one file per persona (and under the size cap).**
```bash
python tools/split_mitra_export.py all_4_persona_data.csv --out-dir out/ --max-mb 80
# out/Youth_Voice.csv, out/General_Public_Voice.csv, out/Educator_Voice.csv, out/Parent_Voice_part1.csv, ... and split_summary.json
```
A session is never split across files, so a persona that is too big for one upload becomes several parts. Uploading
the parts (or the same file again) is safe: the delta check publishes only sessions that are new or grew.

**2. Upload each file with its persona.**
```bash
curl -X POST localhost:8000/v1/upload/ -H "Authorization: Bearer $TOKEN" \
  -F report_type=feedback -F program_name="1M Voice" -F leader_category=Youth \
  -F tenant_code=mitra -F source=mitra -F file=@out/Youth_Voice.csv
```

**3. Record questionnaire versions as data, not code.** Every session carries a `questionnaire_version` (V1, V2 ...)
derived from when it started, using windows stored in the database (the persona's Mitra upload config). A new
database only has the baseline `V1`. When a bot's questions change, record the moment the change went live; no code
change and no deploy are needed, for V2, V3, V4 alike. You do not have to know the date: `detect` reads the
questions the stored sessions were asked (the scripted question of each step, not the LLM probes), and when a new
question set has appeared it proposes the next key, the instant it started and the `set` command to run. It is
read-only; a person confirms by running `set`:
```bash
python tools/questionnaire_versions.py detect --session-type Youth_Voice
python tools/questionnaire_versions.py list
python tools/questionnaire_versions.py set --session-type Youth_Voice --key V3 \
    --effective-from 2026-12-01T10:30:00+05:30 --dry-run     # shows what would change, writes nothing
python tools/questionnaire_versions.py set --session-type Youth_Voice --key V3 \
    --effective-from 2026-12-01T10:30:00+05:30 --note "new confidence question"
```
`set` writes a new config version (the old one stays as history and can be re-activated to roll back) and relabels
the sessions already stored. Sessions uploaded before a version is recorded are relabelled when it is.
`detect` learns what each version asks from its first 100 stored sessions (`--baseline-sessions`), so on a very
small deployment lower that number; it finds new question sets but does not audit dates you recorded by hand.

**4. Per-bot settings live in the config tables, not in `.env`.** Every feedback bot can have a different input
structure, so the settings that differ per bot are keys of the persona's active `feedback_upload` config (Mitra and
Google Forms each have one): `max_csv_upload_bytes` (size cap of one upload, 413 above it, checked before the file is
stored; seeded 104857600), `ingestion_schema` (required / optional fields of the Kafka event, same structure as
`STORY_KAFKA_SCHEMA`; an event that does not satisfy it goes to the DLQ), `process_steps` (steps run after ingestion,
same format as `PROCESS_CONFIG_STORY`; seeded `[]`: extraction is opt-in because it is a paid LLM call per submission; an empty list means the
submission is marked success at ingestion; turn extraction on per persona with the `set --key process_steps` line below) and the optional `expect_stages` (list of stage names; an upload then
reports, per stage that is neither expected nor a closing key, how many sessions carry it, in the upload response
`stage_report` and in `csv_uploads.meta_data`; report-only, it never skips a session). A config that lacks one of the
three required keys, or has one of the wrong type, is rejected with a clear message (400 on upload, DLQ in the
consumer). The Kafka consumer caches the schema per persona for 60 seconds. The seed (`seed_config.sql`, run at every start)
never overwrites, so an existing
database keeps the config version it was seeded with: add the missing keys once, then change values without SQL
(each change is a new config version, the old one stays as history):
```bash
python tools/feedback_config.py show [--session-type Youth_Voice]
python tools/feedback_config.py migrate --dry-run            # lists what would be added; then run without --dry-run
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key max_csv_upload_bytes --value 50000000
# turn metadata extraction ON for one persona (opt-in, one paid LLM call per submission); drop --dry-run to apply
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key process_steps \
    --json '[{"name": "feedback_metadata_extraction"}]' --dry-run
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key expect_stages --unset
```
