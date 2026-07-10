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

