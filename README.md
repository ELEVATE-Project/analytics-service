# Analytics Service

## Running the service

The entry point **`main.py`** supports four modes via the `--mode` flag:

- `consumer` – starts only the Kafka consumer that reads from the `analytics.ingestion.raw` topic.
- `worker`   – starts only the Temporal worker that executes the defined activities and workflows.
- `web`      – starts the FastAPI API server (exposes the `/health` endpoint and any ingestion routes).
- `all`      – **default**. Starts the web server in a background thread and runs the consumer **and** worker concurrently. This is convenient for local development and end‑to‑end testing.

```bash
# Default – launches everything
python main.py

# Explicit mode examples
python main.py --mode consumer   # only Kafka consumer
python main.py --mode worker     # only Temporal worker
python main.py --mode web        # only FastAPI API
```

## Health endpoint

The API provides a `/health` endpoint that reports the overall status and whether the consumer and worker are currently running:

```json
{
  "status": "healthy",
  "consumer_running": true,
  "worker_running": true
}
```

Use this endpoint (e.g., `curl http://localhost:8000/health`) to verify that all components are alive.

## Configuration

All configuration values are read from environment variables (see `.env.example` for a template):
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_GROUP_ID`
- `KAFKA_TOPIC`
- `DATABASE_URL`
- `TEMPORAL_ADDRESS`
- `OPENROUTER_API_KEY`

Make sure to export or source a `.env` file before running the service.
