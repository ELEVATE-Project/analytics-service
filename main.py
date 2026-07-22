import argparse
import asyncio
import logging
import threading

import uvicorn
from fastapi import FastAPI

from app.api.router import api_router
from app.api.exceptions import register_exception_handlers
from app.kafka.consumer import IngestionConsumer
from app.logging_config import configure_logging
from app.temporal.worker import start_worker

logger = logging.getLogger("analytics_service.main")

consumer_running = False
worker_running = False


def run_web():
    """Start the FastAPI web server."""
    app = FastAPI(
        title="Analytics Service API Ingestion & Orchestration Layer",
        description="FastAPI ingestion endpoints and manual orchestration controls.",
        version="1.0.0",
    )

    register_exception_handlers(app)
    app.include_router(api_router)

    @app.get("/health")
    def health_check():
        return {
            "status": "healthy",
            "consumer_running": consumer_running,
            "worker_running": worker_running,
        }

    logger.info("Starting FastAPI web server...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)


async def run_consumer():
    """Start the Kafka consumer loop."""
    consumer = IngestionConsumer()
    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Interrupt received, stopping consumer...")
        consumer.stop()


async def run_worker():
    """Start the Temporal worker."""
    await start_worker()


def main():
    parser = argparse.ArgumentParser(description="Analytics Ingestion and Orchestration runner.")
    parser.add_argument(
        "--mode",
        choices=["consumer", "worker", "web", "all"],
        default="all",
        help="Specify the service mode to start: 'consumer' (Kafka consumer), 'worker' (Temporal worker), 'web' (API server), or 'all' (run all three services).",
    )
    args = parser.parse_args()

    configure_logging(args.mode)

    global consumer_running, worker_running

    if args.mode == "web":
        run_web()
    elif args.mode == "consumer":
        consumer_running = True
        try:
            asyncio.run(run_consumer())
        except KeyboardInterrupt:
            logger.info("Kafka consumer stopped.")
        finally:
            consumer_running = False
    elif args.mode == "worker":
        worker_running = True
        try:
            asyncio.run(run_worker())
        except KeyboardInterrupt:
            logger.info("Temporal worker stopped.")
        finally:
            worker_running = False
    elif args.mode == "all":
        web_thread = threading.Thread(target=run_web, daemon=True)
        web_thread.start()

        async def run_all_services():
            global consumer_running, worker_running
            consumer_running = True
            worker_running = True
            await asyncio.gather(run_consumer(), run_worker())

        try:
            asyncio.run(run_all_services())
        except KeyboardInterrupt:
            logger.info("Shutdown signal received, stopping all services.")
        finally:
            consumer_running = False
            worker_running = False


if __name__ == "__main__":
    main()
