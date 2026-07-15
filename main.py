import argparse
import asyncio
import logging
import sys
import threading

import uvicorn
from fastapi import FastAPI

from app.api.bulk import router as bulk_router
from app.api.csv_upload import router as csv_upload_router
from app.api.routes import router as submissions_router
from app.kafka.consumer import IngestionConsumer
from app.temporal.worker import start_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("analytics_service.main")

consumer_running = False
worker_running = False


async def run_web_async():
    """Start the FastAPI web server asynchronously in the current loop."""
    app = FastAPI(
        title="Analytics Service API Ingestion & Orchestration Layer",
        description="FastAPI ingestion endpoints and manual orchestration controls.",
        version="1.0.0",
    )

    app.include_router(submissions_router)
    app.include_router(bulk_router)
    app.include_router(csv_upload_router)

    @app.get("/health")
    def health_check():
        return {
            "status": "healthy",
            "consumer_running": consumer_running,
            "worker_running": worker_running,
        }

    logger.info("Starting FastAPI web server asynchronously...")
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()


def run_web():
    """Start the FastAPI web server."""
    app = FastAPI(
        title="Analytics Service API Ingestion & Orchestration Layer",
        description="FastAPI ingestion endpoints and manual orchestration controls.",
        version="1.0.0",
    )

    app.include_router(submissions_router)
    app.include_router(bulk_router)
    app.include_router(csv_upload_router)

    @app.get("/health")
    def health_check():
        return {
            "status": "healthy",
            "consumer_running": consumer_running,
            "worker_running": worker_running,
        }

    logger.info("Starting FastAPI web server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)


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
        async def run_all_services():
            global consumer_running, worker_running
            consumer_running = True
            worker_running = True
            await asyncio.gather(run_web_async(), run_consumer(), run_worker())

        try:
            asyncio.run(run_all_services())
        except KeyboardInterrupt:
            logger.info("Shutdown signal received, stopping all services.")
        finally:
            consumer_running = False
            worker_running = False


if __name__ == "__main__":
    main()
