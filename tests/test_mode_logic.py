import asyncio
from unittest.mock import AsyncMock, patch, MagicMock, ANY
import pytest
from app.kafka import consumer as consumer_module
from app.temporal import worker as worker_module

class _FakeConn:
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False

class _FakePool:
    def acquire(self):
        return _FakeConn()


def test_trigger_realtime_workflow_connection_healing_success(monkeypatch):
    """Test that if temporal_client is None, it heals connection and successfully triggers."""
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        consumer.temporal_client = None  # Start disconnected

        # Mock settings and DB pool
        monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time")
        monkeypatch.setattr(consumer_module.db, "pool", MagicMock(acquire=_FakePool().acquire))
        
        mock_update_status = AsyncMock()
        monkeypatch.setattr(consumer_module, "update_submission_status", mock_update_status)

        mock_client = MagicMock()
        mock_client.start_workflow = AsyncMock()

        # Mock Client.connect to return our mock_client
        with patch("app.kafka.consumer.Client.connect", AsyncMock(return_value=mock_client)) as mock_connect:
            await consumer._trigger_realtime_workflow("sub1", "tenant1", "story")
            
            mock_connect.assert_awaited_once_with("localhost:7233")
            mock_client.start_workflow.assert_awaited_once()
            mock_update_status.assert_awaited_once_with(
                ANY, 
                "sub1", "tenant1", "processing"
            )
            assert consumer.temporal_client is mock_client

    asyncio.run(run_test())


def test_trigger_realtime_workflow_connection_healing_failure(monkeypatch):
    """Test that if connection healing fails, it logs error and does not raise exception."""
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        consumer.temporal_client = None

        monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time")
        monkeypatch.setattr(consumer_module.db, "pool", MagicMock(acquire=_FakePool().acquire))
        
        mock_update_status = AsyncMock()
        monkeypatch.setattr(consumer_module, "update_submission_status", mock_update_status)

        # Client.connect raises exception
        with patch("app.kafka.consumer.Client.connect", AsyncMock(side_effect=Exception("Connection refused"))):
            await consumer._trigger_realtime_workflow("sub1", "tenant1", "story")
            
            # Should not update status to processing
            mock_update_status.assert_not_awaited()
            assert consumer.temporal_client is None

    asyncio.run(run_test())


def test_trigger_realtime_workflow_grpc_error_resets_client(monkeypatch):
    """Test that a gRPC connection error during start_workflow resets client to None."""
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        mock_client = MagicMock()
        # Simulate a gRPC unavailable/connection error
        mock_client.start_workflow = AsyncMock(side_effect=Exception("gRPC status: UNAVAILABLE, description: connection lost"))
        consumer.temporal_client = mock_client

        monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time")
        monkeypatch.setattr(consumer_module.db, "pool", MagicMock(acquire=_FakePool().acquire))
        
        mock_update_status = AsyncMock()
        monkeypatch.setattr(consumer_module, "update_submission_status", mock_update_status)

        await consumer._trigger_realtime_workflow("sub1", "tenant1", "story")
        
        # Client should be reset to None for future healing
        assert consumer.temporal_client is None
        mock_update_status.assert_not_awaited()

    asyncio.run(run_test())


def test_worker_startup_registers_batch_schedule(monkeypatch):
    """Test that start_worker registers daily batch schedule when mode is batch."""
    async def run_test():
        monkeypatch.setattr(worker_module.settings, "PROCESSING_MODE", "batch")
        monkeypatch.setattr(worker_module.settings, "BATCH_SCHEDULE_CRON", "0 20 * * *")
        monkeypatch.setattr(worker_module.db, "connect", AsyncMock())
        monkeypatch.setattr(worker_module.db, "disconnect", AsyncMock())

        mock_client = MagicMock()
        mock_client.create_schedule = AsyncMock()
        
        monkeypatch.setattr(worker_module.Client, "connect", AsyncMock(return_value=mock_client))
        
        # Mock Worker class run to raise CancelledError immediately so it exits
        mock_worker_instance = MagicMock()
        mock_worker_instance.run = AsyncMock(side_effect=asyncio.CancelledError())
        with patch("app.temporal.worker.Worker", return_value=mock_worker_instance):
            await worker_module.start_worker()
            
            mock_client.create_schedule.assert_awaited_once()
            # Verify schedule spec cron expression is correct
            args, kwargs = mock_client.create_schedule.call_args
            assert kwargs["id"] == "daily-batch-processing"
            assert kwargs["schedule"].spec.cron_expressions == ["0 20 * * *"]

    asyncio.run(run_test())


def test_deface_blur_activity_success(monkeypatch):
    """Test that deface_blur_activity downloads, processes, uploads, and deletes temp files successfully."""
    async def run_test():
        from app.temporal.deface_blur_activity import deface_blur_activity
        from app.temporal.deface_blur_activity import db as db_module
        from pathlib import Path

        # Mock db connection acquire
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        
        class _CustomFakeConn:
            async def __aenter__(self):
                return mock_conn
            async def __aexit__(self, exc_type, exc, tb):
                return False
        
        monkeypatch.setattr(db_module, "pool", MagicMock(acquire=lambda: _CustomFakeConn()))

        # Mock payload helper
        monkeypatch.setattr(
            "app.temporal.deface_blur_activity.get_submission_type_and_payload",
            AsyncMock(return_value=("story", {"image_urls": ["https://foo.com/3281/bar.png"]}))
        )

        # Mock download/blur/upload
        mock_download = MagicMock(return_value=Path("/tmp/bar.png"))
        monkeypatch.setattr("app.temporal.deface_blur_activity._download_file", mock_download)
        
        mock_anonymize = MagicMock()
        monkeypatch.setattr("app.temporal.deface_blur_activity.anonymize_face", mock_anonymize)
        
        mock_upload = MagicMock(return_value="/bucket/story_blurred_image/3281/bar.png")
        monkeypatch.setattr("app.temporal.deface_blur_activity.upload_to_gcp", mock_upload)

        # Mock path exists and unlink
        mock_path = MagicMock()
        mock_path.__truediv__.return_value = mock_path
        mock_path.exists.return_value = True
        mock_path.unlink = MagicMock()
        
        monkeypatch.setattr("app.temporal.deface_blur_activity.DOWNLOADS_DIR", mock_path)
        monkeypatch.setattr("app.temporal.deface_blur_activity.OUTPUTS_DIR", mock_path)

        res = await deface_blur_activity({"submission_id": "sub1", "tenant_code": "tenant1"})
        
        assert res["status"] == "success"
        assert res["blur_paths"] == ["/bucket/story_blurred_image/3281/bar.png"]
        mock_conn.execute.assert_called_once()
        # Verify cleanups were called on both local_path and output_path
        assert mock_path.unlink.call_count == 2

    asyncio.run(run_test())

