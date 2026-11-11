-- Migration: Support partial index for retryable submissions (failed or stuck processing)
-- Keyset pagination index for bulk retry sweep: efficiently fetches failed and stale processing submissions
-- ordered by (updated_at, submission_id) without a sequential scan.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_submissions_retryable 
ON submissions (updated_at, submission_id) 
WHERE status IN ('failed', 'processing');

DROP INDEX CONCURRENTLY IF EXISTS idx_submissions_failed;

