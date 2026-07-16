# Kafka Ingestion — Event Flows

Every message on `analytics.ingestion.raw` carries an `eventType` of `create`, `update`, or `delete`. Each takes a different path through `insert_or_update_submission` — this doc traces exactly what gets read, written, and triggered for each one.

## At a glance

| | create | update | delete |
|---|---|---|---|
| data source | `event.data` (full object) | `event.newValues` (delta only) | — |
| pre-check | rejected if `(submission_id, tenant_code)` already exists | rejected if `session_id` belongs to a *different* submission | none |
| column writes | every column set from `data` | `COALESCE(new, existing)` — only delta fields change | row + cascades removed |
| `participantsData` | written to `submission_metrics` | replaced in full if present, untouched if absent | cascade-deleted with the row |
| status after | `pending` | `pending` — forces full reprocessing | row gone |
| triggers workflow | yes, in real-time mode | yes, in real-time mode | no |

> **try/except**: every message is processed inside a guard in the consumer's poll loop — a bad payload, a `ValueError` from a rejected write, or any other exception is logged with the raw message and the loop moves on. One bad message never takes down the consumer or the Temporal worker.

---

## `create` — new submission

The full `data` object is written top to bottom. A `session_id` collision or a duplicate `(submission_id, tenant_code)` stops ingestion before anything is written.

```mermaid
flowchart TD
    A["Kafka message<br/>eventType = create"] --> D{"(submission_id, tenant_code)<br/>already in submissions?"}
    D -- yes --> D1["Log 'duplicate entry'<br/>skip ingestion"]
    D -- no --> E["data = event.data (full)<br/>tags = event.tags"]
    E --> E2["upsert_metadata:<br/>tenant, leader_category, programs"]
    E2 --> E3["INSERT submissions<br/>session_id, user_id, state, district,<br/>organization, status = 'pending'"]
    E3 --> F{"session_id already<br/>used by another submission?"}
    F -- yes --> F1["UniqueViolationError caught →<br/>ValueError → logged, message skipped"]
    F -- no --> G{submissionType}
    G -- story --> G1["INSERT story_submissions<br/>full row"]
    G -- discussion --> G2["INSERT discussion_submissions<br/>full row"]
    G2 --> G3{"participantsData<br/>present?"}
    G3 -- yes --> G4["upsert_participant_metrics:<br/>metric_definitions + submission_metrics"]
    G3 -- no --> H["transaction commits"]
    G4 --> H
    G1 --> H
    H --> I{PROCESSING_MODE}
    I -- real-time --> J["start_workflow<br/>status → 'processing'"]
    I -- batch --> K["leave status = 'pending'<br/>picked up by next batch cron"]
```

`consumer.py` 116–134 &middot; `operations.py` 124–229, 349–366

---

## `update` — partial update

`newValues` is delta-only — a field missing from it means "unchanged," not "clear it." Every write uses `COALESCE(new, existing)` so fields outside the delta — including any already PII-masked text — are left exactly as they are.

```mermaid
flowchart TD
    A["Kafka message<br/>eventType = update"] --> B["data = event.newValues (delta only)<br/>tags = event.tags (always sent in full)"]
    B --> B2["upsert_metadata:<br/>re-upserts leader_category / programs"]
    B2 --> B3{"submissionDate<br/>in newValues?"}
    B3 -- no --> B4["submission_date = None"]
    B3 -- yes --> B5["submission_date = parsed value"]
    B4 --> C
    B5 --> C["UPSERT submissions<br/>COALESCE(new, existing) per column<br/>status forced → 'pending' (always)"]
    C --> D{"session_id collides with<br/>a different submission?"}
    D -- yes --> D1["ValueError raised →<br/>logged, message skipped, service keeps running"]
    D -- no --> E{submissionType}
    E -- story --> F1["UPDATE story_submissions<br/>COALESCE($n, column) per field —<br/>only delta fields actually change"]
    E -- discussion --> F2["UPDATE discussion_submissions<br/>same COALESCE pattern"]
    F2 --> F3{"participantsData<br/>present in newValues?"}
    F3 -- yes --> F4["DELETE existing submission_metrics,<br/>re-INSERT full new snapshot"]
    F3 -- no --> G["submission_metrics<br/>left untouched"]
    F4 --> H["transaction commits"]
    G --> H
    F1 --> H
    H --> I{PROCESSING_MODE}
    I -- real-time --> J["start_workflow again —<br/>status='pending' reprocesses the delta"]
    I -- batch --> K["status stays 'pending'<br/>reprocessed on next batch run"]
```

`operations.py` 141–156, 175–229, 244–260, 363–393

---

## `delete` — remove submission

A single `DELETE` on `submissions` — everything else disappears via foreign-key cascade. No workflow runs.

```mermaid
flowchart TD
    A["Kafka message<br/>eventType = delete"] --> B["delete_submission(submission_id, tenant_code)"]
    B --> C["DELETE FROM submissions<br/>WHERE submission_id + tenant_code"]
    C --> D["ON DELETE CASCADE removes:<br/>story/discussion_submissions, llm_logs,<br/>analysis_results, ranking, submission_metrics"]
    D --> E["metric_definitions is NOT touched —<br/>shared reference data across submissions"]
    E --> F["no Temporal workflow triggered"]
```

`operations.py` 116–128 &middot; `schema.sql` ON DELETE CASCADE, 71–258

---

## Shared downstream Temporal workflow

Both `create` and `update` end the same way in real-time mode: a `ConfigDrivenProcessingWorkflow` run, driven by the tenant's `PROCESS_CONFIG_STORY` / `PROCESS_CONFIG_DISCUSSION` step list.

```mermaid
flowchart TD
    A["ConfigDrivenProcessingWorkflow.run"] --> B["update_status_activity → 'processing'"]
    B --> C["Step 1 · pii_and_abusive_language_detection_activity<br/>masks PII / abusive text per configured column"]
    C --> D["Step 2 · thematic_classification_activity<br/>local embedding match → LLM fallback"]
    D --> E["Step 3 · deface_blur_activity<br/>download → blur faces → upload to GCS"]
    E --> F["Step 4 · story_rating_activity<br/>(story submissions only)"]
    F --> G{all steps succeeded?}
    G -- yes --> H["update_status_activity → 'success'"]
    G -- no --> I["mark failed step + remaining as skipped<br/>update_status_activity → 'failed'<br/>re-raise"]
```

`workflows.py` 17–189 — each step's `llm_model` / `max_tokens` / `llm_timeout_seconds` can be overridden per step in the config JSON.

---

*analytics_service — traced from `app/kafka/consumer.py`, `app/database/operations.py`, `app/temporal/workflows.py`*
