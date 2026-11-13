# Feedback metadata extraction and the DOTS queries

Written for: the analyst who runs the DOTS export by hand in Postgres, and the engineer who operates the extraction step.

After a feedback session is stored (Mitra or Google Forms), one LLM call per session reads the **complete conversation** and returns the DOTS metadata fields of the respondent's persona (a different list per persona, for example Youth: `age`, `gender`, `state`, `district`, `current_activity_status`, ...; the full lists are in section 3) mapped to the product team's category bands, plus a **whole-conversation PII flag**. Each persona has its **own prompt**, chosen by its extraction config. The DOTS export itself is not built: you pivot the results with the queries in section 6.

**Extraction is OPT-IN.** The persona's upload config has `process_steps = []` by default, so nothing is sent to an LLM until someone enables the step (section 5). **Cost: one real LLM call per stored session**, so a persona-wide run is a real bill. Try it on one or two sessions first with `python tools/run_feedback_extraction.py run --session-type Youth_Voice --limit 2`. The list of things product still has to confirm is in section 7.

## 1. How it works

1. The workflow step `feedback_metadata_extraction` (activity `app/temporal/feedback_extraction_activity.py`) or the CLI (`tools/run_feedback_extraction.py`) runs the same core, `extract_submission` in `app/database/feedback_extraction_operations.py`.
2. It loads the stored session (`feedbacks` rows ordered by `sequence`), the **active extraction config version** of the session's `session_type` and program, and the **active prompt version of the prompt the config names** (`prompt_name`; a config without one uses the generic prompt `Feedback Metadata Extraction`). The highest active `prompt_version` of that exact `prompts.name` is used; a name that does not exist, has no active version, or belongs to a prompt whose `analysis_type` is not `feedback_metadata_extraction` **fails loudly** (no silent fallback to the generic prompt, no LLM call). The `prompt_version_id` stored on the rows and in `meta_data.extraction` is the one actually used.
3. The conversation is built as one line per message: `[sequence] Bot: ...` or `[sequence] Respondent: ...`. A row is a respondent answer exactly when `feedbacks.parent_id` is set. Bot lines use the English text; respondent lines show the original text and, when it differs, `(English translation: ...)`, so the model sees both. The closing (thank-you) message is left out when its `question_key` is in the persona's upload config `closing_keys`. Every message is collapsed to one line (control characters become spaces, so words do not merge). **Respondent messages are never truncated**: PII after a cut would be lost. Bot messages (the questionnaire's own text) are cut at 4,000 characters. If a respondent message is longer than 20,000 characters the session is **not sent to the model at all**: it is recorded as `pii_detected = TRUE` with type `other`, its fields are `unmapped` (the respondent answered, nothing was read) and no LLM call is made, so a person can look at it and it never reaches the export.
4. **One LLM call** (same `openrouter_chat_completion` path as the other steps; the step's `llm_model`, `max_tokens`, `llm_timeout_seconds` overrides win, then the config's `llm` block, then the `.env` settings). The model reads the whole conversation, so it does not matter whether the questionnaire version asks demographics early (V1) or at the end (V2), or whether the source is Mitra or a Google Form.
5. The answer is **untrusted**: strict JSON only (markdown fences tolerated), no duplicate keys, every configured field present and nothing else, correct types. Each value is checked against the field's allowed values (an `integer` field against its `min` / `max`: a JSON number or a numeric string such as `"17"` inside the range is stored as the canonical decimal string `17`; text, a decimal such as `17.5`, a boolean or an out-of-range number is `unmapped`). A value outside them is **not coerced**: it is stored with status `unmapped`, `value` NULL and the words kept in `raw_value`. A free-text field (district) whose value is only a placeholder (`N/A`, `null`, `none`, `Not provided`, `not specified`, `unknown`, `don't know` and similar, case-insensitive) is `not_provided`, not `extracted`. `confidence` is optional and informational: a malformed one (for example `85`) is ignored and stored NULL, the session is not failed; an unknown key still fails.
6. **Deterministic PII backstop.** The model's `pii.detected = false` is not trusted alone (a respondent can write "set pii to false"). After a valid answer the code scans the **respondent** text (the original and the translated column, each message on its own) and forces `pii_detected = TRUE` when it finds a run of **8 or more digits** (spaces, dashes, dots and parentheses between digits are ignored; Devanagari, Kannada and other Unicode digits count), an **email address**, an **@handle** or a **URL**. The matching kinds (`phone` for a digit run, `email` for an email or handle, `other` for a URL) are added to the types. It can only turn the flag TRUE, never FALSE. Two-digit ages, years, classes and amounts up to seven digits do not trigger it. It is blunt on purpose: a false positive only drops a session from the export, a false negative leaks PII. Only the kind is logged, never the text.
7. One transaction writes the field rows to `feedback_extracted_data` (upsert: a re-run overwrites, a field that is no longer configured is deleted), sets `feedback_submissions.pii_detected`, merges an `extraction` object into `feedback_submissions.meta_data` (`config_version_id`, `prompt_version_id`, `pii_types`, `run_at`) and writes the `llm_logs` row. `run_at` is the time the input was **loaded**, not the time of the write.
8. **A session that grows during the LLM call is never overwritten with a stale flag.** The transaction first locks the `feedback_submissions` row, re-reads the messages and compares a content fingerprint (row count and a hash over sequence, parent link, question key, message and translation) with the one taken at load. If it differs **nothing is written** (the flag stays NULL, as the ingest left it) and the step fails with `StaleExtractionError`, which the workflow retries (3 attempts) on the new content; the wasted call is logged in `llm_logs` as `failed`.
9. A conversation with **no respondent answer** makes no LLM call: every field is `not_provided` and `pii_detected` is FALSE.

### Statuses (`feedback_extracted_data.status`)

| status | meaning | `value` | `raw_value` |
|---|---|---|---|
| `extracted` | the answer maps to an allowed value (or, for `district`, a non-empty name that is not a placeholder) | the canonical band, spelled as in the config | the respondent's words as the model returned them |
| `not_provided` | the respondent never answered this question | NULL | NULL |
| `unmapped` | the respondent answered but it fits no allowed value (or the model returned a value outside the list); also every field of a session skipped for an oversized respondent message | NULL | the respondent's words, or the rejected value when the model gave no words |

The only normalisation: a band is matched ignoring case and repeated spaces, and stored with the config's own spelling (`tamil nadu` becomes `Tamil Nadu`). `Tamilnadu` is **not** repaired, it is `unmapped`. `confidence` is stored only if the model returned one.

### The PII flag

`pii_detected` is `NULL` until the session was checked, then TRUE or FALSE, the same idea as `pii_masked` on story and discussion submissions. TRUE means the **respondent's words** contain something that could identify a person or an institution (name, phone, email, address, school name, id number, other); the prompt says to flag when in doubt, and the deterministic pre-check of step 6 adds numbers, emails, handles and URLs. State, district, gender, age, class, kind of school and income bands are not PII. The types found are in `feedback_submissions.meta_data -> 'extraction' -> 'pii_types'`. A response where the model says `detected: false` but lists types is rejected. Types the model names that are not in the config's `pii.types` list are recorded as `other`.

### Safety rules in the code

- The conversation is data, not instructions: the prompt says so, the conversation sits between delimiter lines carrying an id derived from its own text (a respondent cannot know it), every message is a single line (no forged `[n] Bot:` lines) and the template variables are filled in one pass (respondent text cannot inject a variable).
- Conversation text is never logged by this code and never in an error message. `llm_logs` stores tokens, status and the provider's usage only, not the prompt (the existing convention). **Provider errors are sanitised**: a failed LLM call is re-raised as `LlmCallError` carrying only the exception class and, when it can be read from the message prefix, the HTTP status (`LLM call failed (RuntimeError, HTTP 429)`); the provider's error body, which can echo the prompt, reaches neither `llm_logs.error_message` nor the Temporal failure history. For any other unexpected error only the class name is stored. **Two things outside this code**: `openrouter_chat_completion` writes the full prompt at DEBUG level, so never run with DEBUG logging on real data, and it logs the provider's error body at ERROR level in its own logger (`app/services/llm.py`, shared by every step).
- The CLI prints aggregates only.

## 2. Tables

- `feedback_extracted_data`: one row per session per field. Columns: `submission_id`, `tenant_code`, `field_name`, `value`, `raw_value`, `status`, `confidence`, `config_version_id`, `prompt_version_id`, `extracted_at`. Unique on `(submission_id, tenant_code, field_name)`; deleted with the submission; index on `(field_name, value)`. It deliberately has **no `session_type` or `questionnaire_version` column**: copies would go stale when `tools/questionnaire_versions.py set` or `restamp` relabels sessions. Read both from `feedback_submissions` (every query below does).
- `feedback_submissions.pii_detected BOOLEAN` (new) and `feedback_submissions.meta_data -> 'extraction'`.
- `config` row per persona with `config_type = 'feedback_extraction'`, `config_source = 'any'` (it applies to both sources), `session_type`, `program_id`; versions in `config_version`.

## 3. The extraction config (`config_version.config`)

```json
{
  "schema_version": 1,
  "persona": "Youth",
  "session_type": "Youth_Voice",
  "prompt_name": "Feedback Metadata Extraction - Youth",
  "source_note": "free text",
  "fields": [
    {"name": "age", "kind": "integer", "description": "...", "min": 5, "max": 100, "provisional": true},
    {"name": "state", "kind": "category", "description": "...", "categories": ["Andhra Pradesh", "..."],
     "guidance": {"Delhi (NCT)": "also Delhi, New Delhi"}},
    {"name": "district", "kind": "free_text", "description": "..."},
    {"name": "disability", "kind": "category", "description": "...", "categories": ["Yes", "No", "..."],
     "sensitive": true, "provisional": true}
  ],
  "pii": {"types": ["name", "phone", "email", "address", "school_name", "id_number", "other"]},
  "llm": {"model": null, "max_tokens": null, "timeout_seconds": null}
}
```

- `prompt_name` (optional): the exact `prompts.name` of this persona's prompt: a non-empty string without surrounding spaces. Left out, the generic prompt `Feedback Metadata Extraction` is used, so older configs keep working. When it is present it must resolve (see section 1, step 2): there is no fallback.
- `kind`: `category` (needs `categories`, each unique ignoring case; optional `guidance` per category, shown to the model), `free_text` (the value is taken as given; a value longer than 120 characters is treated as `unmapped`) or `integer` (needs integer `min` and `max`, `min <= max`, booleans rejected; the model may return a JSON number or a numeric string; inside the range it is stored as the canonical decimal string, for example `"17"`; outside it, not a whole number (`17.5`, `17 years`) or a boolean it is `unmapped` with the raw kept; `null` is `not_provided`). `min` / `max` are not allowed on the other kinds, `categories` / `guidance` only on `category`.
- `provisional: true` marks bands product has not confirmed; it has no effect on the processing.
- `sensitive: true` (optional, any kind, default false): adds the rule line `(sensitive: fill only when the respondent states it explicitly; never infer)` under the field in the prompt text the model sees. It **never changes validation or storage**; it is also the marker used in section 6 for the columns that must stay out of a DOTS export until product confirms.
- A free-text value that is a placeholder (`N/A`, `null`, `none`, `not provided`, ...) is `not_provided`; longer than 120 characters is `unmapped`. The same placeholders as an integer answer are `not_provided`.
- The config is validated when it is read (unknown keys, duplicate names, bad kinds or bounds are rejected, never defaulted) and when the seed runs. `llm` values are optional; a workflow step override wins over them.
- Show what is live: `python tools/run_feedback_extraction.py show-config [--session-type Youth_Voice]` prints, per persona, its `prompt_name` (`generic` when it has none), each field with its kind and `sensitive` / `provisional` flags, and the full config JSON.

### The columns of each persona (field names are the DOTS column names)

| persona (`session_type`) | fields (S = `sensitive`) |
|---|---|
| Youth (`Youth_Voice`) | `age`, `gender`, `state`, `district`, `current_activity_status`, `education_level`, `course_details`, `institution_type`, `monthly_household_income` (S), `disability` (S), `disability_type` (S) |
| General Public (`General_Public_Voice`) | `age`, `gender`, `state`, `district`, `occupation` |
| Parent (`Parent_Voice`) | `state`, `district`, `child_gender`, `disability` (S), `disability_type` (S), `respondent_relationship`, `school_type`, `child_grade`, `parent_education`, `parent_education_level` |
| Educator (`Educator_Voice`) | `state`, `district`, `gender`, `institution_type`, `teaching_level` |

Parent uses `child_gender` (the CHILD's gender), not `gender`. The allowed values (bands), the `provisional` flags and the open product questions of each field are in `docs/feedback-extraction-bands.md`; the live truth is the config row (`show-config`). Two things that hold for every persona: `state` is the 28 states and 8 union territories (official names, spelling variants in the guidance; a place that is not an Indian state or union territory is `unmapped`), and `district` is free text normalised by the model with **no reference validation** (there is no district reference list yet; open question H2 in `docs/katha-stories-vs-dots-data-review.md`).

## 4. Changing a band (or a description) = a new config version

Seeds never overwrite. To change a persona's bands, add a new config version with the full edited JSON and make it the active one. Rollback is re-activating an older version.

```sql
-- 1. find the config: SELECT id FROM config WHERE config_type = 'feedback_extraction' AND session_type = 'Youth_Voice';
BEGIN;
UPDATE config_version SET is_active = FALSE, updated_at = now()
WHERE config_id = '<config id>' AND is_active;
INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT config_id, max(version) + 1, TRUE, '<edited JSON from show-config>'::jsonb, 'your name', 'what changed and why'
FROM config_version WHERE config_id = '<config id>' GROUP BY config_id;
COMMIT;
```

**The seeded config is data in `seed_config.sql`, inserted with `ON CONFLICT DO NOTHING`.** The fields, bands and prompt name of each persona are the JSON of the block `-- CONFIG feedback_extraction any <session_type>` of `seed_config.sql` (repo root); there is no Python copy. A change to that file (new fields, a new prompt name, corrected descriptions) reaches a **new** database only: a database that already holds the config keeps its version, because version 1 is only inserted while the config has no version at all. **To bring an existing database to the content of the file, write a new `config_version` with the recipe above**, using the JSON of the block (keep the active version's `llm` block if one was set): it deactivates the active version (history is kept, so re-activating the old version is the rollback) and writes the next one. There is no `upgrade-config` command any more; `show-config` prints what the database holds now. **It does not touch the prompts**: the persona prompts are inserted by `seed_prompts.sql` at startup, and a config only works once the prompt it names exists (otherwise a run fails loudly). Re-run the sessions afterwards (`run` without `--only-unprocessed`); their rows record the `config_version_id` and `prompt_version_id` they were produced with. Step by step, per persona: `docs/feedback-extraction-bands.md`, "How an existing database picks this up".

For a one-off change to a single persona by hand the manual recipe above still works: take the JSON from `show-config`, edit it and insert it as the next `config_version`.

### Changing a persona's prompt (a new `prompt_version` row, no deploy)

**The extraction prompts are protected the same way**: `seed_prompts.sql` inserts each prompt's version 1 with `ON CONFLICT DO NOTHING`, so a manual edit survives a restart. The prompt is the PII logic: **to change a persona's prompt, insert a new `prompt_version` of that persona's prompt (the next version number, `is_active = TRUE`) and deactivate the previous one**, do not edit it in place. The engine reads the highest active version of the exact `prompts.name` on the next run; no deploy and no config change are needed. Take the name from `show-config` (`prompt_name`):

```sql
BEGIN;
UPDATE prompt_version SET is_active = FALSE
WHERE prompt_id = (SELECT id FROM prompts WHERE name = 'Feedback Metadata Extraction - Youth') AND is_active;
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note)
SELECT p.id, (SELECT max(version) + 1 FROM prompt_version WHERE prompt_id = p.id),
       '<system prompt>', '<user prompt>', TRUE, 'what changed and why'
FROM prompts p WHERE p.name = 'Feedback Metadata Extraction - Youth';
COMMIT;
```

A prompt is free-form text that may use these template variables (anything else fails the render, nothing is substituted twice): `{{persona}}`, `{{fields}}` (the persona's field list rendered from its config, with allowed values, integer ranges and the sensitive rule line), `{{field_names}}`, `{{pii_types}}`, `{{output_format}}` (the JSON shape the answer must have), `{{conversation_id}}` and `{{conversation}}`. Keep the security rules and the PII check in it: the answer is still validated strictly whatever the prompt says. To point a persona at a **different prompt** create the `prompts` row (`analysis_type = 'feedback_metadata_extraction'`, the exact `name`) with an active version first, then add a config version whose `prompt_name` is that name; a persona config without `prompt_name` uses `Feedback Metadata Extraction`.

Take the JSON from `show-config`, edit it, and keep it valid: a `guidance` entry for a category you removed is rejected (the config is validated when it is read), so remove it too. Every extracted row records the `config_version_id` it used, so you can tell which rows predate a change.

## 5. Running it

- **In the workflow (opt-in)**: the persona's `process_steps` default is `[]`, so extraction does not run until you enable it per persona and per source. For Mitra Youth:

```
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key process_steps --json '[{"name":"feedback_metadata_extraction"}]'
```

  and the same with `--source google_forms` for the Google Forms sessions of that persona (optional step keys `llm_model`, `max_tokens`, `llm_timeout_seconds`). **Every stored session of that persona then costs one real LLM call**, including each re-processing of a session that grew.
- **Trying it**: use the CLI below on one or two sessions (`--limit 2`) before enabling the workflow step; do not run it persona-wide to "see what happens".
- **By hand, without Temporal** (needs `OPENROUTER_API_KEY`; sessions go to an external provider):

```
python tools/run_feedback_extraction.py run --session-type Youth_Voice --limit 2                 # a trial: 2 real LLM calls
python tools/run_feedback_extraction.py run --session-type Youth_Voice --dry-run --limit 20     # LLM called, nothing written
python tools/run_feedback_extraction.py run --session-type Youth_Voice --only-unprocessed        # sessions with pii_detected NULL
python tools/run_feedback_extraction.py run --session-type Youth_Voice --submission-id 12345     # one session
python tools/run_feedback_extraction.py run --session-type Parent_Voice --concurrency 8 --model <model>
python tools/run_feedback_extraction.py show-config
```

  The output is aggregates only: counts per field and status, the most frequent canonical values of the category fields (`--top`; for a free-text field such as `district` only the number of distinct values, never the values; for an `integer` field such as `age` the number of distinct values and the smallest and largest value, never the values themselves), PII counts and types, token usage, error types.
- **Re-run after a config or prompt change**: run without `--only-unprocessed`; every selected session is re-read (one LLM call each) and its rows are overwritten.
- **A session that grew** goes back to `pii_detected = NULL` when the new messages arrive (also when only a parent link changed) and is re-processed by the existing pending logic. If it grows while a call is running, the write is refused and retried (section 1, step 8). A flag that survives from before a manual change to `feedbacks` is caught by the staleness check in section 6.7.

## 6. Postgres queries for the DOTS export

All queries read `feedback_extracted_data` (long form) and pivot it to **one row per submission**. Each persona has its own query (6.1 to 6.4) with its own columns. They **exclude `pii_detected = TRUE`** and, in the export-ready form, also the sessions that were not checked yet (`pii_detected IS NULL`): only `pii_detected = FALSE` rows are exported. `value` is the band (for `age`, the number as text; the queries cast it); an `unmapped` or `not_provided` field shows NULL here. The **`<field>_status` columns** tell them apart: `extracted` (the value is set), `not_provided` (the respondent never answered, or answered only a placeholder such as "N/A"), `unmapped` (answered, but the words fit no allowed value: for example a non-Indian place or an unclear state, since `state` has no `Others` band, or an age outside the allowed range; see 6.9 for the words), and NULL status = no extraction row for that field at all (the persona has no such field, or the config changed). `session_type` and `questionnaire_version` are read from `feedback_submissions`, so they follow a relabel.

### 6.1 Export-ready rows: Youth (PII-checked and clean)

`age` is cast to a number (its stored value is the canonical decimal string, or NULL). `monthly_household_income`, `disability` and `disability_type` are **sensitive**: they are in the commented-out block, **do not include them in a DOTS export until product confirms** (see section 7). They are still extracted and stored.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.age::int AS age,
       e.age_status,
       e.gender,
       e.gender_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.current_activity_status,
       e.current_activity_status_status,
       e.education_level,
       e.education_level_status,
       e.course_details,
       e.course_details_status,
       e.institution_type,
       e.institution_type_status
       -- SENSITIVE columns: do NOT export until product confirms. To include them, remove the leading '-- ' of the
       -- lines below here and in the LATERAL list further down (the comma is part of each commented line).
       -- , e.monthly_household_income
       -- , e.monthly_household_income_status
       -- , e.disability
       -- , e.disability_status
       -- , e.disability_type
       -- , e.disability_type_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'age') AS age,
           max(d.status) FILTER (WHERE d.field_name = 'age') AS age_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'current_activity_status') AS current_activity_status,
           max(d.status) FILTER (WHERE d.field_name = 'current_activity_status') AS current_activity_status_status,
           max(d.value)  FILTER (WHERE d.field_name = 'education_level') AS education_level,
           max(d.status) FILTER (WHERE d.field_name = 'education_level') AS education_level_status,
           max(d.value)  FILTER (WHERE d.field_name = 'course_details') AS course_details,
           max(d.status) FILTER (WHERE d.field_name = 'course_details') AS course_details_status,
           max(d.value)  FILTER (WHERE d.field_name = 'institution_type') AS institution_type,
           max(d.status) FILTER (WHERE d.field_name = 'institution_type') AS institution_type_status
           -- SENSITIVE (see above):
           -- , max(d.value)  FILTER (WHERE d.field_name = 'monthly_household_income') AS monthly_household_income
           -- , max(d.status) FILTER (WHERE d.field_name = 'monthly_household_income') AS monthly_household_income_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability') AS disability
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability') AS disability_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability_type') AS disability_type
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability_type') AS disability_type_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Youth_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.2 Parent

`child_gender` is the CHILD's gender; `school_type` and `child_grade` are the child's. `disability` and `disability_type` are **sensitive** and commented out: **do not include them in a DOTS export until product confirms**.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.child_gender,
       e.child_gender_status,
       e.respondent_relationship,
       e.respondent_relationship_status,
       e.school_type,
       e.school_type_status,
       e.child_grade,
       e.child_grade_status,
       e.parent_education,
       e.parent_education_status,
       e.parent_education_level,
       e.parent_education_level_status
       -- SENSITIVE columns: do NOT export until product confirms. To include them, remove the leading '-- ' of the
       -- lines below here and in the LATERAL list further down (the comma is part of each commented line).
       -- , e.disability
       -- , e.disability_status
       -- , e.disability_type
       -- , e.disability_type_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'child_gender') AS child_gender,
           max(d.status) FILTER (WHERE d.field_name = 'child_gender') AS child_gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'respondent_relationship') AS respondent_relationship,
           max(d.status) FILTER (WHERE d.field_name = 'respondent_relationship') AS respondent_relationship_status,
           max(d.value)  FILTER (WHERE d.field_name = 'school_type') AS school_type,
           max(d.status) FILTER (WHERE d.field_name = 'school_type') AS school_type_status,
           max(d.value)  FILTER (WHERE d.field_name = 'child_grade') AS child_grade,
           max(d.status) FILTER (WHERE d.field_name = 'child_grade') AS child_grade_status,
           max(d.value)  FILTER (WHERE d.field_name = 'parent_education') AS parent_education,
           max(d.status) FILTER (WHERE d.field_name = 'parent_education') AS parent_education_status,
           max(d.value)  FILTER (WHERE d.field_name = 'parent_education_level') AS parent_education_level,
           max(d.status) FILTER (WHERE d.field_name = 'parent_education_level') AS parent_education_level_status
           -- SENSITIVE (see above):
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability') AS disability
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability') AS disability_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability_type') AS disability_type
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability_type') AS disability_type_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Parent_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.3 General Public

No sensitive columns. `age` is cast to a number.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.age::int AS age,
       e.age_status,
       e.gender,
       e.gender_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.occupation,
       e.occupation_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'age') AS age,
           max(d.status) FILTER (WHERE d.field_name = 'age') AS age_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'occupation') AS occupation,
           max(d.status) FILTER (WHERE d.field_name = 'occupation') AS occupation_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'General_Public_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.4 Educator

No sensitive columns.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.gender,
       e.gender_status,
       e.institution_type,
       e.institution_type_status,
       e.teaching_level,
       e.teaching_level_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'institution_type') AS institution_type,
           max(d.status) FILTER (WHERE d.field_name = 'institution_type') AS institution_type_status,
           max(d.value)  FILTER (WHERE d.field_name = 'teaching_level') AS teaching_level,
           max(d.status) FILTER (WHERE d.field_name = 'teaching_level') AS teaching_level_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Educator_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.5 The sessions that were not checked yet (variant: `pii_detected IS NULL`)

These are not in the export-ready rows. Run the extraction for them, or list them:

```sql
SELECT fs.session_type, fs.questionnaire_version, count(*) AS unchecked_sessions
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra' AND fs.pii_detected IS NULL
GROUP BY fs.session_type, fs.questionnaire_version
ORDER BY fs.session_type, fs.questionnaire_version;
```

To list the rows themselves (one persona), use 6.1 with `fs.pii_detected IS NULL` in place of `fs.pii_detected = FALSE`; its field columns will be empty.

### 6.6 How many sessions are excluded, per persona

```sql
SELECT fs.session_type,
       count(*) FILTER (WHERE fs.pii_detected = FALSE) AS exported,
       count(*) FILTER (WHERE fs.pii_detected = TRUE)  AS excluded_pii,
       count(*) FILTER (WHERE fs.pii_detected IS NULL) AS not_checked
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra'
GROUP BY fs.session_type
ORDER BY fs.session_type;
```

### 6.7 Stale flags: sessions that grew after their extraction

A session that gained messages after its extraction is re-processed later; until then its `pii_detected` describes the older text. Exclude these as well in a strict export:

```sql
SELECT fs.submission_id, fs.session_type, fs.pii_detected,
       (fs.meta_data -> 'extraction' ->> 'run_at')::timestamptz AS extracted_at,
       (SELECT max(f.updated_at) FROM feedbacks f
         WHERE f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code) AS last_message_change
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra' AND fs.pii_detected IS NOT NULL
  AND (fs.meta_data -> 'extraction' ->> 'run_at')::timestamptz <
      (SELECT max(f.updated_at) FROM feedbacks f
        WHERE f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code)
ORDER BY fs.session_type, fs.submission_id;
```

### 6.8 Review: statuses per persona and field

```sql
SELECT fs.session_type, d.field_name, d.status, count(*) AS sessions
FROM feedback_extracted_data d
JOIN feedback_submissions fs ON fs.submission_id = d.submission_id AND fs.tenant_code = d.tenant_code
WHERE d.tenant_code = 'mitra'
GROUP BY fs.session_type, d.field_name, d.status
ORDER BY fs.session_type, d.field_name, d.status;
```

### 6.9 Review: the unmapped answers of one field (raw words are respondent text: analysts only, not for sharing)

```sql
SELECT d.submission_id, fs.questionnaire_version, d.field_name, d.raw_value
FROM feedback_extracted_data d
JOIN feedback_submissions fs ON fs.submission_id = d.submission_id AND fs.tenant_code = d.tenant_code
WHERE d.tenant_code = 'mitra' AND fs.session_type = 'Youth_Voice' AND d.field_name = 'state'
  AND d.status = 'unmapped' AND fs.pii_detected = FALSE
ORDER BY d.raw_value;
```

## 7. Caveats and what product still has to confirm (`docs/katha-stories-vs-dots-data-review.md`, `docs/feedback-extraction-bands.md`)

Confirmation items found in review. Until product answers, the affected fields are marked `provisional` in the config (see `show-config` or the bands document for which) and a change is a new config version plus a re-run:

- **Sensitive columns are not for the DOTS export yet.** `monthly_household_income`, `disability` and `disability_type` (Youth) and `disability` and `disability_type` (Parent) are flagged `sensitive` in the config: they are extracted only when the respondent states them explicitly, and they are in the commented-out block of the queries in 6.1 and 6.2. **Do not include them in a DOTS export until product confirms** that they may be exported.
- **State has no `Others` band.** An unclear state or a non-Indian place becomes `unmapped` (status column `state_status`). Question H2 asks for `Others when unclear` and a separate `Not provided` slice: today `not_provided` (never answered) and `unmapped` (answered, not mappable) are kept apart, and product has to say which one reports as `Others`.
- **Gender mapping rules (M6)** are unconfirmed: `skip` / `do not want to say` as `Prefer not to say`, `something else` as `Others`. They live in the band guidance.
- **School and institution type (H8)**: the Youth bot asks everybody, including people who work (about their past school), and there is no `Not applicable` band; the bands differ per persona (see the bands document).
- **Parent child gender** is `Male` / `Female` here; story H5 says `Boy` / `Girl`.
- **Parent state and district: whose? (H4)** The Mitra V2 bot asks for the **child's** state and district, the Google Form asks for the parent's own; the extraction takes the state and district the bot asked for, so the two sources may mean different places.
- **No district reference list.** Districts are free text, normalised by the model, not validated; one district can be spelled several ways. A list (for example LGD) and a validation step are a follow-up.
- **Educator `teaching_level` (B1)**: the bot's question ("primary, middle, high school or college") may not give the story's categories one to one; see the bands document.
- **`age` range**: `min` / `max` of the `age` field are provisional; an age outside them is `unmapped` with the words kept, not clamped.

Other caveats:

- **V2 asks demographics at the end**: many V2 sessions stop earlier, so `not_provided` is common (Youth V2: about 45% had a state answer in the real export; Parent V2 77%; General Public V2 68%). `not_provided` is not the same as `unmapped`, and neither is `Prefer not to say` or `Invalid`, which are real bands.
- **Educator**: 8 sessions in the export so far.
- All stories are still Draft: a changed band means a new config version and a re-run.

## 8. Upgrading a database that already has these tables

This version is unreleased, so these are one-off steps for a database that created the table or seeds earlier:

```sql
-- feedback_extracted_data no longer carries copies of session_type and questionnaire_version (they went stale
-- when sessions were relabelled). Dropping the column also drops the old (session_type, field_name, value) index.
ALTER TABLE feedback_extracted_data DROP COLUMN IF EXISTS session_type;
ALTER TABLE feedback_extracted_data DROP COLUMN IF EXISTS questionnaire_version;
```

Restart the service (or re-apply `schema.sql`) afterwards to create the new `idx_feedback_extracted_field_value` index. Then: write the content of `seed_config.sql` as a new `config_version` for each persona (section 4; the persona prompts arrive with `seed_prompts.sql` at startup), and, if an extraction prompt was changed by hand, keep it (the seed never overwrites it). Use the queries of section 6 as printed here: the older ones read the dropped columns.
