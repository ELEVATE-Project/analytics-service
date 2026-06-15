INSERT INTO prompts (name, analysis_type, created_at, updated_at)
VALUES
  ('PII Default', 'pii', now(), now()),
  ('Theme Default', 'theme', now(), now())
ON CONFLICT (name) DO NOTHING;

INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  1,
  'Mask personally identifiable information from the text while preserving meaning and structure.',
  'Please mask all personally identifiable information in the following content:\n\n{{content}}',
  TRUE,
  'Seeded default PII prompt',
  now()
FROM prompts p
WHERE p.name = 'PII Default'
ON CONFLICT (prompt_id, version) DO NOTHING;

INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note, created_at)
SELECT
  p.id,
  1,
  'Perform thematic analysis and return a concise theme name, definition, keywords, and confidence score.',
  'Analyze the following content and extract the dominant theme:\n\n{{content}}',
  TRUE,
  'Seeded default theme prompt',
  now()
FROM prompts p
WHERE p.name = 'Theme Default'
ON CONFLICT (prompt_id, version) DO NOTHING;
