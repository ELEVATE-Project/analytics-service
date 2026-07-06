import os
import sys
import warnings
from pathlib import Path

# Suppress warnings from transformers, UMAP, and OpenMP
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
os.environ["KMP_WARNINGS"] = "0"

# Add the root directory containing the 'app' module to sys.path to resolve imports correctly
# And load the parent .env file to ensure settings are loaded properly when run from a subfolder
current_dir = Path(__file__).resolve().parent
root_dir = None
while current_dir != current_dir.parent:
    if (current_dir / "app").is_dir():
        root_dir = current_dir
        if str(current_dir) not in sys.path:
            sys.path.insert(0, str(current_dir))
        break
    current_dir = current_dir.parent

if root_dir:
    env_path = root_dir / ".env"
    if env_path.is_file():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            # Fallback manual parsing of .env
            with open(env_path, mode="r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "=" in line:
                            key, val = line.split("=", 1)
                            key = key.strip()
                            val = val.strip().strip("'\"")
                            if key:
                                os.environ[key] = val

# Also load .env from the script's own directory (thematic_analysis/) if it exists
script_env_path = Path(__file__).resolve().parent / ".env"
if script_env_path.is_file():
    try:
        from dotenv import load_dotenv
        load_dotenv(script_env_path, override=True)
    except ImportError:
        with open(script_env_path, mode="r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip("'\"")
                        if key:
                            os.environ[key] = val

import argparse
import asyncio
import csv
import json
import re
import logging
import time
import asyncpg
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from tritopic import TriTopic, TriTopicConfig
from app.config import settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("thematic_tritopic")

def is_english(text: str) -> bool:
    """Strictly checks if a string contains only ASCII characters (standard English text), ignoring safe punctuation."""
    # Replace common non-ASCII quotes, dashes, and spaces often found in copy-pasted English text
    safe_text = text
    for char in ['’', '‘', '“', '”', '–', '—', '\xa0', '\u200b']:
        safe_text = safe_text.replace(char, '')
    try:
        safe_text.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def split_and_clean_data(input_string: str) -> list[str]:
    """
    Splits the input string into individual objective statements.
    Handles three formats:
      1. Numbered:  '1. Good education | 2. Skilled teachers | 3. ...'
      2. Pipe-only: 'Statement one. | Statement two. | Statement three.'
      3. Single:    'Just one objective statement.'
    Returns a list of cleaned, non-empty strings.
    """
    text = input_string.strip()
    if not text:
        return []

    # Check if the text contains numbered prefixes like "1. ", "2. "
    has_numbering = bool(re.search(r'(?:^|[|\s])\d+\.\s', text))

    if has_numbering:
        # Split on numbering prefixes (e.g. "1. ", "2. ")
        raw_items = re.split(r'\s*\d+\.\s*', text)
    elif '|' in text:
        # Split on pipe separator
        raw_items = text.split('|')
    else:
        # Single statement — return as-is
        return [text]

    # Clean each item: strip whitespace and trailing/leading pipes
    cleaned = []
    for item in raw_items:
        item_str = item.strip().strip('|').strip()
        if item_str:
            cleaned.append(item_str)

    return cleaned if cleaned else [text]

# Semantic matching settings
# CROSS_REF_SIMILARITY_THRESHOLD is now a CLI argument (--threshold), default 0.70



async def migrate_database(conn: asyncpg.Connection):
    """Performs the dynamic database migrations for the themes table."""
    logger.info("Running database migration checks...")
    # Add total_objective_count if not exists
    await conn.execute(
        "ALTER TABLE themes ADD COLUMN IF NOT EXISTS total_objective_count INTEGER DEFAULT 0;"
    )
    # Add original_statement_text if not exists
    await conn.execute(
        "ALTER TABLE themes ADD COLUMN IF NOT EXISTS original_statement_text TEXT[] DEFAULT '{}';"
    )
    logger.info("Database migration checks completed.")

    # Seed approved themes if table is empty
    count = await conn.fetchval("SELECT COUNT(*) FROM themes")
    if int(count or 0) == 0:
        seed_file = Path(__file__).resolve().parent / "seed_themes.sql"
        if seed_file.exists():
            logger.info("Seeding pre-approved themes from seed_themes.sql...")
            seed_sql = seed_file.read_text(encoding="utf-8")
            await conn.execute(seed_sql)
            logger.info("Successfully seeded pre-approved themes.")
        else:
            logger.warning("seed_themes.sql not found; skipping seeding.")


async def fetch_approved_themes(conn: asyncpg.Connection):
    """Fetches all approved themes from themes table."""
    rows = await conn.fetch(
        "SELECT id, name, definitions as definition, keywords, examples FROM themes WHERE status ILIKE 'approved'"
    )
    return [dict(row) for row in rows]


async def save_mappings_to_db(conn: asyncpg.Connection, mapped_themes_data: dict, draft_themes: list, all_statements_dict: dict):
    """Saves updates to the approved themes and inserts new draft themes."""
    # 1. Update Approved Themes
    logger.info("Saving mappings for approved themes in database...")
    for theme_id, mappings in mapped_themes_data.items():
        # Format mapping items into JSON strings
        new_items = []
        for m in mappings:
            story_id = m.get("story_id") or m.get("discussion_id") or m.get("id")
            orig_text = m.get("text") or all_statements_dict.get(story_id, "")
            score = float(m["score"])
            new_items.append(json.dumps({"id": story_id, "text": orig_text, "score": score}, ensure_ascii=False))

        # Update theme by directly overwriting the values
        await conn.execute(
            """
            UPDATE themes
            SET total_objective_count = $2,
                original_statement_text = $3::TEXT[]
            WHERE id = $1::UUID
            """,
            theme_id, len(new_items), new_items
        )
    logger.info("Approved themes successfully updated.")

    # 2. Save Draft Themes
    logger.info("Inserting generated Draft candidate themes...")
    for dt in draft_themes:
        theme_name = dt["theme_name"]
        theme_def = dt["theme_definition"]
        keywords_str = ",".join(dt["keywords"]) if isinstance(dt["keywords"], list) else dt["keywords"]
        examples_str = "|".join(dt["examples"]) if isinstance(dt["examples"], list) else dt["examples"]
        mappings = dt["mappings"]

        new_items = []
        for m in mappings:
            story_id = m.get("story_id") or m.get("discussion_id") or m.get("id")
            orig_text = m.get("text") or all_statements_dict.get(story_id, "")
            score = float(m["score"])
            new_items.append(json.dumps({"id": story_id, "text": orig_text, "score": score}, ensure_ascii=False))

        # Check if theme already exists in draft
        existing_id = await conn.fetchval("SELECT id FROM themes WHERE name = $1", theme_name)
        if existing_id:
            # Update existing by overwriting the values
            await conn.execute(
                """
                UPDATE themes
                SET total_objective_count = $2,
                    original_statement_text = $3::TEXT[],
                    status = 'Draft'
                WHERE id = $1
                """,
                existing_id, len(new_items), new_items
            )
        else:
            # Insert new
            await conn.execute(
                """
                INSERT INTO themes (name, definitions, keywords, examples, status, total_objective_count, original_statement_text)
                VALUES ($1, $2, $3, $4, 'Draft', $5, $6::TEXT[])
                """,
                theme_name, theme_def, keywords_str, examples_str, len(new_items), new_items
            )
    logger.info("Draft candidate themes successfully updated.")


# =========================================================================
# LLM NAMING & DEFINITION GENERATION
# =========================================================================

from app.services.llm import openrouter_chat_completion

def estimate_tokens(text: str) -> int:
    try:
        import tiktoken
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    except Exception:
        # Fallback to standard character-based heuristic: ~4 chars per token for English
        return max(1, int(len(text) / 4))

def generate_draft_theme_metadata_llm(cluster_texts: list) -> dict:
    """
    Given a list of representative objectives belonging to a cluster,
    uses LLM to generate a concise Theme Name, definition, and keywords.
    """
    sample_texts = cluster_texts[:15]
    prompt = f"""
    You are an educational researcher. Group these similar objectives under a single concise theme:
    Objectives:
    {json.dumps(sample_texts, indent=2)}

    Generate a Theme Name, a detailed and comprehensive Theme Definition (2-3 sentences outlining the semantic boundaries of this theme, what it includes, and the context or impact, matching the style and depth of academic definitions), and Keywords.
    Return ONLY a valid English-only JSON object matching:
    {{
        "theme_name": "Concise Theme Name",
        "theme_definition": "Detailed, comprehensive semantic definition of 2-3 sentences outlining what the theme captures and includes.",
        "keywords": ["key1", "key2", "key3"]
    }}
    Do not wrap in ```json ... ``` code blocks.
    """
    
    prompt_tokens = estimate_tokens(prompt)
    max_retries = 10
    retry_delay = 60
    
    for attempt in range(1, max_retries + 1):
        try:
            response_text = openrouter_chat_completion(prompt)
            completion_tokens = estimate_tokens(response_text)
            
            # clean markdown wrappers
            cleaned_text = response_text
            if cleaned_text.strip().startswith("```"):
                lines = cleaned_text.strip().splitlines()
                if lines[0].startswith("```json") or lines[0].startswith("```"):
                    lines = lines[1:-1]
                cleaned_text = "\n".join(lines).strip()
            
            result = json.loads(cleaned_text)
            result["prompt_tokens"] = prompt_tokens
            result["completion_tokens"] = completion_tokens
            result["total_tokens"] = prompt_tokens + completion_tokens
            return result
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "rate limit" in error_str.lower():
                if attempt < max_retries:
                    logger.warning(f"Rate limit exceeded (429) on attempt {attempt}/{max_retries}. Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                    continue
            
            logger.error(f"Failed to generate theme metadata using LLM (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                return {
                    "theme_name": "Error Theme",
                    "theme_definition": "Error fallback theme definition",
                    "keywords": ["error"],
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": 0,
                    "total_tokens": prompt_tokens
                }
# =========================================================================

async def generate_pm_review_csv(conn: asyncpg.Connection, output_path: Path):
    logger.info(f"Generating PM review CSV file at {output_path.resolve()}...")
    rows = await conn.fetch(
        """
        SELECT id, name, definitions, keywords, status, total_objective_count, original_statement_text
        FROM themes
        """
    )
    themes = [dict(row) for row in rows]
    
    approved_themes = []
    draft_gt_10 = []
    draft_lte_10 = []
    others = []

    for t in themes:
        status = (t["status"] or "").strip()
        count = t["total_objective_count"] or 0
        if status.lower() == "approved":
            approved_themes.append(t)
        elif status.lower() == "draft":
            if count > 10:
                draft_gt_10.append(t)
            else:
                draft_lte_10.append(t)
        else:
            others.append(t)

    approved_themes.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
    draft_gt_10.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
    draft_lte_10.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
    others.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)

    ordered_themes = approved_themes + draft_gt_10 + draft_lte_10 + others
    headers = ["Theme Id", "Theme Name", "Defination", "Keywords", "Status", "Objective Count", "Original Statements"]
    
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for theme in ordered_themes:
            theme_id = str(theme["id"])
            theme_name = theme["name"]
            theme_def = theme["definitions"] or ""
            theme_kw = theme["keywords"] or ""
            theme_status = theme["status"] or ""
            theme_count = theme["total_objective_count"] or 0
            
            statements = []
            statement_items = theme["original_statement_text"] or []
            for item in statement_items:
                try:
                    data = json.loads(item)
                    s_id = data.get("id")
                    s_text = data.get("text", "")
                    s_score = data.get("score")
                    
                    if isinstance(s_score, (int, float)):
                        s_score_str = f"{s_score:.4f}".rstrip("0").rstrip(".") if s_score != 0 else "0"
                    else:
                        s_score_str = str(s_score)
                        
                    if " | " in s_text:
                        formatted = f"{s_id} | {s_text}"
                    else:
                        formatted = f"{s_id} | {s_text} | {s_score_str}"
                    statements.append(formatted)
                except Exception:
                    statements.append(str(item))

            if not statements:
                writer.writerow([theme_id, theme_name, theme_def, theme_kw, theme_status, theme_count, ""])
            else:
                writer.writerow([theme_id, theme_name, theme_def, theme_kw, theme_status, theme_count, statements[0]])
                for stmt in statements[1:]:
                    writer.writerow(["", "", "", "", "", "", stmt])

import sys
import io
from contextlib import contextmanager

class CleanStdoutRedirector(io.TextIOBase):
    def __init__(self, original_stdout, logger_func):
        self.original_stdout = original_stdout
        self.logger_func = logger_func
        self.buffer = ""

    def write(self, s):
        self.buffer += s
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            line_lower = line.lower()
            if "iteration" in line_lower or "ari" in line_lower or "converged" in line_lower or "fitting complete" in line_lower or "fitting model" in line_lower:
                cleaned = line.strip()
                if "iteration" in line_lower and "/" in line_lower:
                    self.logger_func(f"TriTopic {cleaned}")
                elif "ari vs previous" in line_lower:
                    self.logger_func(f"TriTopic {cleaned}")
                elif "converged" in line_lower:
                    self.logger_func(f"TriTopic {cleaned}")
                elif "fitting complete" in line_lower:
                    self.logger_func(f"TriTopic {cleaned}")
                else:
                    self.logger_func(cleaned)
        return len(s)

    def flush(self):
        pass

@contextmanager
def clean_tritopic_print():
    original_stdout = sys.stdout
    sys.stdout = CleanStdoutRedirector(original_stdout, logger.info)
    try:
        yield
    finally:
        sys.stdout = original_stdout



async def main_async():
    parser = argparse.ArgumentParser(description="Standalone batch thematic analysis runner using TriTopic.")
    parser.add_argument(
        "--csv",
        default="story_objectives.csv",
        help="Path to the story objectives CSV file."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of objectives to process (useful for testing)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=30,
        help="Batch size for processing (retained for parameter alignment)."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.70,
        help="Cosine similarity threshold for mapping to approved themes (e.g., 0.60, 0.70)."
    )
    args = parser.parse_args()

    # Derive the similarity threshold and output paths from --threshold
    similarity_threshold = args.threshold
    threshold_str = f"{similarity_threshold:.2f}"
    output_base_dir = Path(__file__).resolve().parent  # thematic_analysis directory
    viz_dir = output_base_dir / f"{threshold_str}_review_visualizations"
    viz_dir.mkdir(parents=True, exist_ok=True)
    output_csv_path = viz_dir / "tritopic_review.csv"
    logger.info(f"Similarity threshold: {similarity_threshold}")
    logger.info(f"Output directory: {viz_dir}")
    logger.info(f"Output CSV: {output_csv_path}")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"Objectives CSV file not found: {csv_path}")
        sys.exit(1)

    # 1. Load CSV objectives
    raw_objectives = []
    skipped_non_english_count = 0
    skipped_short_count = 0
    empty_objectives_count = 0
    total_csv_rows_in_file = 0
    total_csv_rows_processed = 0

    # Count total CSV rows in the file for statistics
    with open(csv_path, mode="r", encoding="utf-8") as f_count:
        total_csv_rows_in_file = sum(1 for _ in csv.DictReader(f_count))

    if args.limit:
        logger.info(f"Limiting execution to first {args.limit} valid English objectives.")

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        
        # Match ID column dynamically
        id_col = None
        for col in ["story_id", "discussion_id", "id", "ID"]:
            if col in headers:
                id_col = col
                break
        if not id_col:
            for col in headers:
                if "id" in col.lower():
                    id_col = col
                    break
        
        # Match Text column dynamically
        text_col = None
        for col in ["Challenges", "objectives", "objective", "challenge", "text"]:
            if col in headers:
                text_col = col
                break
        if not text_col:
            for col in headers:
                col_lower = col.lower()
                if "objective" in col_lower or "challenge" in col_lower or "text" in col_lower or "content" in col_lower:
                    text_col = col
                    break
                    
        if not id_col or not text_col:
            logger.error(f"Could not dynamically detect ID or text columns in CSV headers: {headers}")
            sys.exit(1)
            
        logger.info(f"Dynamically detected ID column: '{id_col}', Text column: '{text_col}'")

        for row in reader:
            if args.limit and len(raw_objectives) >= args.limit:
                break
            total_csv_rows_processed += 1
            
            story_id = row.get(id_col)
            raw_val = row.get(text_col, "")
            if raw_val is not None:
                raw_val = raw_val.strip()
            else:
                raw_val = ""
            
            # Check for empty/missing objective text
            if not story_id or not raw_val:
                empty_objectives_count += 1
                continue

            try:
                clean_id = int(str(story_id).replace(",", ""))
            except ValueError:
                clean_id = str(story_id)

            split_statements = split_and_clean_data(raw_val)
            if not split_statements:
                empty_objectives_count += 1
                continue

            for idx, stmt in enumerate(split_statements, 1):
                if args.limit and len(raw_objectives) >= args.limit:
                    break

                if not is_english(stmt):
                    skipped_non_english_count += 1
                    continue

                if len(stmt.split()) < 3:
                    skipped_short_count += 1
                    continue

                raw_objectives.append({
                    "id": clean_id,
                    "objective": stmt,
                    "raw_unsplit": raw_val
                })

    logger.info(f"Loaded {len(raw_objectives)} valid English objectives (skipped {skipped_non_english_count} non-English, skipped {skipped_short_count} less than 3 words, skipped {empty_objectives_count} empty).")

    # Connect database & run migrations
    database_url = "postgresql://postgres:postgres@localhost:5432/analytics_db"
    conn = await asyncpg.connect(dsn=database_url)
    try:
        await migrate_database(conn)

        # Fetch approved themes
        approved_themes = await fetch_approved_themes(conn)
        logger.info(f"Fetched {len(approved_themes)} approved themes from database.")

        # Clear existing Draft candidate themes in the database to prevent stale drafts piling up
        logger.info("Clearing existing Draft themes in database...")
        await conn.execute("DELETE FROM themes WHERE status = 'Draft'")

        # Reset counts and statement arrays for all approved themes
        logger.info("Resetting counts and mapping statement arrays for approved themes...")
        await conn.execute(
            "UPDATE themes SET total_objective_count = 0, original_statement_text = '{}' WHERE status ILIKE 'approved'"
        )

        if not raw_objectives:
            logger.info("No objectives to process.")
            return

        # 2. Initialize local Sentence Transformer model
        logger.info("Initializing SentenceTransformer model 'all-MiniLM-L6-v2' (runs locally)...")
        # In TriTopic, we can leverage the sentence-transformers model directly
        sentence_model = SentenceTransformer("all-MiniLM-L6-v2")

        # 3. Create embeddings for approved themes (multi-vector representation using base details and individual examples)
        logger.info("Encoding approved themes (multi-vector representation)...")
        approved_themes_vectors = {}
        for theme in approved_themes:
            base_text = f"Theme: {theme['name']}. Definition: {theme['definition'] or ''}. Keywords: {theme['keywords'] or ''}."
            base_emb = sentence_model.encode(base_text)
            
            # Split examples and encode them individually
            example_embs = []
            if theme.get("examples"):
                example_list = [ex.strip() for ex in theme["examples"].split("|") if ex.strip()]
                if example_list:
                    example_embs = sentence_model.encode(example_list)
            
            # Stack theme vectors
            if len(example_embs) > 0:
                theme_vectors = np.vstack([base_emb.reshape(1, -1), example_embs])
            else:
                theme_vectors = base_emb.reshape(1, -1)
            
            approved_themes_vectors[str(theme["id"])] = theme_vectors

        # 4. Create embeddings for raw objectives
        logger.info("Encoding objectives...")
        objectives_list = [item["objective"] for item in raw_objectives]
        objectives_embeddings = sentence_model.encode(objectives_list, show_progress_bar=True)

        # 5. Classify objectives using multi-vector cosine similarity (max similarity over base details & examples)
        logger.info("Calculating similarity and mapping to approved themes...")
        mapped_themes_data = {str(t["id"]): [] for t in approved_themes}
        unmapped_indices = []

        for i, obj_emb in enumerate(objectives_embeddings):
            obj_emb_reshaped = obj_emb.reshape(1, -1)
            best_theme_id = None
            best_score = -1.0
            
            for theme in approved_themes:
                theme_id = str(theme["id"])
                theme_vectors = approved_themes_vectors[theme_id]
                
                # Compute similarities between objective and all vectors of this theme
                sims = cosine_similarity(obj_emb_reshaped, theme_vectors)[0]
                max_sim = np.max(sims)
                
                if max_sim > best_score:
                    best_score = max_sim
                    best_theme_id = theme_id
            
            story_id = raw_objectives[i]["id"]
            if best_score >= similarity_threshold:
                mapped_themes_data[best_theme_id].append({
                    "story_id": story_id,
                    "discussion_id": story_id,
                    "text": raw_objectives[i]["objective"],
                    "score": float(best_score)
                })
            else:
                unmapped_indices.append(i)

        logger.info(f"Mapped to approved themes: {sum(len(v) for v in mapped_themes_data.values())}")
        logger.info(f"Unmapped objectives: {len(unmapped_indices)}")

        # 6. Cluster unmapped objectives locally using TriTopic (ConsensusLeiden + Iterative Refinement)
        draft_themes = []
        if unmapped_indices:
            logger.info("Clustering unmapped objectives locally using TriTopic...")
            unmapped_texts = [raw_objectives[idx]["objective"] for idx in unmapped_indices]
            unmapped_embeddings = objectives_embeddings[unmapped_indices]
            
            # Configure TriTopic for the unmapped subset.
            # - Bypasses dimension reduction if N is very small to avoid UMAP neighborhood issues.
            # - We keep verbose logging enabled to show intermediate refinement states.
            tritopic_cfg = TriTopicConfig(
                use_dim_reduction=len(unmapped_texts) >= 15,
                use_lexical_view=True,
                use_iterative_refinement=True,
                resolution=0.75,
                n_consensus_runs=15,
                max_iterations=8,
                convergence_threshold=0.95,
                min_cluster_size=2,
                verbose=True
                # knn_backend="exact"
            )
            tritopic_model = TriTopic(tritopic_cfg)
            
            with clean_tritopic_print():
                tritopic_model.fit(unmapped_texts, embeddings=unmapped_embeddings)
            
            # Map TriTopic local cluster labels to candidate themes
            # Filter out outliers (marked as -1)
            unique_labels = np.unique(tritopic_model.labels_)
            unique_labels = unique_labels[unique_labels != -1]
            logger.info(f"Identified {len(unique_labels)} new candidate clusters via TriTopic.")

            all_statements_dict = {item["id"]: item["objective"] for item in raw_objectives}
            
            for local_id in unique_labels:
                cluster_member_indices = [unmapped_indices[j] for j, l in enumerate(tritopic_model.labels_) if l == local_id]
                
                # Extract objectives belonging to this cluster
                cluster_objectives = [raw_objectives[idx] for idx in cluster_member_indices]
                cluster_texts = [item["objective"] for item in cluster_objectives]

                # Fetch cluster details from TriTopic (which handles c-TF-IDF keyword extraction)
                topic_info_obj = tritopic_model.get_topic(local_id)
                keywords = topic_info_obj.keywords[:5]

                # # Average similarity within the cluster as the confidence score
                # score = 0.85
                # Calculate average similarity of cluster members to the cluster centroid
                cluster_embs = objectives_embeddings[cluster_member_indices]
                cluster_centroid = np.mean(cluster_embs, axis=0).reshape(1, -1)
                member_similarities = cosine_similarity(cluster_embs, cluster_centroid)
                score = float(np.mean(member_similarities))

                # Generate metadata (LLM call is used only if cluster size > 10)
                p_tokens, c_tokens, t_tokens = 0, 0, 0
                if len(cluster_texts) > 10:
                    logger.info(f"Generating theme name & definition via LLM for Candidate Theme (Cluster {local_id + 1}) with {len(cluster_texts)} objectives...")
                    llm_meta = generate_draft_theme_metadata_llm(cluster_texts)
                    theme_name = llm_meta.get("theme_name", f"Candidate Theme (Cluster {local_id + 1})")
                    theme_def = llm_meta.get("theme_definition", f"Locally clustered theme containing {len(cluster_texts)} objectives. Sample text: '{cluster_texts[0]}'")
                    keywords = llm_meta.get("keywords", keywords)
                    p_tokens = llm_meta.get("prompt_tokens", 0)
                    c_tokens = llm_meta.get("completion_tokens", 0)
                    t_tokens = llm_meta.get("total_tokens", 0)
                    logger.info(f"LLM generated theme '{theme_name}' using {p_tokens} prompt tokens and {c_tokens} completion tokens (Total: {t_tokens}).")
                else:
                    theme_name = f"Candidate Theme (Cluster {local_id + 1})"
                    theme_def = f"Locally clustered theme containing {len(cluster_texts)} objectives. Sample text: '{cluster_texts[0]}'"
                
                examples = cluster_texts[:3]

                draft_themes.append({
                    "theme_name": theme_name,
                    "theme_definition": theme_def,
                    "keywords": keywords,
                    "examples": examples,
                    "confidence_score": score,
                    "prompt_tokens": p_tokens,
                    "completion_tokens": c_tokens,
                    "total_tokens": t_tokens,
                    "mappings": [
                        {
                            "story_id": item["id"],
                            "discussion_id": item["id"],
                            "text": item["objective"],
                            "score": float(member_similarities[j][0])
                        }
                        for j, item in enumerate(cluster_objectives)
                    ]
                })

                # Update the label in the model's topic metadata so that visualizations pick it up!
                # Truncate long names for chart readability (full name is preserved in CSV/DB)
                short_label = (theme_name[:37] + "...") if len(theme_name) > 40 else theme_name
                for t in tritopic_model.topics_:
                    if t.topic_id == local_id:
                        t.label = short_label

            # Generate HTML visualizations if draft_themes were created
            if draft_themes:
                logger.info("Generating interactive Plotly visualizations...")
                
                # 1. 2D Document Map
                try:
                    fig_doc = tritopic_model.visualize(width=1000, height=700)
                    fig_doc.update_layout(
                        margin=dict(l=60, r=60, t=80, b=60),
                    )
                    fig_doc.write_html(str(viz_dir / "document_map.html"))
                    fig_doc.write_json(str(viz_dir / "document_map.json"))
                    logger.info(f"Saved 2D Document Map to {viz_dir / 'document_map.html'}")
                except Exception as e:
                    logger.error(f"Failed to generate 2D Document Map: {e}")
                    
                # 2. Horizontal Keyword Bars
                try:
                    n_topics = len(unique_labels)
                    # Scale height: ~300px per topic, capped at 8000px to avoid Plotly crashes
                    # Reduce n_keywords for large topic counts to keep the chart manageable
                    n_kw = 8 if n_topics > 15 else 15
                    kw_height = min(max(600, 300 * n_topics), 8000)
                    fig_topics = tritopic_model.visualize_topics(width=1000, height=kw_height, n_keywords=n_kw)
                    # Increase left margin for y-axis labels
                    fig_topics.update_layout(
                        margin=dict(l=220, r=60, t=80, b=60),
                    )
                    # Push subplot titles upward for breathing room
                    for ann in fig_topics.layout.annotations:
                        if hasattr(ann, 'y'):
                            ann.update(yshift=10)
                    fig_topics.write_html(str(viz_dir / "topics_keywords.html"))
                    fig_topics.write_json(str(viz_dir / "topics_keywords.json"))
                    logger.info(f"Saved Topics Keywords (height={kw_height}px, n_topics={n_topics}, n_keywords={n_kw}) to {viz_dir / 'topics_keywords.html'}")
                except Exception as e:
                    logger.error(f"Failed to generate Topics Keywords: {e}")


                # 3. Hierarchy Dendrogram (custom build with distance annotations)
                try:
                    from scipy.cluster.hierarchy import linkage, dendrogram
                    from scipy.spatial.distance import pdist
                    import plotly.graph_objects as go

                    valid_topics = [t for t in tritopic_model.topics_ if t.topic_id != -1]
                    topic_embs = tritopic_model.topic_embeddings_
                    distances = pdist(topic_embs, metric="cosine")
                    Z = linkage(distances, method="ward")
                    labels_dendro = [t.label or f"Topic {t.topic_id}" for t in valid_topics]
                    dendro = dendrogram(Z, labels=labels_dendro, no_plot=True)

                    fig_hier = go.Figure()
                    icoord = dendro["icoord"]
                    dcoord = dendro["dcoord"]

                    # Draw the U-shaped merge lines
                    for xs, ys in zip(icoord, dcoord):
                        fig_hier.add_trace(go.Scatter(
                            x=xs, y=ys,
                            mode="lines",
                            line=dict(color="#636EFA", width=2),
                            showlegend=False,
                            hoverinfo="skip",
                        ))

                    # Add distance annotations at each merge point (top of the U)
                    for xs, ys in zip(icoord, dcoord):
                        # The merge height is the horizontal segment: ys[1] == ys[2]
                        merge_height = ys[1]
                        merge_x = (xs[1] + xs[2]) / 2  # center of horizontal bar
                        fig_hier.add_annotation(
                            x=merge_x,
                            y=merge_height,
                            text=f"<b>{merge_height:.2f}</b>",
                            showarrow=False,
                            font=dict(size=10, color="#333"),
                            yshift=10,  # shift above the line
                            bgcolor="rgba(255,255,255,0.8)",
                            borderpad=2,
                        )

                    fig_hier.update_layout(
                        title=dict(text="Topic Hierarchy", font=dict(size=16)),
                        width=1000,
                        height=700,
                        margin=dict(l=80, r=80, t=80, b=280),
                        xaxis=dict(
                            ticktext=dendro["ivl"],
                            tickvals=list(range(5, len(dendro["ivl"]) * 10, 10)),
                        tickangle=-35,
                        tickfont=dict(size=11),
                        automargin=True,
                        ),
                        yaxis=dict(title="Distance"),
                        template="plotly_white",
                    )
                    fig_hier.write_html(str(viz_dir / "hierarchy_tree.html"))
                    fig_hier.write_json(str(viz_dir / "hierarchy_tree.json"))
                    logger.info(f"Saved Hierarchy Tree (with distance annotations) to {viz_dir / 'hierarchy_tree.html'}")
                except Exception as e:
                    logger.error(f"Failed to generate Hierarchy Tree: {e}")

                # 4. Cosine Similarity Heatmap
                try:
                    from tritopic.visualization.plotter import TopicVisualizer
                    visualizer = TopicVisualizer()
                    fig_sim = visualizer.plot_topic_similarity(
                        topic_embeddings=tritopic_model.topic_embeddings_,
                        topics=tritopic_model.topics_,
                        width=900,
                        height=900
                    )
                    # Increase margins for long axis labels on both axes
                    fig_sim.update_layout(
                        margin=dict(l=250, r=60, t=80, b=250),
                    )
                    fig_sim.update_xaxes(
                        tickangle=-35,
                        tickfont=dict(size=11),
                        automargin=True,
                    )
                    fig_sim.update_yaxes(
                        tickfont=dict(size=11),
                        automargin=True,
                    )
                    fig_sim.write_html(str(viz_dir / "topic_similarity.html"))
                    fig_sim.write_json(str(viz_dir / "topic_similarity.json"))
                    logger.info(f"Saved Topic Similarity Heatmap to {viz_dir / 'topic_similarity.html'}")
                except Exception as e:
                    logger.error(f"Failed to generate Topic Similarity Heatmap: {e}")

        # 7. Save results back to Postgres database
        all_statements_dict = {item["id"]: item.get("raw_unsplit", item["objective"]) for item in raw_objectives}
        await save_mappings_to_db(conn, mapped_themes_data, draft_themes, all_statements_dict)

        # 8. Print overview statistics
        mapped_count = sum(len(v) for v in mapped_themes_data.values())
        unmapped_count = len(unmapped_indices)
        
        cluster_counts = {i: 0 for i in range(1, 11)}
        clusters_gt_10 = 0
        for dt in draft_themes:
            size = len(dt["mappings"])
            if size > 10:
                clusters_gt_10 += 1
            elif size in cluster_counts:
                cluster_counts[size] += 1

        total_prompt_tokens = sum(dt.get("prompt_tokens", 0) for dt in draft_themes)
        total_completion_tokens = sum(dt.get("completion_tokens", 0) for dt in draft_themes)
        total_llm_tokens = total_prompt_tokens + total_completion_tokens

        logger.info("=========================================")
        logger.info("           OVERVIEW STATISTICS           ")
        logger.info("=========================================")
        logger.info(f"Total CSV rows:                          {total_csv_rows_in_file}")
        logger.info(f"Total CSV rows processed:                {total_csv_rows_processed}")
        logger.info(f"Total valid objectives/challenges:       {len(raw_objectives)}")
        logger.info(f"Total skipped non-English:               {skipped_non_english_count}")
        logger.info(f"Total skipped short (<3 words):          {skipped_short_count}")
        logger.info(f"Total empty objectives:                  {empty_objectives_count}")
        logger.info(f"Mapped to approved themes:               {mapped_count}")
        logger.info(f"Unmapped objectives:                     {unmapped_count}")
        logger.info(f"Identified new candidate clusters:       {len(draft_themes)}")
        logger.info(f"Clusters with count > 10 objectives:     {clusters_gt_10}")
        logger.info("-----------------------------------------")
        for size in range(1, 11):
            logger.info(f"Number of clusters with count = {size:2d}:       {cluster_counts[size]}")
        logger.info("-----------------------------------------")
        logger.info(f"Total LLM Prompt Tokens:                 {total_prompt_tokens}")
        logger.info(f"Total LLM Completion Tokens:             {total_completion_tokens}")
        logger.info(f"Total LLM Tokens Used:                   {total_llm_tokens}")
        logger.info("=========================================")

        # 9. Generate PM review CSV automatically
        logger.info(f"Generating PM review CSV file at {output_csv_path}...")
        await generate_pm_review_csv(conn, output_csv_path)

        # 10. Save run metadata JSON to viz_dir for the Streamlit dashboard
        import json as _json
        from datetime import datetime as _dt
        draft_mapped_count = sum(len(dt["mappings"]) for dt in draft_themes)
        run_meta = {
            "run_timestamp":            _dt.now().isoformat(timespec="seconds"),
            "similarity_threshold":     similarity_threshold,
            "total_objectives_in_csv":  total_csv_rows_in_file,
            "total_objectives_processed": total_csv_rows_processed,
            "skipped_non_english":      skipped_non_english_count,
            "skipped_short":            skipped_short_count,
            "skipped_empty":            empty_objectives_count,
            "mapped_to_approved_themes": mapped_count,
            "unmapped_after_approved":  unmapped_count,
            "draft_clusters_identified": len(draft_themes),
            "clusters_gt_10":           clusters_gt_10,
            "mapped_to_draft_themes":   draft_mapped_count,
            "total_mapped":             mapped_count + draft_mapped_count,
            "llm_prompt_tokens":        total_prompt_tokens,
            "llm_completion_tokens":    total_completion_tokens,
            "llm_total_tokens":         total_llm_tokens,
        }
        try:
            meta_path = viz_dir / "run_meta.json"
            with open(str(meta_path), "w") as _f:
                _json.dump(run_meta, _f, indent=2)
            logger.info(f"Saved run metadata to {meta_path}")
        except Exception as e:
            logger.warning(f"Could not save run_meta.json: {e}")

    finally:
        await conn.close()
    
    logger.info("Batch thematic analysis successfully completed!")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
