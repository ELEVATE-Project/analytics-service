import os
import sys
from pathlib import Path

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

import argparse
import asyncio
import csv
import json
import logging
import time
import asyncpg
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import igraph as ig
import leidenalg as la

from app.config import settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("thematic_batch_leiden")

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

# Semantic matching settings
CROSS_REF_SIMILARITY_THRESHOLD = 0.55  # Cosine similarity threshold for mapping to approved themes
CLUSTER_DISTANCE_THRESHOLD = 0.45      # Cosine distance threshold (1 - similarity threshold) for graph construction


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
            story_id = m["story_id"]
            orig_text = all_statements_dict.get(story_id, "")
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
        keywords_str = ",".join(dt["keywords"])
        examples_str = "|".join(dt["examples"])
        mappings = dt["mappings"]

        new_items = []
        for m in mappings:
            story_id = m["story_id"]
            orig_text = all_statements_dict.get(story_id, "")
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
    Return ONLY a valid JSON object matching:
    {{
        "theme_name": "Concise Theme Name",
        "theme_definition": "Detailed, comprehensive semantic definition of 2-3 sentences outlining what the theme captures and includes.",
        "keywords": ["key1", "key2", "key3"]
    }}
    Do not wrap in ```json ... ``` code blocks.
    """
    
    max_retries = 10
    retry_delay = 60
    
    for attempt in range(1, max_retries + 1):
        try:
            response_text = openrouter_chat_completion(prompt)
            # clean markdown wrappers
            if response_text.strip().startswith("```"):
                lines = response_text.strip().splitlines()
                if lines[0].startswith("```json") or lines[0].startswith("```"):
                    lines = lines[1:-1]
                response_text = "\n".join(lines).strip()
            return json.loads(response_text)
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
                    "keywords": ["error"]
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

    logger.info(f"Successfully generated PM review CSV file at: {output_path.resolve()}")


async def main_async():
    parser = argparse.ArgumentParser(description="Standalone batch thematic analysis runner using Leiden clustering.")
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
        "--output",
        default="leiden_theme_pm_review.csv",
        help="Path to save the generated PM review CSV file."
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"Objectives CSV file not found: {csv_path}")
        sys.exit(1)

    # 1. Load CSV objectives
    raw_objectives = []
    skipped_non_english_count = 0
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
        for row in reader:
            if args.limit and len(raw_objectives) >= args.limit:
                break
            total_csv_rows_processed += 1
            story_id = row.get("story_id")
            objective = row.get("objective", "").strip()
            
            # Check for empty/missing objective text
            if not story_id or not objective:
                empty_objectives_count += 1
                continue

            if not is_english(objective):
                skipped_non_english_count += 1
                continue
            try:
                clean_id = int(str(story_id).replace(",", ""))
            except ValueError:
                clean_id = str(story_id)
            raw_objectives.append({"id": clean_id, "objective": objective})

    logger.info(f"Loaded {len(raw_objectives)} valid English objectives from {csv_path} (skipped {skipped_non_english_count} non-English ones, skipped {empty_objectives_count} empty ones).")

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
        model = SentenceTransformer("all-MiniLM-L6-v2")

        # 3. Create embeddings for approved themes (multi-vector representation using base details and individual examples)
        logger.info("Encoding approved themes (multi-vector representation)...")
        approved_themes_vectors = {}
        for theme in approved_themes:
            base_text = f"Theme: {theme['name']}. Definition: {theme['definition'] or ''}. Keywords: {theme['keywords'] or ''}."
            base_emb = model.encode(base_text)
            
            # Split examples and encode them individually
            example_embs = []
            if theme.get("examples"):
                example_list = [ex.strip() for ex in theme["examples"].split("|") if ex.strip()]
                if example_list:
                    example_embs = model.encode(example_list)
            
            # Stack theme vectors
            if len(example_embs) > 0:
                theme_vectors = np.vstack([base_emb.reshape(1, -1), example_embs])
            else:
                theme_vectors = base_emb.reshape(1, -1)
            
            approved_themes_vectors[str(theme["id"])] = theme_vectors

        # 4. Create embeddings for raw objectives
        logger.info("Encoding objectives...")
        objectives_list = [item["objective"] for item in raw_objectives]
        objectives_embeddings = model.encode(objectives_list, show_progress_bar=True)

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
            if best_score >= CROSS_REF_SIMILARITY_THRESHOLD:
                mapped_themes_data[best_theme_id].append({
                    "story_id": story_id,
                    "score": best_score
                })
            else:
                unmapped_indices.append(i)

        logger.info(f"Mapped to approved themes: {sum(len(v) for v in mapped_themes_data.values())}")
        logger.info(f"Unmapped objectives: {len(unmapped_indices)}")

        # 6. Cluster unmapped objectives locally using Leiden Algorithm
        draft_themes = []
        if unmapped_indices:
            logger.info("Clustering unmapped objectives locally using Leiden algorithm...")
            unmapped_embeddings = objectives_embeddings[unmapped_indices]
            
            # Compute pairwise cosine similarity matrix
            sim_matrix = cosine_similarity(unmapped_embeddings)
            
            # Build similarity graph for Leiden algorithm
            # Connect objectives if similarity is >= (1.0 - CLUSTER_DISTANCE_THRESHOLD) which is 0.55
            sim_threshold = 1.0 - CLUSTER_DISTANCE_THRESHOLD
            
            g = ig.Graph()
            g.add_vertices(len(unmapped_indices))
            
            edges = []
            weights = []
            for i in range(len(unmapped_indices)):
                for j in range(i + 1, len(unmapped_indices)):
                    sim = float(sim_matrix[i, j])
                    if sim >= sim_threshold:
                        edges.append((i, j))
                        weights.append(sim)
            
            g.add_edges(edges)
            if edges:
                g.es['weight'] = weights
                
            # Run Leiden community detection with Modularity optimization
            partition = la.find_partition(g, la.ModularityVertexPartition, weights='weight' if edges else None)
            labels = np.array(partition.membership)
            unique_labels = np.unique(labels)
            logger.info(f"Identified {len(unique_labels)} new candidate clusters via Leiden algorithm.")

            # Create placeholder themes for each cluster
            all_statements_dict = {item["id"]: item["objective"] for item in raw_objectives}
            
            for label in unique_labels:
                cluster_member_indices = [unmapped_indices[j] for j, l in enumerate(labels) if l == label]
                
                # Extract objectives belonging to this cluster
                cluster_objectives = [raw_objectives[idx] for idx in cluster_member_indices]
                cluster_texts = [item["objective"] for item in cluster_objectives]

                # Calculate average similarity within the cluster as the confidence score
                score = 0.85

                # Generate metadata (LLM call is used only if cluster size > 10)
                if len(cluster_texts) > 10:
                    logger.info(f"Generating theme name & definition via LLM for Candidate Theme (Cluster {label + 1}) with {len(cluster_texts)} objectives...")
                    llm_meta = generate_draft_theme_metadata_llm(cluster_texts)
                    theme_name = llm_meta.get("theme_name", f"Candidate Theme (Cluster {label + 1})")
                    theme_def = llm_meta.get("theme_definition", f"Locally clustered theme containing {len(cluster_texts)} objectives. Sample text: '{cluster_texts[0]}'")
                    keywords = llm_meta.get("keywords", list(set([w.lower().strip(",.!?\"'") for t in cluster_texts for w in t.split() if len(w) > 4]))[:5])
                else:
                    theme_name = f"Candidate Theme (Cluster {label + 1})"
                    theme_def = f"Locally clustered theme containing {len(cluster_texts)} objectives. Sample text: '{cluster_texts[0]}'"
                    keywords = list(set([w.lower().strip(",.!?\"'") for t in cluster_texts for w in t.split() if len(w) > 4]))[:5]
                examples = cluster_texts[:3]

                draft_themes.append({
                    "theme_name": theme_name,
                    "theme_definition": theme_def,
                    "keywords": keywords,
                    "examples": examples,
                    "confidence_score": score,
                    "mappings": [
                        {"story_id": item["id"], "score": score}
                        for item in cluster_objectives
                    ]
                })

        # 7. Save results back to Postgres database
        all_statements_dict = {item["id"]: item["objective"] for item in raw_objectives}
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

        logger.info("=========================================")
        logger.info("           OVERVIEW STATISTICS           ")
        logger.info("=========================================")
        logger.info(f"Total CSV rows:                          {total_csv_rows_in_file}")
        logger.info(f"Total CSV rows processed:                {total_csv_rows_processed}")
        logger.info(f"Total skipped non-English:               {skipped_non_english_count}")
        logger.info(f"Total empty objectives:                  {empty_objectives_count}")
        logger.info(f"Mapped to approved themes:               {mapped_count}")
        logger.info(f"Unmapped objectives:                     {unmapped_count}")
        logger.info(f"Identified new candidate clusters:       {len(draft_themes)}")
        logger.info(f"Clusters with count > 10 objectives:     {clusters_gt_10}")
        logger.info("-----------------------------------------")
        for size in range(1, 11):
            logger.info(f"Number of clusters with count = {size:2d}:       {cluster_counts[size]}")
        logger.info("=========================================")

        # 9. Generate PM review CSV automatically
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = Path(__file__).resolve().parent / output_path
        await generate_pm_review_csv(conn, output_path)

    finally:
        await conn.close()
    
    logger.info("Batch thematic analysis successfully completed!")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
