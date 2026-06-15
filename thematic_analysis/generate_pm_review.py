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
import asyncpg

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("generate_pm_review")


async def main_async():
    parser = argparse.ArgumentParser(description="Generate CSV for Program Manager review.")
    parser.add_argument(
        "--output",
        default="theme_pm_review.csv",
        help="Path to the output CSV file."
    )
    
    # Retrieve default DB URL from environment or fallback
    default_db = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/analytics_db")
    # Pydantic Settings/BaseSettings class validates and replaces 'postgresql+asyncpg://' with 'postgresql://'
    if default_db.startswith("postgresql+asyncpg://"):
        default_db = default_db.replace("postgresql+asyncpg://", "postgresql://")
        
    parser.add_argument(
        "--db",
        default=default_db,
        help="Database connection URL DSN."
    )
    args = parser.parse_args()

    output_path = Path(args.output)

    logger.info("Connecting to database to fetch themes...")
    try:
        conn = await asyncpg.connect(dsn=args.db)
    except Exception as e:
        logger.error(f"Failed to connect to database at {args.db}: {e}")
        sys.exit(1)

    try:
        # Fetch all themes from database
        rows = await conn.fetch(
            """
            SELECT id, name, definitions, keywords, status, total_objective_count, original_statement_text
            FROM themes
            """
        )
        
        themes = [dict(row) for row in rows]
        logger.info(f"Fetched {len(themes)} themes from database.")

        # Categorize and prioritize themes:
        # 1. Approved themes
        # 2. Draft themes with total_objective_count > 10
        # 3. Draft themes with total_objective_count <= 10
        # 4. Others
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

        # Sort within groups by count descending for clean organization
        approved_themes.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
        draft_gt_10.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
        draft_lte_10.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)
        others.sort(key=lambda x: x["total_objective_count"] or 0, reverse=True)

        ordered_themes = approved_themes + draft_gt_10 + draft_lte_10 + others
        logger.info(
            f"Ordering breakdown:\n"
            f"  - Approved themes: {len(approved_themes)}\n"
            f"  - Draft themes (> 10): {len(draft_gt_10)}\n"
            f"  - Draft themes (<= 10): {len(draft_lte_10)}\n"
            f"  - Other statuses: {len(others)}"
        )

        # Write CSV
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
                
                # Parse statements
                statements = []
                statement_items = theme["original_statement_text"] or []
                for item in statement_items:
                    try:
                        data = json.loads(item)
                        s_id = data.get("id")
                        s_text = data.get("text", "")
                        s_score = data.get("score")
                        
                        # Format score to a readable decimal format
                        if isinstance(s_score, (int, float)):
                            s_score_str = f"{s_score:.4f}".rstrip("0").rstrip(".") if s_score != 0 else "0"
                        else:
                            s_score_str = str(s_score)
                            
                        # Format: "ID | Text | Score"
                        formatted = f"{s_id} | {s_text} | {s_score_str}"
                        statements.append(formatted)
                    except Exception:
                        statements.append(str(item))

                if not statements:
                    # Write row with empty statements column
                    writer.writerow([theme_id, theme_name, theme_def, theme_kw, theme_status, theme_count, ""])
                else:
                    # Write first statement with full theme info
                    writer.writerow([theme_id, theme_name, theme_def, theme_kw, theme_status, theme_count, statements[0]])
                    # Write subsequent statements with empty theme info for clean visualization
                    for stmt in statements[1:]:
                        writer.writerow(["", "", "", "", "", "", stmt])

        logger.info(f"Successfully generated PM review CSV file at: {output_path.resolve()}")

    finally:
        await conn.close()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
