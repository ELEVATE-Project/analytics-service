# Analytics Service

An orchestration and ingestion service built with FastAPI, Kafka, and Temporal. It manages dynamic data ingestion pipelines, rule-based content moderation, local NLP vector similarity matching, and fallback LLM classification.

---

###  Batch Thematic Analysis 

This is what `run_thematic_batch_tritopic.py` does. It takes a **large batch** of statements (either from your CSV file or from the "Others" pile in the database) and discovers new emerging themes from them.

```
  Input: Your CSV file  OR  "Others" from the database
          │
          ▼
  Clean & filter statements
  (English-only, ≥ 3 words, split multi-statement rows)
          │
          ▼
  Re-match against approved themes
  at YOUR chosen threshold (e.g. 0.70)
          │
     ┌────┴──────────────────────────┐
     │ Matched ≥ threshold           │ Not matched
     │ → Mapped to approved theme    │ → Goes to clustering
     └───────────────────────────────┘
                                     │
                                     ▼
                          TriTopic clusters the unmatched
                          (BAAI embeddings + Hybrid Graph)
                                     │
                          Clusters with > 10 objectives
                                     │
                                     ▼
                          LLM names the new cluster
                          → Saved as "Draft" theme in DB
                                     │
                                     ▼
                    📄 PM Review CSV  +  📊 HTML Visualisations
```

---

## 📋 Table of Contents

1. [What You Need (Prerequisites)](#1-what-you-need-prerequisites)
2. [One-Time Setup](#2-one-time-setup)
3. [How to Run — All Commands](#3-how-to-run--all-commands)
4. [Understanding the Threshold](#4-understanding-the-threshold)
5. [Where to Find the Output](#5-where-to-find-the-output)
6. [Viewing Results in the Dashboard](#6-viewing-results-in-the-dashboard)
7. [Understanding the Output CSV](#7-understanding-the-output-csv)
8. [How the Matching Works (Technical)](#8-how-the-matching-works-technical)
9. [Approved Themes — Where They Come From](#9-approved-themes--where-they-come-from)

---

## 1. What You Need (Prerequisites)

| Requirement | Why it's needed | Minimum version |
|---|---|---|
| **Python** | Runs the analysis script | 3.10+ |
| **PostgreSQL** | Stores themes and analysis results | 14+ |
| **OpenRouter API Key** | Names newly discovered themes via LLM | — |
| **Internet connection** | First-time model download only (runs offline after that) | — |

### Python packages

All required packages are in `requirements.txt`:

```
tritopic
sentence-transformers==3.0.1
asyncpg>=0.29.0
python-dotenv>=1.0.1
plotly>=5.15.0
pandas>=2.0.0
streamlit>=1.35.0
scipy
tiktoken
scikit-learn
numpy
```

---

## 2. One-Time Setup

1. Clone the repository and navigate to the project root.
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your environment variables:
   Copy `.env.example` to `.env` and fill in the required configurations (such as database credentials and your OpenRouter API key):
   ```bash
   cp .env.example .env



OR
### Step 1 — Install dependencies

```bash


# Navigate to the thematic_analysis folder
cd thematic_analysis/

# (Recommended) Create and activate a virtual environment
python -m venv myvenv
source myvenv/bin/activate        # On Windows: myvenv\Scripts\activate

# Install all packages
pip install -r requirements.txt
```

### Step 2 — Create your `.env` file

Create a file called `.env` inside the `thematic_analysis/` folder:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/analytics_db
OPENROUTER_API_KEY=sk-or-xxxxxxxxxxxxxxxxxxxxxxxx
```

> Replace the API key with your actual OpenRouter key.  
> Database URL format: `postgresql://<user>:<password>@<host>:<port>/<dbname>`

### Step 3 — Initialize the database

Run this once from the project root to create all tables:

```bash
psql -U postgres -d analytics_db -f schema.sql
```

### Step 4 — Seed approved themes

The script auto-seeds themes on first run if the `themes` table is empty.  
To manually force a fresh seed:

```bash
psql -U postgres -d analytics_db -c "TRUNCATE themes RESTART IDENTITY CASCADE;"
psql -U postgres -d analytics_db -f thematic_analysis/seed_themes.sql
```

---

## 3. How to Run — All Commands

Run all commands from inside the `thematic_analysis/` directory.

```bash
cd thematic_analysis/
```

---

### 🟢 Mode 1 — Process your own CSV file (Primary mode)

Use this when you already have a CSV file of objectives or challenges you want to analyse.

**Both `--input` and `--process_column` are required together.**

```bash
# Analysing a story CSV — process the "objective" column
python run_thematic_batch_tritopic.py \
    --input story.csv \
    --process_column objective \
    --threshold 0.70

# Analysing a discussion CSV — process the "Challenges" column
python run_thematic_batch_tritopic.py \
    --input discussion.csv \
    --process_column Challenges \
    --threshold 0.60

# Save results to a custom file path as well
python run_thematic_batch_tritopic.py \
    --input story.csv \
    --process_column objective \
    --threshold 0.70 \
    --output my_results.csv

# Quick test — limit to first 200 rows
python run_thematic_batch_tritopic.py \
    --input story.csv \
    --process_column objective \
    --threshold 0.70 \
    --limit 200
```

> ❗ **Error cases:**
> - `--input` without `--process_column` → script stops with: `--process_column is required when --input is provided`
> - `--process_column` without `--input` → script stops with: `--input is required when --process_column is provided`

**Your CSV must have:**
- An `id` column (or any column containing "id" in its name)
- The column you name in `--process_column` (exact match, case-insensitive)

Example CSV formats:

| id | objective |
|---|---|
| 101 | Improve quality of education in rural areas |
| 102 | 1. Better teachers \| 2. Modern classrooms \| 3. Digital tools |

| id | Challenges |
|---|---|
| 201 | Lack of infrastructure \| Poor connectivity |
| 202 | Teachers are not trained in digital methods |

> Multi-statement rows (pipe-separated `|` or numbered `1. ... 2. ...`) are automatically split into individual statements.

---

### 🟠 Mode 2 — Database mode (no CSV needed)

Use this when you want to process the statements that the **real-time tagging service** (Flow 1 above) could not map — i.e. those tagged as `"Others"`.

No `--input` flag is needed. The script fetches directly from the `analysis_results` table.

```bash
# Fetch all "Others" from the database and run thematic analysis
python run_thematic_batch_tritopic.py --threshold 0.70

# Same but also save an extra copy of the results to a custom path
python run_thematic_batch_tritopic.py --threshold 0.70 --output themes_from_db.csv

# Quick test — limit to first 200 rows from DB
python run_thematic_batch_tritopic.py --threshold 0.70 --limit 200
```

> The script uses this database filter automatically:
> ```sql
> SELECT id, statements, statement_type
> FROM analysis_results
> WHERE analysis_type = 'theme'
>   AND statement_type IN ('challenges', 'solutions', 'objective')
>   AND category_type = 'Others'
> ```

---

### All available flags at a glance

| Flag | Example | Required? | Default | What it does |
|---|---|---|---|---|
| `--input` | `--input story.csv` | ⚠️ pair with `--process_column` | — | Path to your CSV file |
| `--process_column` | `--process_column objective` | ⚠️ pair with `--input` | — | Which column to read text from |
| `--threshold` | `--threshold 0.70` | No | `0.70` | Cosine similarity cutoff for matching |
| `--output` | `--output results.csv` | No | — | Save an extra copy of the CSV here |
| `--limit` | `--limit 200` | No | — | Process only the first N valid statements |


---

## 4. Understanding the Threshold

The `--threshold` controls **how strictly** a statement must match an approved theme to be assigned to it in the batch analysis.

> ⚠️ **Note:** The threshold here is different from the real-time tagging (Flow 1) which uses 90% (0.90) fixed internally. The batch tool threshold is flexible and user-controlled.

| Threshold | Behaviour |
|---|---|
| **0.90** | Very strict — only very obvious matches are mapped. More statements go to new cluster discovery. |
| **0.70** | Balanced (recommended default) — reasonable matches are accepted. |
| **0.60** | Lenient — more statements get mapped to existing approved themes; fewer go to cluster discovery. |

**Rule of thumb:** Start with `0.70`. If you're getting too many new draft clusters, try `0.60`. If theme assignments look incorrect, try `0.75` or `0.80`.

Each run with a different threshold creates its **own separate output folder**:

```
thematic_analysis/
├── 0.60_review_visualizations/   ← results from threshold 0.60 run
├── 0.70_review_visualizations/   ← results from threshold 0.70 run
└── 0.90_review_visualizations/   ← results from threshold 0.90 run
```

---

## 5. Where to Find the Output

After running the script, all output is saved automatically inside a folder named after your threshold:

```
thematic_analysis/
└── 0.70_review_visualizations/           ← created automatically
    │
    ├── tritopic_review.csv               ← 📄 MAIN RESULT — open in Excel/Google Sheets
    │
    ├── run_meta.json                     ← Run statistics (row counts, tokens used)
    │
    ├── document_map.html                 ← 🗺️ 2D scatter of all statements by topic
    ├── topics_keywords.html              ← 🔑 Keyword bar chart per topic
    ├── hierarchy_tree.html               ← 🌳 Dendrogram showing how topics relate
    └── topic_similarity.html             ← 🔥 Heatmap of similarity between topics
```

> 💡 Open any `.html` file directly in your browser for an interactive chart.

If you used `--output my_results.csv`, that file is also written at the path you specified (same content as `tritopic_review.csv`).

---

## 6. Viewing Results in the Dashboard

The Streamlit dashboard gives you an interactive web UI to explore all results without opening any files manually.

### Launch the dashboard

```bash
# From the thematic_analysis/ directory
streamlit run streamlit_dashboard.py
```

Open **http://localhost:8501** in your browser.

### What you'll see

**Sidebar** — Switch between different threshold runs (automatically detected from available folders).

**Run Statistics** — Total rows processed, skipped, mapped, token usage.

**Theme Summary** — Approved vs Draft theme counts.

**Tabs:**

| Tab | What it shows |
|---|---|
| 📁 **Themes Explorer** | Full themes table; click a theme to see all statements mapped to it with similarity scores |
| 🗺️ **2D Document Map** | All statements plotted in 2D semantic space, coloured by cluster |
| 🌳 **Topic Hierarchy** | Dendrogram showing how close or far topics are from each other |
| 🔥 **Centroid Similarity** | Heatmap of cosine similarity between topic centroids |
| ⬇️ **Download Data** | Download the full CSV directly from the browser |

> The dashboard auto-detects all threshold folders. Run the batch script with a new threshold and it appears in the sidebar automatically.

---

## 7. Understanding the Output CSV

The `tritopic_review.csv` file has these columns:

| Column | What it means |
|---|---|
| **Theme Id** | Unique database ID of the theme |
| **Theme Name** | Human-readable theme name |
| **Defination** | Description of what this theme covers |
| **Keywords** | Comma-separated keywords for this theme |
| **Status** | `approved` = pre-existing theme · `Draft` = newly discovered |
| **Objective Count** | Total statements mapped to this theme in this run |
| **Original Statements** | Each mapped statement: `id \| text \| similarity_score` |

### How rows are arranged

Each theme occupies one or more rows:
- **First row**: all theme details + first statement
- **Following rows**: only `Original Statements` column filled (one per statement)

**Row ordering in the CSV:**
1. Approved themes (highest count first)
2. New Draft themes with > 10 statements (highest count first)
3. New Draft themes with ≤ 10 statements
4. All other statuses

> ⚠️ **Draft themes with 10 or fewer statements are excluded from the CSV and the database.** A cluster that small isn't statistically meaningful. They only appear in the terminal log output.

---

## 8. How the Matching Works (Technical)

### Step 1 — Load and clean input

- Reads statements from your CSV (or database)
- Splits multi-statement rows: `"1. Education | 2. Health"` → two separate statements
- Filters out: non-English text, statements under 3 words, empty rows

### Step 2 — Match against approved themes

- Each approved theme is encoded into **multiple embedding vectors**:
  - One vector: theme name + definition + keywords combined
  - Additional vectors: one per example statement in the theme
- Each incoming statement is encoded using `all-MiniLM-L6-v2` (runs **locally**, no API call)
- **Maximum cosine similarity** is calculated across all theme vectors
- Score ≥ `--threshold` → statement is mapped to that approved theme ✅
- Score < `--threshold` → statement goes to TriTopic clustering

### Step 3 — Cluster the unmatched (TriTopic)

- TriTopic uses `BAAI/bge-base-en-v1.5` embeddings + Hybrid Graph + ConsensusLeiden algorithm
- Finds natural groupings across the unmatched statements
- Clusters with **≤ 10 statements are silently discarded**

### Step 4 — Name new clusters via LLM

- Top 15 sample texts from each cluster are sent to OpenRouter
- LLM generates: Theme Name, Definition, Keywords
- Rate limit errors (429) are retried up to 10 times with 60-second waits
- Named clusters saved to `themes` table with `status = 'Draft'`

---

## 9. Approved Themes — Where They Come From

Approved themes live in the `themes` table in PostgreSQL, initially seeded from `seed_themes.sql`.

**At runtime, matching always reads from the database — not from the SQL file.**

Each approved theme has:
- `name` — Theme name
- `definitions` — Semantic description
- `keywords` — Comma-separated keywords (used in matching)
- `examples` — Pipe-separated example sentences (each encoded as a separate vector)
- `status` — Must be `approved` to be used in matching

To update your approved themes:
```bash
psql -U postgres -d analytics_db -c "DELETE FROM themes WHERE status = 'approved';"
psql -U postgres -d analytics_db -f thematic_analysis/seed_themes.sql
```

> 💡 `Objective Count` and `Original Statements` are **never stored in the database**. They are computed in memory from the current run and appear **only in the CSV output**.

---

## ⚡ Quick Reference Card

```bash
# ─── SETUP (once only) ──────────────────────────────────────────────────
pip install -r requirements.txt
# Create .env with DATABASE_URL and OPENROUTER_API_KEY
psql -U postgres -d analytics_db -f schema.sql

# ─── RUN ANALYSIS ───────────────────────────────────────────────────────

# From a CSV (story — "objective" column)
python run_thematic_batch_tritopic.py \
    --input story.csv --process_column objective --threshold 0.70

# From a CSV (discussion — "Challenges" column)
python run_thematic_batch_tritopic.py \
    --input discussion.csv --process_column Challenges --threshold 0.60

# From database (processes "Others" tagged statements)
python run_thematic_batch_tritopic.py --threshold 0.70

# Quick test — only process 200 rows
python run_thematic_batch_tritopic.py \
    --input story.csv --process_column objective --threshold 0.70 --limit 200

# ─── VIEW RESULTS ────────────────────────────────────────────────────────

# Open the CSV in Excel / Google Sheets
0.70_review_visualizations/tritopic_review.csv

# Open interactive HTML charts in your browser
0.70_review_visualizations/document_map.html
0.70_review_visualizations/topics_keywords.html
0.70_review_visualizations/hierarchy_tree.html
0.70_review_visualizations/topic_similarity.html

# Launch the Streamlit dashboard
streamlit run streamlit_dashboard.py
# → http://localhost:8501
```
