import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.io as pio
import streamlit.components.v1 as components
import re

# Set Page Config
st.set_page_config(
    page_title="Thematic Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        flex-wrap: wrap;
    }
    .stTabs [data-baseweb="tab"] {
        height: 46px;
        padding: 8px 16px;
        font-weight: 600;
        font-size: 15px;
        white-space: nowrap;
        border-radius: 8px 8px 0 0;
    }
    .metric-card {
        background-color: #f8fafc;
        border: 1px solid #e2e8f0;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
    }
    .stPlotlyChart {
        width: 100% !important;
    }
    .stPlotlyChart > div > div > div > button {
        opacity: 0.3;
    }
    .stPlotlyChart > div > div > div > button:hover {
        opacity: 1;
    }
    .threshold-badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 13px;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📊 Thematic Analysis Dashboard")
st.markdown("Interactive Explorer for Topic Extraction, Semantic Mapping, and Thematic Clusters.")

# ─── Base directory ──────────────────────────────────────────────────────────
base_dir = Path(__file__).resolve().parent

# Scan base_dir for directories matching: <threshold>_review_visualizations
available_thresholds = []
pattern = re.compile(r"^(\d+\.\d+)_review_visualizations$")
for path in base_dir.iterdir():
    if path.is_dir():
        match = pattern.match(path.name)
        if match:
            available_thresholds.append(match.group(1))

# Sort thresholds descending (e.g. ['0.90', '0.65', '0.60'])
available_thresholds = sorted(available_thresholds, key=float, reverse=True)

# Fallback in case none found
if not available_thresholds:
    available_thresholds = ["0.90", "0.65"]

# ─── Sidebar: Similarity Threshold selector ───────────────────────────────────
st.sidebar.header("Dashboard Configuration")

threshold = st.sidebar.radio(
    "🎚️ Similarity Threshold",
    options=available_thresholds,
    index=0,
    help=(
        "Controls how strictly a statement must match a theme to be mapped."
    ),
)

# Folder names follow the pattern: <threshold>_review_visualizations/
viz_dir = base_dir / f"{threshold}_review_visualizations"
csv_file = viz_dir / "tritopic_review.csv"

# Sidebar info
badge_color = "#16a34a" if threshold == "0.90" else "#d97706"
st.sidebar.markdown(
    f"**Active run:** <span class='threshold-badge' style='background:{badge_color}22; color:{badge_color}; border:1px solid {badge_color}'>"
    f"Threshold {threshold}</span>",
    unsafe_allow_html=True,
)
st.sidebar.markdown(f"""
* **Viz folder**: `{viz_dir.name}/`
* **CSV file**: `{csv_file.name}`
""")

# ─── Missing data guard ───────────────────────────────────────────────────────
if not viz_dir.exists():
    st.error(
        f"Folder `{viz_dir.name}/` not found. "
        f"Run the batch script with threshold **{threshold}** first to generate it."
    )
    st.stop()

if not csv_file.exists():
    st.error(
        f"`{csv_file.name}` not found inside `{viz_dir.name}/`. "
        "Re-run the batch script to regenerate the results."
    )
    st.stop()

# ─── Load CSV (ffill for statement lookup) ────────────────────────────────────
df = pd.read_csv(csv_file)
df["Theme Name"] = df["Theme Name"].ffill()
df["Theme Id"]   = df["Theme Id"].ffill()

# ─── Load run metadata (saved by batch script) ───────────────────────────────
import json as _json

meta_file = viz_dir / "run_meta.json"
meta = {}
if meta_file.exists():
    with open(meta_file) as _f:
        meta = _json.load(_f)

# ─── Metrics ─────────────────────────────────────────────────────────────────
total_themes     = df["Theme Name"].dropna().nunique()
status_counts    = df["Status"].dropna().value_counts().to_dict()
draft_count      = status_counts.get("Draft", 0)    + status_counts.get("draft", 0)
approved_count   = status_counts.get("Approved", 0) + status_counts.get("approved", 0)

# ── Run stats from run_meta.json ──────────────────────────────────────────────
if meta:
    run_ts = meta.get("run_timestamp", "")
    run_label = (run_ts[11:16] + "  " + run_ts[:10]) if run_ts else "—"
    st.caption(f"📅 Last run: **{run_label}** · Threshold: **{meta.get('similarity_threshold', threshold)}**")

    # Calculate total valid objectives with support for backward compatibility
    total_val_obj = meta.get("total_valid_objectives")
    if total_val_obj is None:
        total_val_obj = meta.get("total_mapped")
        if total_val_obj is None and "mapped_to_approved_themes" in meta and "mapped_to_draft_themes" in meta:
            total_val_obj = meta["mapped_to_approved_themes"] + meta["mapped_to_draft_themes"]
    if total_val_obj is None:
        total_val_obj = "—"

    st.markdown("#### 📋 Run Statistics")
    r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns(5)
    r1c1.metric("Total CSV Rows",              meta.get("total_objectives_in_csv",     "—"))
    r1c2.metric("Total Rows Processed",        meta.get("total_objectives_processed",  "—"))
    r1c3.metric("Total Valid Objectives",      total_val_obj)
    r1c4.metric("Skipped (Non-English)",       meta.get("skipped_non_english",         "—"))
    r1c5.metric("Skipped (Empty)",             meta.get("skipped_empty",               "—"))

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    r2c1.metric("Mapped to Approved Themes",   meta.get("mapped_to_approved_themes",   "—"))
    r2c2.metric("Unmapped Objectives",         meta.get("unmapped_after_approved",     "—"))
    r2c3.metric("New Candidate Clusters",      meta.get("draft_clusters_identified",   "—"))
    r2c4.metric("Clusters > 10 Objectives",    meta.get("clusters_gt_10",              "—"))
else:
    st.caption(
        "ℹ️ _Run metadata not available — re-run the batch script to populate these stats._"
    )

# ── Theme-level summary (always from CSV) ─────────────────────────────────────
st.markdown("#### 🏷️ Theme Summary")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Similarity Threshold",    threshold)
col2.metric("Total Themes",            total_themes)
col3.metric("Approved Themes",         approved_count)
col4.metric("Draft Candidate Themes",  draft_count)



st.markdown("---")

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_explorer, tab_map, tab_hierarchy, tab_similarity, tab_download = st.tabs([
    "📁 Themes Explorer",
    "🗺️ 2D Document Map",
    "🌳 Topic Hierarchy",
    "🔥 Centroid Similarity",
    "⬇️ Download Data",
])

# ── Tab 1: Themes Explorer ────────────────────────────────────────────────────
with tab_explorer:
    st.subheader("Extracted Themes & Mapped Statements")
    st.write("Browse the themes table below. Select a theme in the dropdown to view its mapped statements and similarity scores.")

    # One row per theme (deduplicate on Theme Id)
    display_cols   = ["Theme Id", "Theme Name", "Defination", "Keywords", "Status", "Objective Count"]
    available_cols = [c for c in display_cols if c in df.columns]
    themes_df = (
        df[available_cols]
        .drop_duplicates(subset=["Theme Id"])
        .dropna(subset=["Theme Name"])
        .reset_index(drop=True)
    )
    st.dataframe(themes_df, use_container_width=True)

    # Statement inspector
    theme_names    = themes_df["Theme Name"].tolist()
    selected_theme = st.selectbox("Select a theme to inspect mapped statements:", theme_names)

    if selected_theme:
        st.markdown(f"### Mapped Statements for: **{selected_theme}**")

        theme_details = df[df["Theme Name"] == selected_theme]
        statements    = theme_details["Original Statements"].dropna().tolist()

        if not statements:
            st.info("No statements mapped to this theme.")
        else:
            rows = []
            for stmt in statements:
                parts = str(stmt).split(" | ")
                if len(parts) >= 2:
                    stmt_id = parts[0].strip()
                    for i in range(1, len(parts), 2):
                        text_part = parts[i].strip()
                        score_part = parts[i+1].strip() if i+1 < len(parts) else None
                        
                        try:
                            score_val = float(score_part) if score_part else None
                        except ValueError:
                            score_val = None
                            
                        rows.append({
                            "Statement ID": stmt_id if i == 1 else "",
                            "Statement": text_part,
                            "Similarity Score": score_val
                        })
                else:
                    rows.append({"Statement ID": "", "Statement": stmt.strip(), "Similarity Score": None})

            stmts_df = pd.DataFrame(rows)
            st.caption(f"Showing **{len(stmts_df)}** mapped statements")
            st.dataframe(
                stmts_df,
                use_container_width=True,
                column_config={
                    "Statement ID":    st.column_config.TextColumn("ID", width="small"),
                    "Statement":       st.column_config.TextColumn("Statement", width="large"),
                    "Similarity Score": st.column_config.NumberColumn("Similarity Score", format="%.4f", width="small"),
                },
                hide_index=True,
            )

def render_plotly_chart(json_filename: str, html_filename: str, chart_height: int = 700):
    """
    Render a Plotly chart. Prefers pre-rendered HTML (via iframe) to prevent
    Plotly version deserialization errors and browser freezes, falling back to JSON.
    """
    json_file = viz_dir / json_filename
    html_file = viz_dir / html_filename

    if html_file.exists():
        with open(html_file, "r", encoding="utf-8") as f:
            html_markup = f.read()
        components.html(html_markup, height=chart_height, scrolling=True)
    elif json_file.exists():
        fig = pio.read_json(str(json_file))
        st.plotly_chart(fig, use_container_width=True, height=chart_height)
    else:
        st.warning(
            f"Visualization not found in `{viz_dir.name}/`. "
            f"Re-run the batch script with threshold **{threshold}** to generate it."
        )

# ── Tab 2: 2D Document Map ────────────────────────────────────────────────────
with tab_map:
    st.subheader("2D Semantic Document Map")
    st.write("Interactive projection of objectives in 2D space based on semantic embeddings. Move/zoom to inspect clustering boundaries.")
    render_plotly_chart("document_map.json", "document_map.html", chart_height=750)

# ── Tab 4: Topic Hierarchy ────────────────────────────────────────────────────
with tab_hierarchy:
    st.subheader("Topic Hierarchy Dendrogram")
    st.write("Hierarchical clustering tree representing the relative cosine distances between theme centroid embeddings.")
    render_plotly_chart("hierarchy_tree.json", "hierarchy_tree.html", chart_height=850)

# ── Tab 5: Centroid Similarity ────────────────────────────────────────────────
with tab_similarity:
    st.subheader("Centroid Similarity Heatmap")
    st.write("Heatmap matrix comparing cosine similarity overlaps between different topic/theme centroid embeddings.")
    render_plotly_chart("topic_similarity.json", "topic_similarity.html", chart_height=950)

# ── Tab 6: Download Data ──────────────────────────────────────────────────────
with tab_download:
    st.subheader("⬇️ Download Raw Data")
    st.write("Download the full thematic analysis results CSV for offline review or reporting.")

    # Use raw (non-ffilled) CSV for the download
    df_raw = pd.read_csv(csv_file)

    st.markdown(f"#### File: `{csv_file.name}` — Threshold **{threshold}**")
    col_info, col_btn = st.columns([3, 1])
    with col_info:
        st.caption(
            f"Contains **{len(df_raw)}** rows across "
            f"**{df_raw['Theme Name'].dropna().nunique()}** themes. "
            f"Columns: {', '.join(f'`{c}`' for c in df_raw.columns.tolist())}"
        )
    with col_btn:
        st.download_button(
            label="📥 Download CSV",
            data=csv_file.read_bytes(),
            file_name=f"tritopic_review_{threshold}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("---")
    st.markdown("#### Full Data Preview")
    st.dataframe(df_raw, use_container_width=True, hide_index=True)
