
import json
import sqlite3
import tarfile
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"
DATABASE_PATH = DATA_DIR / "metadata.db"
OUTPUT_DIR = DATA_DIR / "dataset_output"
MANIFEST_PATH = OUTPUT_DIR / "manifest.json"

st.set_page_config(
    page_title="Distributed Dataset Builder",
    page_icon="🗂️",
    layout="wide",
)

st.title("🗂️ Distributed Dataset Builder")
st.caption("Image ingestion, processing, deduplication and dataset sharding")

# Count files in each folder.
def count_files(folder, extensions=None):
    if not folder.exists():
        return 0

    files = [p for p in folder.iterdir() if p.is_file()]

    if extensions:
        files = [
            p for p in files
            if p.suffix.lower() in extensions
        ]

    return len(files)


image_extensions = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
video_extensions = {".mp4", ".avi", ".mov", ".mkv"}

incoming_count = count_files(
    INCOMING_DIR, image_extensions | video_extensions
)
processed_count = count_files(PROCESSED_DIR, {".jpg", ".jpeg", ".png"})
incoming_images = count_files(INCOMING_DIR, image_extensions)
incoming_videos = count_files(INCOMING_DIR, video_extensions)

# Load database records, if available.
records = []
if DATABASE_PATH.exists():
    try:
        with sqlite3.connect(DATABASE_PATH) as connection:
            records = pd.read_sql_query(
                "SELECT * FROM media_files ORDER BY id DESC",
                connection,
            ).to_dict(orient="records")
    except Exception as error:
        st.warning(f"Could not read metadata database: {error}")

# Load dataset manifest, if available.
manifest = None
if MANIFEST_PATH.exists():
    try:
        manifest = json.loads(
            MANIFEST_PATH.read_text(encoding="utf-8")
        )
    except Exception as error:
        st.warning(f"Could not read manifest: {error}")

st.subheader("Dataset overview")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Incoming media", incoming_count)
col2.metric("Incoming images", incoming_images)
col3.metric("Incoming videos", incoming_videos)
col4.metric("Processed images", processed_count)

st.divider()

st.subheader("Metadata database")
col1, col2 = st.columns(2)
col1.metric("Recorded media", len(records))
col2.metric(
    "Dataset total",
    manifest.get("total_images", 0) if manifest else 0,
)

if records:
    st.dataframe(pd.DataFrame(records), use_container_width=True)
else:
    st.info(
        "No metadata records found yet. Start the ingestion producer "
        "to record incoming media."
    )

st.divider()

st.subheader("Dataset splits and shards")

if manifest:
    split_data = []

    for split_name, details in manifest.get("splits", {}).items():
        split_data.append({
            "Split": split_name,
            "Images": details.get("image_count", 0),
            "Shards": details.get("shard_count", 0),
        })

    if split_data:
        split_df = pd.DataFrame(split_data)
        st.dataframe(split_df, use_container_width=True)

        st.bar_chart(
            split_df.set_index("Split")[["Images"]]
        )
else:
    st.info(
        "No dataset manifest found. Run "
        "`python -m processing.dataset_sharder` first."
    )

if OUTPUT_DIR.exists():
    shard_files = sorted(OUTPUT_DIR.rglob("*.tar"))
    st.metric("Generated TAR shards", len(shard_files))

    if shard_files:
        with st.expander("View shard files"):
            for shard in shard_files:
                st.write(str(shard.relative_to(PROJECT_DIR)))

st.divider()

st.subheader("Recent incoming images")
if INCOMING_DIR.exists():
    image_files = sorted(
        [
            p for p in INCOMING_DIR.iterdir()
            if p.is_file() and p.suffix.lower() in image_extensions
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:12]

    if image_files:
        columns = st.columns(4)

        for index, image_path in enumerate(image_files):
            with columns[index % 4]:
                st.image(
                    str(image_path),
                    caption=image_path.name,
                    use_container_width=True,
                )
    else:
        st.info("No incoming images found.")
else:
    st.info("The incoming folder has not been created yet.")

if st.button("Refresh dashboard"):
    st.rerun()

st.caption(
    "Prototype dashboard: validation, deduplication and processing "
    "metrics must be connected to display their actual results."
)
