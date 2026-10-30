
import sqlite3
import hashlib

from pathlib import Path
from datetime import datetime, timezone


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_DIR / "data" / "metadata.db"


# --------------------------------------------------
# TIME AND DATABASE CONNECTION
# --------------------------------------------------

def current_time():
    return datetime.now(timezone.utc).isoformat()


def get_connection():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DATABASE_PATH)


# --------------------------------------------------
# DATABASE INITIALIZATION
# --------------------------------------------------

def initialize_database():

    with get_connection() as connection:

        connection.execute("""
            CREATE TABLE IF NOT EXISTS media_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_id TEXT UNIQUE NOT NULL,
                filename TEXT NOT NULL,
                source_path TEXT NOT NULL,
                media_type TEXT NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'RECEIVED',
                output_path TEXT,
                updated_at TEXT,
                kafka_published INTEGER NOT NULL DEFAULT 0,
                content_hash TEXT
            )
        """)

        existing_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(media_files)"
            ).fetchall()
        }

        if "output_path" not in existing_columns:
            connection.execute("""
                ALTER TABLE media_files
                ADD COLUMN output_path TEXT
            """)

        if "updated_at" not in existing_columns:
            connection.execute("""
                ALTER TABLE media_files
                ADD COLUMN updated_at TEXT
            """)

        if "kafka_published" not in existing_columns:
            connection.execute("""
                ALTER TABLE media_files
                ADD COLUMN kafka_published INTEGER NOT NULL DEFAULT 0
            """)

        if "content_hash" not in existing_columns:
            connection.execute("""
                ALTER TABLE media_files
                ADD COLUMN content_hash TEXT
            """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_content_hash
            ON media_files(content_hash)
        """)

    print(f"Database initialized: {DATABASE_PATH}")


# --------------------------------------------------
# SHA-256 CONTENT HASHING
# --------------------------------------------------

def calculate_file_hash(file_path):
    """Calculate SHA-256 hash of a file."""

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


def is_hash_already_published(content_hash):

    with get_connection() as connection:

        row = connection.execute("""
            SELECT 1
            FROM media_files
            WHERE content_hash = ?
              AND kafka_published = 1
            LIMIT 1
        """, (
            content_hash,
        )).fetchone()

    return row is not None


def save_content_hash(sample_id, content_hash):

    with get_connection() as connection:

        connection.execute("""
            UPDATE media_files
            SET content_hash = ?,
                updated_at = ?
            WHERE sample_id = ?
        """, (
            content_hash,
            current_time(),
            sample_id
        ))


# --------------------------------------------------
# INSERT MEDIA METADATA
# --------------------------------------------------

def insert_media(event):

    with get_connection() as connection:

        cursor = connection.execute("""
            INSERT OR IGNORE INTO media_files (
                sample_id,
                filename,
                source_path,
                media_type,
                file_size_bytes,
                received_at,
                status,
                updated_at,
                kafka_published,
                content_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["sample_id"],
            event["filename"],
            event["source_path"],
            event["media_type"],
            event["file_size_bytes"],
            event["received_at"],
            "RECEIVED",
            current_time(),
            0,
            event.get("content_hash")
        ))

        return cursor.rowcount == 1


# --------------------------------------------------
# KAFKA PUBLISHING STATUS
# --------------------------------------------------

def mark_kafka_published(sample_id):

    with get_connection() as connection:

        connection.execute("""
            UPDATE media_files
            SET kafka_published = 1,
                updated_at = ?
            WHERE sample_id = ?
        """, (
            current_time(),
            sample_id
        ))


def is_kafka_published(source_path, file_size_bytes):

    with get_connection() as connection:

        row = connection.execute("""
            SELECT 1
            FROM media_files
            WHERE source_path = ?
              AND file_size_bytes = ?
              AND kafka_published = 1
            LIMIT 1
        """, (
            str(source_path),
            file_size_bytes
        )).fetchone()

    return row is not None


# --------------------------------------------------
# SUCCESSFUL OUTPUT CHECK
# --------------------------------------------------

def get_successful_output(source_path):

    with get_connection() as connection:

        row = connection.execute("""
            SELECT output_path
            FROM media_files
            WHERE source_path = ?
              AND status = 'SUCCESS'
              AND output_path IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 1
        """, (
            str(source_path),
        )).fetchone()

    if row and row[0] and Path(row[0]).is_file():
        return row[0]

    return None


# --------------------------------------------------
# UPDATE MEDIA STATUS
# --------------------------------------------------

def update_media_status(
    source_path,
    status,
    output_path=None,
    sample_id=None
):

    allowed_statuses = {
        "RECEIVED",
        "PROCESSING",
        "SUCCESS",
        "FAILED"
    }

    if status not in allowed_statuses:
        raise ValueError(f"Invalid status: {status}")

    with get_connection() as connection:

        if sample_id:

            cursor = connection.execute("""
                UPDATE media_files
                SET status = ?,
                    output_path = ?,
                    updated_at = ?
                WHERE sample_id = ?
            """, (
                status,
                output_path,
                current_time(),
                sample_id
            ))

        else:

            cursor = connection.execute("""
                UPDATE media_files
                SET status = ?,
                    output_path = ?,
                    updated_at = ?
                WHERE source_path = ?
            """, (
                status,
                output_path,
                current_time(),
                str(source_path)
            ))

        if cursor.rowcount == 0:
            print(f"Warning: No database record found for {source_path}")


# --------------------------------------------------
# FETCH RECORDS
# --------------------------------------------------

def get_all_media():

    with get_connection() as connection:

        rows = connection.execute("""
            SELECT sample_id,
                   filename,
                   media_type,
                   file_size_bytes,
                   received_at,
                   status,
                   output_path,
                   updated_at
            FROM media_files
            ORDER BY id DESC
        """).fetchall()

    return rows


def get_media_by_status(status):

    with get_connection() as connection:

        rows = connection.execute("""
            SELECT sample_id,
                   filename,
                   source_path,
                   status,
                   output_path,
                   updated_at
            FROM media_files
            WHERE status = ?
            ORDER BY id DESC
        """, (
            status,
        )).fetchall()

    return rows


def get_pending_media():

    with get_connection() as connection:

        rows = connection.execute("""
            SELECT sample_id,
                   filename,
                   source_path,
                   status,
                   output_path,
                   updated_at
            FROM media_files
            WHERE status IN ('RECEIVED', 'PROCESSING', 'FAILED')
              AND media_type = 'image'
            ORDER BY id ASC
        """).fetchall()

    return rows


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    initialize_database()

    records = get_all_media()

    print("Saved media records:", len(records))

    for record in records[:5]:
        print(record)
