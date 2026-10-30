
import json
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from storage.metadata_db import (
    initialize_database,
    insert_media,
    mark_kafka_published,
    calculate_file_hash,
    is_hash_already_published,
    save_content_hash,
)

from kafka_integration.kafka_producer import ImageKafkaProducer


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parents[1]

INCOMING_DIR = PROJECT_DIR / "data" / "incoming"

SUPPORTED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".webp",
    ".mp4", ".avi", ".mov", ".mkv",
}

POLL_INTERVAL = 2


# --------------------------------------------------
# CREATE EVENT
# --------------------------------------------------

def create_event(file_path, content_hash):

    extension = file_path.suffix.lower()

    media_type = (
        "video"
        if extension in {".mp4", ".avi", ".mov", ".mkv"}
        else "image"
    )

    return {
        "sample_id": str(uuid.uuid4()),
        "filename": file_path.name,
        "source_path": str(file_path.resolve()),
        "media_type": media_type,
        "file_size_bytes": file_path.stat().st_size,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "content_hash": content_hash,
    }


# --------------------------------------------------
# FILE STABILITY CHECK
# --------------------------------------------------

def is_file_ready(file_path):

    try:

        first_stat = file_path.stat()

        time.sleep(0.2)

        second_stat = file_path.stat()

        return (
            first_stat.st_size == second_stat.st_size
            and first_stat.st_mtime_ns == second_stat.st_mtime_ns
        )

    except OSError:

        return False


# --------------------------------------------------
# CONTINUOUS KAFKA INGESTION
# --------------------------------------------------

def run_producer():

    INCOMING_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    initialize_database()

    kafka_producer = None

    pending_events = {}

    # Avoid recalculating the same file hash repeatedly
    # during one producer session.
    hash_cache = {}

    print("=" * 60)
    print("DISTRIBUTED DATASET BUILDER")
    print("CONTINUOUS INGESTION WITH SHA-256 DEDUPLICATION")
    print("=" * 60)

    print("Watching:", INCOMING_DIR)
    print("Kafka topic: image-ingestion")
    print("Press Ctrl+C to stop.")

    try:

        kafka_producer = ImageKafkaProducer()

        while True:

            for file_path in sorted(INCOMING_DIR.iterdir()):

                if not file_path.is_file():
                    continue

                if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                    continue

                if not is_file_ready(file_path):
                    continue

                try:

                    file_stat = file_path.stat()

                    source_path = str(file_path.resolve())

                    signature = (
                        source_path,
                        file_stat.st_size,
                        file_stat.st_mtime_ns
                    )

                    # Reuse a hash when the file version is unchanged.
                    if signature not in hash_cache:

                        content_hash = calculate_file_hash(file_path)

                        hash_cache[signature] = content_hash

                    else:

                        content_hash = hash_cache[signature]

                    # Check whether identical content was already published.
                    if is_hash_already_published(content_hash):

                        print(
                            f"Duplicate content detected: {file_path.name}"
                        )

                        continue

                    # Reuse event after a failed Kafka publish.
                    if signature not in pending_events:

                        event = create_event(
                            file_path,
                            content_hash
                        )

                        insert_media(event)

                        save_content_hash(
                            event["sample_id"],
                            content_hash
                        )

                        pending_events[signature] = event

                    event = pending_events[signature]

                    print("\nNew media detected:")
                    print(json.dumps(event, indent=4))

                    # Send event to Kafka.
                    published = kafka_producer.send_image_event(event)

                    if published:

                        mark_kafka_published(event["sample_id"])

                        pending_events.pop(signature, None)

                        print("Kafka publication confirmed.")

                        print("Filename:", event["filename"])

                        print("SHA-256:", content_hash)

                    else:

                        print("Kafka publication failed.")
                        print("Will retry during the next scan.")

                except OSError as error:

                    print(
                        f"File access error: {file_path.name}: {error}"
                    )

                except Exception as error:

                    print(
                        f"Ingestion error for {file_path.name}: {error}"
                    )

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:

        print("\nIngestion producer stopped.")

    finally:

        if kafka_producer is not None:
            kafka_producer.close()

        print("Kafka producer connection closed.")


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":
    run_producer()
