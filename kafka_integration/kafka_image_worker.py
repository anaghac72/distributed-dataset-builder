
import json
from pathlib import Path

import ray
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image, ImageOps

from recovery.retry_manager import run_with_retries
from recovery.dead_letter import (
    build_dead_letter_record,
    deserialize_event,
    publish_dead_letter,
)

from storage.metadata_db import (
    initialize_database,
    insert_media,
    update_media_status,
    get_successful_output,
)
from processing.video_processor import process_video_once


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = PROJECT_DIR / "data" / "processed"
VIDEO_OUTPUT_DIR = OUTPUT_DIR / "videos"

KAFKA_TOPIC = "image-ingestion"
DEAD_LETTER_TOPIC = "image-ingestion-dlq"
KAFKA_SERVER = "localhost:9092"

TARGET_SIZE = (224, 224)


# --------------------------------------------------
# IMAGE PREPROCESSING
# --------------------------------------------------

def process_image_once(input_path, output_path):
    """Resize an image to 224x224 and save it as JPEG."""

    input_path = Path(input_path)
    output_path = Path(output_path)

    with Image.open(input_path) as image:

        image = ImageOps.exif_transpose(image)

        image = image.convert("RGB")

        image = ImageOps.fit(image, TARGET_SIZE)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        temporary_path = output_path.with_name(
            output_path.stem + ".tmp.jpg"
        )

        image.save(
            temporary_path,
            format="JPEG",
            quality=90
        )

        temporary_path.replace(output_path)

    return str(output_path)


# --------------------------------------------------
# RAY DISTRIBUTED PROCESSING TASK
# --------------------------------------------------

@ray.remote
def process_image_task(input_path, output_path):
    """Process an image using Ray with retry support."""

    try:

        result = run_with_retries(
            operation_name="image_preprocessing",
            item=input_path,
            operation=lambda: process_image_once(
                input_path,
                output_path
            ),
        )

        return {
            "filename": Path(input_path).name,
            "status": "SUCCESS",
            "output": result,
        }

    except Exception as error:

        return {
            "filename": Path(input_path).name,
            "status": "FAILED",
            "error": str(error),
        }


@ray.remote
def process_video_task(input_path, output_dir):
    """Extract sampled frames from a video with retry support."""
    try:
        result = run_with_retries(
            operation_name="video_frame_extraction",
            item=input_path,
            operation=lambda: process_video_once(
                input_path,
                output_dir,
            ),
        )

        return {
            "filename": Path(input_path).name,
            "status": "SUCCESS",
            "output": result["manifest_path"],
            "frame_count": result["frame_count"],
        }
    except Exception as error:
        return {
            "filename": Path(input_path).name,
            "status": "FAILED",
            "error": str(error),
        }


# --------------------------------------------------
# EVENT VALIDATION
# --------------------------------------------------

def validate_event(event):
    """Check whether a Kafka message contains supported media metadata."""

    required_fields = [
        "sample_id",
        "filename",
        "source_path",
        "media_type",
        "file_size_bytes",
        "received_at",
    ]

    if not isinstance(event, dict):
        return False

    if not all(field in event for field in required_fields):
        return False

    if event["media_type"] not in ("image", "video"):
        return False

    if not isinstance(event["sample_id"], str) or not event["sample_id"]:
        return False

    if not isinstance(event["source_path"], str) or not event["source_path"]:
        return False

    if (
        not isinstance(event["file_size_bytes"], int)
        or event["file_size_bytes"] < 0
    ):
        return False

    return True


# --------------------------------------------------
# PROCESS ONE KAFKA EVENT
# --------------------------------------------------

def process_event(event):

    # Step 1: Validate the event.
    if not validate_event(event):
        raise ValueError("Invalid or unsupported media event.")

    input_path = Path(event["source_path"])

    # Step 2: Check whether the original file exists.
    if not input_path.is_file():

        raise FileNotFoundError(
            f"Image file does not exist: {input_path}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # Step 3: Insert metadata into SQLite.
    is_new_event = insert_media(event)

    if not is_new_event:
        print("Event already exists in SQLite:", event["sample_id"])

    # Step 4: Check for an existing successful output.
    existing_output = get_successful_output(input_path)

    if existing_output:

        print("Media was already processed. Skipping Ray processing.")

        print("Existing output:", existing_output)

        update_media_status(
            input_path,
            "SUCCESS",
            existing_output,
            sample_id=event["sample_id"],
        )

        return

    # Step 5: Update the current event status.
    update_media_status(
        input_path,
        "PROCESSING",
        sample_id=event["sample_id"],
    )

    if event["media_type"] == "image":
        output_path = OUTPUT_DIR / f"{input_path.stem}.jpg"
        print("Submitting image to Ray:", input_path.name)
        task = process_image_task.remote(
            str(input_path),
            str(output_path),
        )
    else:
        output_dir = VIDEO_OUTPUT_DIR / event["sample_id"]
        print("Submitting video to Ray:", input_path.name)
        task = process_video_task.remote(
            str(input_path),
            str(output_dir),
        )

    result = ray.get(task)

    print("Processing result:", result)

    # Step 7: Update SQLite based on processing result.
    if result["status"] == "SUCCESS":

        update_media_status(
            input_path,
            "SUCCESS",
            result["output"],
            sample_id=event["sample_id"],
        )

        if event["media_type"] == "video":
            print(
                "Video processed successfully; extracted frames:",
                result["frame_count"],
            )
        else:
            print("Image processed successfully!")

    else:

        update_media_status(
            input_path,
            "FAILED",
            sample_id=event["sample_id"],
        )

        raise RuntimeError(
            result.get("error", "Image processing failed")
        )


def handle_message(consumer, dead_letter_producer, message):
    event = message.value

    print("\n--------------------------------")
    print("Received Kafka event:")
    print(event)
    print("--------------------------------")

    try:
        process_event(event)
    except Exception as error:
        dead_letter_record = build_dead_letter_record(
            event=event,
            error=error,
            source_topic=message.topic,
            source_partition=message.partition,
            source_offset=message.offset,
        )
        publish_dead_letter(
            dead_letter_producer,
            DEAD_LETTER_TOPIC,
            dead_letter_record,
        )
        print(
            "Failure recorded in dead-letter topic; "
            "committing source offset."
        )
    else:
        print("Media processing succeeded.")

    consumer.commit()
    print("Kafka offset committed.")


# --------------------------------------------------
# KAFKA CONSUMER
# --------------------------------------------------

def start_worker():

    initialize_database()

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("Starting Ray...")

    ray.init(
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
    )

    consumer = None
    dead_letter_producer = None

    try:

        consumer = KafkaConsumer(
            KAFKA_TOPIC,

            bootstrap_servers=[KAFKA_SERVER],

            auto_offset_reset="earliest",

            enable_auto_commit=False,

            group_id="image-processing-worker",

            value_deserializer=deserialize_event,
        )
        dead_letter_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
        )

        print("\nKafka image worker started.")

        print("Topic:", KAFKA_TOPIC)

        print("Waiting for image events...\n")

        for message in consumer:
            handle_message(consumer, dead_letter_producer, message)

    except KeyboardInterrupt:

        print("\nKafka worker stopped by user.")

    except Exception as error:

        print("\nKafka worker encountered an error:", error)

    finally:
        try:
            if consumer is not None:
                consumer.close()
        finally:
            try:
                if dead_letter_producer is not None:
                    dead_letter_producer.flush()
                    dead_letter_producer.close()
            finally:
                if ray.is_initialized():
                    ray.shutdown()
                print("Kafka and Ray connections closed.")


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":
    start_worker()
