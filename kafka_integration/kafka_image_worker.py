
import json
from pathlib import Path

import ray
from kafka import KafkaConsumer
from PIL import Image, ImageOps

from recovery.retry_manager import run_with_retries

from storage.metadata_db import (
    initialize_database,
    insert_media,
    update_media_status,
    get_successful_output,
)


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = PROJECT_DIR / "data" / "processed"

KAFKA_TOPIC = "image-ingestion"
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


# --------------------------------------------------
# EVENT VALIDATION
# --------------------------------------------------

def validate_event(event):
    """Check whether a Kafka message contains valid image metadata."""

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

    if event["media_type"] != "image":
        return False

    return True


# --------------------------------------------------
# PROCESS ONE KAFKA EVENT
# --------------------------------------------------

def process_event(event):

    # Step 1: Validate the event.
    if not validate_event(event):

        print("Invalid image event. Skipping.")

        # Invalid messages are treated as poison messages.
        # The consumer will commit their offsets.
        return

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

    output_path = OUTPUT_DIR / f"{input_path.stem}.jpg"

    # Step 3: Insert metadata into SQLite.
    is_new_event = insert_media(event)

    if not is_new_event:
        print("Event already exists in SQLite:", event["sample_id"])

    # Step 4: Check for an existing successful output.
    existing_output = get_successful_output(input_path)

    if existing_output:

        print("Duplicate image detected. Skipping Ray processing.")

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

    print("Submitting image to Ray:", input_path.name)

    # Step 6: Submit the image to a Ray worker.
    result = ray.get(
        process_image_task.remote(
            str(input_path),
            str(output_path)
        )
    )

    print("Processing result:", result)

    # Step 7: Update SQLite based on processing result.
    if result["status"] == "SUCCESS":

        update_media_status(
            input_path,
            "SUCCESS",
            result["output"],
            sample_id=event["sample_id"],
        )

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

    try:

        consumer = KafkaConsumer(
            KAFKA_TOPIC,

            bootstrap_servers=[KAFKA_SERVER],

            auto_offset_reset="earliest",

            enable_auto_commit=False,

            group_id="image-processing-worker",

            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            ),
        )

        print("\nKafka image worker started.")

        print("Topic:", KAFKA_TOPIC)

        print("Waiting for image events...\n")

        for message in consumer:

            event = message.value

            print("\n--------------------------------")
            print("Received Kafka event:")
            print(event)
            print("--------------------------------")

            try:

                process_event(event)

                # Commit only after successful processing
                # or intentional handling of an invalid event.
                consumer.commit()

                print("Kafka offset committed.")

            except Exception as error:

                print("Event processing failed:", error)

                print("Kafka offset NOT committed.")

                # Stop the worker so the failed event can be
                # retried when the worker is restarted.
                raise

    except KeyboardInterrupt:

        print("\nKafka worker stopped by user.")

    except Exception as error:

        print("\nKafka worker encountered an error:", error)

    finally:

        if consumer is not None:
            consumer.close()

        if ray.is_initialized():
            ray.shutdown()

        print("Kafka and Ray connections closed.")


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":
    start_worker()
