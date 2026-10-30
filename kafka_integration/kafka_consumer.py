
import json
from kafka import KafkaConsumer

from storage.metadata_db import initialize_database, insert_media


def start_consumer():

    initialize_database()

    consumer = KafkaConsumer(
        "image-ingestion",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="image-processing-group",
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        )
    )

    print("Kafka Consumer started...")
    print("Waiting for image events...")

    try:
        for message in consumer:

            event = message.value

            print("\nReceived image event:")
            print(event)

            # Store the event in SQLite
            if not all(
                key in event
                for key in [
                    "sample_id",
                    "filename",
                    "source_path",
                    "media_type",
                    "file_size_bytes",
                    "received_at"
                ]
            ):
                print("Invalid event. Skipping.")
                continue

            insert_media(event)

            print("Image metadata saved to SQLite.")

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    start_consumer()
