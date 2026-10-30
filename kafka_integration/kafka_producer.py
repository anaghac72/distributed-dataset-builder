
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError


class ImageKafkaProducer:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3
        )

        self.topic = "image-ingestion"

    def send_image_event(self, event):
        try:
            future = self.producer.send(
                self.topic,
                value=event
            )

            metadata = future.get(timeout=10)

            print("Kafka message sent successfully!")
            print("Topic:", metadata.topic)
            print("Partition:", metadata.partition)
            print("Offset:", metadata.offset)

            return True

        except KafkaError as error:
            print("Kafka sending failed:", error)
            return False

    def close(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":

    producer = ImageKafkaProducer()

    sample_event = {
        "sample_id": "test-001",
        "filename": "test_image.png",
        "media_type": "image",
        "file_size_bytes": 2048,
        "status": "RECEIVED"
    }

    producer.send_image_event(sample_event)

    producer.close()
