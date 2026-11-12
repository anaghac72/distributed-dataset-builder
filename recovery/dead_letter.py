import json
from datetime import datetime, timezone


def deserialize_event(message):
    """Keep malformed JSON available for dead-lettering instead of crashing."""
    decoded = message.decode("utf-8", errors="replace")
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        return {"_raw_message": decoded}


def build_dead_letter_record(
    event,
    error,
    source_topic,
    source_partition,
    source_offset,
):
    return {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "topic": source_topic,
            "partition": source_partition,
            "offset": source_offset,
        },
        "event": event,
        "error": {
            "type": type(error).__name__,
            "message": str(error),
        },
    }


def publish_dead_letter(producer, topic, record):
    """Wait for broker acknowledgement before allowing source offset commit."""
    future = producer.send(topic, value=record)
    return future.get(timeout=10)
