import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from kafka_integration import kafka_image_worker
from orchestration import dataset_scheduler
from recovery.dead_letter import (
    build_dead_letter_record,
    deserialize_event,
    publish_dead_letter,
)


class DeadLetterTests(unittest.TestCase):
    def test_deserializer_keeps_malformed_json_for_dead_lettering(self):
        decoded = deserialize_event(b'{"broken":')
        self.assertEqual(decoded, {"_raw_message": '{"broken":'})

    def test_deserializer_parses_valid_json(self):
        self.assertEqual(
            deserialize_event(b'{"sample_id":"sample-1"}'),
            {"sample_id": "sample-1"},
        )

    def test_dead_letter_has_source_and_error_details(self):
        record = build_dead_letter_record(
            event={"sample_id": "sample-1"},
            error=ValueError("bad event"),
            source_topic="image-ingestion",
            source_partition=2,
            source_offset=15,
        )

        self.assertEqual(record["event"]["sample_id"], "sample-1")
        self.assertEqual(record["error"], {
            "type": "ValueError",
            "message": "bad event",
        })
        self.assertEqual(record["source"], {
            "topic": "image-ingestion",
            "partition": 2,
            "offset": 15,
        })

    def test_dead_letter_publish_waits_for_broker_ack(self):
        future = Mock()
        producer = Mock()
        producer.send.return_value = future

        publish_dead_letter(producer, "image-ingestion-dlq", {"event": {}})

        producer.send.assert_called_once_with(
            "image-ingestion-dlq",
            value={"event": {}},
        )
        future.get.assert_called_once_with(timeout=10)

    def test_failed_event_is_dead_lettered_before_source_commit(self):
        consumer = Mock()
        dead_letter_producer = Mock()
        message = SimpleNamespace(
            value={"sample_id": "broken"},
            topic="image-ingestion",
            partition=0,
            offset=7,
        )
        order = []

        consumer.commit.side_effect = lambda: order.append("commit")

        with (
            patch.object(
                kafka_image_worker,
                "process_event",
                side_effect=ValueError("invalid event"),
            ),
            patch.object(
                kafka_image_worker,
                "publish_dead_letter",
                side_effect=lambda *args: order.extend(
                    ["dead-letter", "ack"]
                ),
            ),
        ):
            kafka_image_worker.handle_message(
                consumer,
                dead_letter_producer,
                message,
            )

        self.assertEqual(order, ["dead-letter", "ack", "commit"])

    def test_source_offset_is_not_committed_when_dead_letter_publish_fails(self):
        consumer = Mock()
        message = SimpleNamespace(
            value={"sample_id": "broken"},
            topic="image-ingestion",
            partition=0,
            offset=7,
        )

        with (
            patch.object(
                kafka_image_worker,
                "process_event",
                side_effect=ValueError("invalid event"),
            ),
            patch.object(
                kafka_image_worker,
                "publish_dead_letter",
                side_effect=RuntimeError("broker unavailable"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
                kafka_image_worker.handle_message(
                    consumer,
                    Mock(),
                    message,
                )

        consumer.commit.assert_not_called()


class DatasetSchedulerTests(unittest.TestCase):
    def test_scheduler_rebuilds_once_for_new_success_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "scheduler.json"
            cursor = ("2026-11-12T10:00:00+00:00", 12)

            with (
                patch.object(dataset_scheduler, "initialize_database"),
                patch.object(
                    dataset_scheduler,
                    "get_latest_success_cursor",
                    return_value=cursor,
                ),
                patch.object(
                    dataset_scheduler,
                    "build_dataset",
                    return_value={"total_samples": 4},
                ) as build_dataset,
            ):
                self.assertTrue(
                    dataset_scheduler.run_scheduler_once(state_path)
                )
                self.assertFalse(
                    dataset_scheduler.run_scheduler_once(state_path)
                )

            build_dataset.assert_called_once_with()
            self.assertEqual(dataset_scheduler.load_cursor(state_path), cursor)

    def test_scheduler_does_not_checkpoint_when_build_finds_no_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "scheduler.json"

            with (
                patch.object(dataset_scheduler, "initialize_database"),
                patch.object(
                    dataset_scheduler,
                    "get_latest_success_cursor",
                    return_value=("2026-11-12T10:00:00+00:00", 1),
                ),
                patch.object(
                    dataset_scheduler,
                    "build_dataset",
                    return_value=None,
                ),
            ):
                self.assertFalse(
                    dataset_scheduler.run_scheduler_once(state_path)
                )

            self.assertFalse(state_path.exists())


if __name__ == "__main__":
    unittest.main()
