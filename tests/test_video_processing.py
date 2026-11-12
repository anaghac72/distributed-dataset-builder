import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cv2
import numpy as np

from kafka_integration import kafka_image_worker
from processing.video_processor import process_video_once


class FakeVideoCapture:
    def __init__(self, frame_count):
        self.frame_count = frame_count
        self.current_index = 0
        self.released = False

    def isOpened(self):
        return True

    def get(self, property_id):
        return self.frame_count

    def set(self, property_id, value):
        self.current_index = int(value)
        return True

    def read(self):
        frame = np.full(
            (100, 160, 3),
            self.current_index,
            dtype=np.uint8,
        )
        self.current_index += 1
        return True, frame

    def release(self):
        self.released = True


class VideoProcessingTests(unittest.TestCase):
    def test_worker_accepts_video_events(self):
        event = {
            "sample_id": "video-sample",
            "filename": "sample.mp4",
            "source_path": "/tmp/sample.mp4",
            "media_type": "video",
            "file_size_bytes": 100,
            "received_at": "2026-11-12T00:00:00+00:00",
        }

        self.assertTrue(kafka_image_worker.validate_event(event))

    def test_worker_dispatches_video_and_records_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video_path = root / "sample.mp4"
            video_path.touch()
            task = object()
            event = {
                "sample_id": "video-sample",
                "filename": video_path.name,
                "source_path": str(video_path),
                "media_type": "video",
                "file_size_bytes": 100,
                "received_at": "2026-11-12T00:00:00+00:00",
            }
            result = {
                "status": "SUCCESS",
                "output": str(root / "manifest.json"),
                "frame_count": 3,
            }

            with (
                patch.object(kafka_image_worker, "insert_media", return_value=True),
                patch.object(kafka_image_worker, "get_successful_output", return_value=None),
                patch.object(kafka_image_worker, "update_media_status") as update_status,
                patch.object(
                    kafka_image_worker,
                    "VIDEO_OUTPUT_DIR",
                    root / "processed",
                ),
                patch.object(
                    kafka_image_worker.process_video_task,
                    "remote",
                    return_value=task,
                ) as submit_video,
                patch.object(kafka_image_worker.ray, "get", return_value=result),
            ):
                kafka_image_worker.process_event(event)

            submit_video.assert_called_once_with(
                str(video_path),
                str(root / "processed" / "video-sample"),
            )
            success_call = update_status.call_args_list[-1]
            self.assertEqual(success_call.args[1:3], (
                "SUCCESS",
                result["output"],
            ))
            self.assertEqual(
                success_call.kwargs["sample_id"],
                "video-sample",
            )

    def test_extracts_resized_sampled_frames_and_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video_path = root / "sample.mp4"
            video_path.touch()
            output_dir = root / "frames"
            capture = FakeVideoCapture(frame_count=10)

            with patch(
                "processing.video_processor.cv2.VideoCapture",
                return_value=capture,
            ):
                result = process_video_once(video_path, output_dir)

            self.assertEqual(result["frame_count"], 5)
            self.assertTrue(capture.released)
            self.assertEqual(
                [record["frame_index"] for record in result["frames"]],
                [0, 2, 4, 7, 9],
            )

            manifest = json.loads(
                Path(result["manifest_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["extracted_frame_count"], 5)
            self.assertEqual(manifest["target_size"], [224, 224])

            for record in result["frames"]:
                image = cv2.imread(record["path"])
                self.assertEqual(image.shape[:2], (224, 224))

    def test_rejects_non_positive_frame_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            video_path = Path(directory) / "sample.mp4"
            video_path.touch()

            with self.assertRaises(ValueError):
                process_video_once(
                    video_path,
                    Path(directory) / "frames",
                    max_frames=0,
                )


if __name__ == "__main__":
    unittest.main()
