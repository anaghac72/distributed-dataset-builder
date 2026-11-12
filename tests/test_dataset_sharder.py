import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from processing.dataset_sharder import build_dataset, split_dataset


class DatasetSharderTests(unittest.TestCase):
    def test_image_only_split_preserves_existing_ratios(self):
        samples = [
            {
                "path": f"image-{index}.jpg",
                "source_type": "image",
                "source_file": f"image-{index}.jpg",
            }
            for index in range(10)
        ]

        splits = split_dataset(samples)

        self.assertEqual(
            [len(splits[name]) for name in ("train", "val", "test")],
            [7, 1, 2],
        )

    def test_video_frames_are_sharded_with_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_dir = root / "processed"
            video_dir = input_dir / "videos" / "video-sample-1"
            video_dir.mkdir(parents=True)

            image_path = input_dir / "processed-image.jpg"
            input_dir.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (20, 20), "red").save(image_path)

            frame_records = []
            for frame_index in range(2):
                frame_path = video_dir / f"frame_{frame_index + 1:04d}.jpg"
                Image.new("RGB", (224, 224), (0, frame_index, 255)).save(
                    frame_path
                )
                frame_records.append({
                    "frame_index": frame_index * 10,
                    "path": str(frame_path),
                    "width": 224,
                    "height": 224,
                })

            (video_dir / "manifest.json").write_text(
                json.dumps({
                    "source_video": "/data/incoming/original-video.mp4",
                    "source_frame_count": 20,
                    "extracted_frame_count": 2,
                    "target_size": [224, 224],
                    "frames": frame_records,
                }),
                encoding="utf-8",
            )

            output_dir = root / "dataset_output"
            manifest = build_dataset(input_dir, output_dir)

            self.assertEqual(manifest["total_samples"], 3)
            self.assertEqual(manifest["total_images"], 1)
            self.assertEqual(manifest["total_video_frames"], 2)

            all_samples = []
            archived_sample_count = 0
            video_split_names = set()
            for split_details in manifest["splits"].values():
                split_video_ids = {
                    sample["video_sample_id"]
                    for shard in split_details["shards"]
                    for sample in shard["samples"]
                    if sample["source_type"] == "video_frame"
                }
                self.assertLessEqual(len(split_video_ids), 1)
                if split_video_ids:
                    video_split_names.add(
                        next(iter(split_video_ids))
                    )
                all_samples.extend(
                    sample
                    for shard in split_details["shards"]
                    for sample in shard["samples"]
                )

                for shard in split_details["shards"]:
                    with tarfile.open(
                        output_dir / shard["shard"],
                        "r",
                    ) as archive:
                        archived_sample_count += len(archive.getnames())

            self.assertEqual(archived_sample_count, 3)
            video_samples = [
                sample
                for sample in all_samples
                if sample["source_type"] == "video_frame"
            ]
            self.assertEqual(len(video_samples), 2)
            self.assertEqual(len(video_split_names), 1)
            self.assertTrue(all(
                sample["source_video"]
                == "/data/incoming/original-video.mp4"
                for sample in video_samples
            ))
            self.assertEqual(
                {sample["video_frame_index"] for sample in video_samples},
                {0, 10},
            )

    def test_missing_input_directory_returns_no_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = build_dataset(
                root / "missing",
                root / "dataset_output",
            )

        self.assertIsNone(manifest)


if __name__ == "__main__":
    unittest.main()
