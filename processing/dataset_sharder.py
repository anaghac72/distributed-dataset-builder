import json
import random
import tarfile
from pathlib import Path

from PIL import Image

PROJECT_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = PROJECT_DIR / "data" / "processed"
OUTPUT_DIR = PROJECT_DIR / "data" / "dataset_output"

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

# Dataset split proportions
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15
TEST_RATIO = 0.15

# Maximum images per shard
SHARD_SIZE = 100

SEED = 42


def split_dataset(files):
    """Split samples while keeping frames from each video in one split."""
    files = list(files)
    if not any(sample["source_type"] == "video_frame" for sample in files):
        random.Random(SEED).shuffle(files)

        total = len(files)
        train_end = int(total * TRAIN_RATIO)
        val_end = train_end + int(total * VAL_RATIO)

        return {
            "train": files[:train_end],
            "val": files[train_end:val_end],
            "test": files[val_end:],
        }

    groups = {}
    for index, sample in enumerate(files):
        if sample["source_type"] == "video_frame":
            group_key = ("video", sample["video_sample_id"])
        else:
            group_key = ("image", index)
        groups.setdefault(group_key, []).append(sample)

    shuffled_groups = list(groups.values())
    random.Random(SEED).shuffle(shuffled_groups)

    ratios = (TRAIN_RATIO, VAL_RATIO, TEST_RATIO)
    target_counts = [len(files) * ratio for ratio in ratios]
    split_names = ("train", "val", "test")
    splits = {name: [] for name in split_names}
    counts = [0, 0, 0]

    for group in shuffled_groups:
        split_index = min(
            range(len(split_names)),
            key=lambda index: (
                counts[index] + len(group) - target_counts[index]
            ) ** 2
            - (counts[index] - target_counts[index]) ** 2,
        )
        splits[split_names[split_index]].extend(group)
        counts[split_index] += len(group)

    return splits


def _is_readable_image(path):
    try:
        with Image.open(path) as image:
            image.verify()
        return True
    except (OSError, ValueError) as error:
        print(f"Skipping invalid image {path.name}: {error}")
        return False


def collect_samples(input_dir=INPUT_DIR):
    """Collect valid processed images and frames with video provenance."""
    input_dir = Path(input_dir)
    samples = []

    if not input_dir.is_dir():
        return samples

    for path in sorted(input_dir.iterdir()):
        if (
            path.is_file()
            and path.suffix.lower() in IMAGE_EXTENSIONS
            and _is_readable_image(path)
        ):
            samples.append({
                "path": str(path),
                "source_type": "image",
                "source_file": path.name,
            })

    video_frames_dir = input_dir / "videos"
    if not video_frames_dir.is_dir():
        return samples

    for manifest_path in sorted(video_frames_dir.glob("*/manifest.json")):
        try:
            video_manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as error:
            print(f"Skipping unreadable video manifest {manifest_path}: {error}")
            continue

        source_video = video_manifest.get("source_video")
        frames = video_manifest.get("frames")
        if not isinstance(source_video, str) or not isinstance(frames, list):
            print(f"Skipping invalid video manifest: {manifest_path}")
            continue

        for frame in frames:
            if not isinstance(frame, dict) or not isinstance(
                frame.get("path"), str
            ):
                print(f"Skipping invalid frame entry in {manifest_path}")
                continue

            frame_path = Path(frame["path"])
            if not frame_path.is_absolute():
                frame_path = manifest_path.parent / frame_path

            if (
                frame_path.suffix.lower() not in IMAGE_EXTENSIONS
                or not frame_path.is_file()
                or not _is_readable_image(frame_path)
            ):
                continue

            samples.append({
                "path": str(frame_path),
                "source_type": "video_frame",
                "source_file": frame_path.name,
                "source_video": source_video,
                "video_frame_index": frame.get("frame_index"),
                "video_sample_id": manifest_path.parent.name,
            })

    return samples


def create_shards(split_name, samples, output_dir=OUTPUT_DIR):
    """Write samples into numbered TAR shards and return their metadata."""
    output_dir = Path(output_dir)
    split_dir = output_dir / split_name
    split_dir.mkdir(parents=True, exist_ok=True)

    shard_records = []

    for shard_number, start in enumerate(range(0, len(samples), SHARD_SIZE)):
        batch = samples[start:start + SHARD_SIZE]
        shard_path = split_dir / f"{split_name}-{shard_number:05d}.tar"
        sample_records = []

        with tarfile.open(shard_path, "w") as archive:
            for index, sample in enumerate(batch):
                extension = Path(sample["path"]).suffix.lower()
                archive_name = f"{start + index:08d}{extension}"
                archive.add(sample["path"], arcname=archive_name)
                sample_records.append({
                    "archive_path": archive_name,
                    "source_type": sample["source_type"],
                    "source_file": sample["source_file"],
                    **(
                        {
                            "source_video": sample["source_video"],
                            "video_sample_id": sample["video_sample_id"],
                            "video_frame_index": sample["video_frame_index"],
                        }
                        if sample["source_type"] == "video_frame"
                        else {}
                    ),
                })

        print(f"Created: {shard_path} ({len(batch)} samples)")
        shard_records.append({
            "shard": str(shard_path.relative_to(output_dir)),
            "sample_count": len(batch),
            "samples": sample_records,
        })

    return shard_records


def build_dataset(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR):
    """Collect, split, and package image and video-frame samples."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    samples = collect_samples(input_dir)
    if not samples:
        print(f"No valid processed images or video frames found in {input_dir}")
        print("Run the image or video processing step first.")
        return None

    splits = split_dataset(samples)
    manifest = {
        "seed": SEED,
        "ratios": {
            "train": TRAIN_RATIO,
            "val": VAL_RATIO,
            "test": TEST_RATIO,
        },
        "total_samples": len(samples),
        "total_images": sum(
            sample["source_type"] == "image" for sample in samples
        ),
        "total_video_frames": sum(
            sample["source_type"] == "video_frame" for sample in samples
        ),
        "splits": {},
    }

    for split_name, split_samples in splits.items():
        shard_records = create_shards(
            split_name,
            split_samples,
            output_dir,
        )
        image_count = sum(
            sample["source_type"] == "image" for sample in split_samples
        )
        video_frame_count = len(split_samples) - image_count
        manifest["splits"][split_name] = {
            "image_count": image_count,
            "video_frame_count": video_frame_count,
            "sample_count": len(split_samples),
            "shard_count": len(shard_records),
            "shards": shard_records,
            "files": [sample["source_file"] for sample in split_samples],
        }

    manifest_path = output_dir / "manifest.json"
    temporary_manifest = manifest_path.with_suffix(".tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, indent=4),
        encoding="utf-8",
    )
    temporary_manifest.replace(manifest_path)

    print("\n--- DATASET SUMMARY ---")
    print("Total samples:", manifest["total_samples"])
    print("Source images:", manifest["total_images"])
    print("Video frames:", manifest["total_video_frames"])

    for split_name, details in manifest["splits"].items():
        print(
            f"{split_name}: {details['sample_count']} samples "
            f"({details['image_count']} images, "
            f"{details['video_frame_count']} video frames), "
            f"{details['shard_count']} shards"
        )

    print(f"Manifest saved to: {manifest_path}")
    print("Dataset splitting and sharding completed!")
    return manifest


def main():
    build_dataset()


if __name__ == "__main__":
    main()
