
import json
import random
import tarfile
from pathlib import Path

from PIL import Image

PROJECT_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = PROJECT_DIR / "data" / "processed"
OUTPUT_DIR = PROJECT_DIR / "data" / "dataset_output"

# Dataset split proportions
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15
TEST_RATIO = 0.15

# Maximum images per shard
SHARD_SIZE = 100

SEED = 42


def split_dataset(files):
    """Shuffle files reproducibly and divide them into three splits."""
    files = list(files)
    random.Random(SEED).shuffle(files)

    total = len(files)
    train_end = int(total * TRAIN_RATIO)
    val_end = train_end + int(total * VAL_RATIO)

    return {
        "train": files[:train_end],
        "val": files[train_end:val_end],
        "test": files[val_end:],
    }


def create_shards(split_name, files):
    """Write image files into numbered TAR shards."""
    split_dir = OUTPUT_DIR / split_name
    split_dir.mkdir(parents=True, exist_ok=True)

    shard_count = 0

    for start in range(0, len(files), SHARD_SIZE):
        batch = files[start:start + SHARD_SIZE]
        shard_path = split_dir / f"{split_name}-{shard_count:05d}.tar"

        with tarfile.open(shard_path, "w") as archive:
            for index, image_path in enumerate(batch):
                # Use a unique sample name within each shard.
                sample_name = f"{start + index:08d}"

                archive.add(
                    image_path,
                    arcname=f"{sample_name}.jpg"
                )

        print(f"Created: {shard_path} ({len(batch)} images)")
        shard_count += 1

    return shard_count


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(
        path for path in INPUT_DIR.glob("*.jpg")
        if path.is_file()
    )

    if not files:
        print(f"No processed JPEG images found in {INPUT_DIR}")
        print("Run the image preprocessing step first.")
        return

    # Confirm the input files are readable before packaging.
    valid_files = []

    for path in files:
        try:
            with Image.open(path) as image:
                image.verify()
            valid_files.append(path)
        except Exception as error:
            print(f"Skipping invalid image {path.name}: {error}")

    if not valid_files:
        print("No valid images available for sharding.")
        return

    splits = split_dataset(valid_files)

    manifest = {
        "seed": SEED,
        "ratios": {
            "train": TRAIN_RATIO,
            "val": VAL_RATIO,
            "test": TEST_RATIO,
        },
        "total_images": len(valid_files),
        "splits": {},
    }

    for split_name, split_files in splits.items():
        shard_count = create_shards(split_name, split_files)

        manifest["splits"][split_name] = {
            "image_count": len(split_files),
            "shard_count": shard_count,
            "files": [path.name for path in split_files],
        }

    manifest_path = OUTPUT_DIR / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=4),
        encoding="utf-8"
    )

    print("\n--- DATASET SUMMARY ---")
    print("Total images:", len(valid_files))

    for split_name, details in manifest["splits"].items():
        print(
            f"{split_name}: {details['image_count']} images, "
            f"{details['shard_count']} shards"
        )

    print(f"Manifest saved to: {manifest_path}")
    print("Dataset splitting and sharding completed!")


if __name__ == "__main__":
    main()
