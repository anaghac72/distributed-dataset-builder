
from pathlib import Path
from time import perf_counter

import ray
from PIL import Image, ImageOps

from recovery.retry_manager import run_with_retries
from storage.metadata_db import (
    initialize_database,
    get_pending_media,
    update_media_status,
)

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "data" / "processed"

TARGET_SIZE = (224, 224)


def process_image_once(input_path, output_path):
    """Resize and save one image."""

    input_path = Path(input_path)
    output_path = Path(output_path)

    with Image.open(input_path) as image:
        image = ImageOps.exif_transpose(image)
        image = image.convert("RGB")
        image = ImageOps.fit(image, TARGET_SIZE)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path = output_path.with_name(
            output_path.stem + ".tmp.jpg"
        )

        image.save(temporary_path, format="JPEG", quality=90)
        temporary_path.replace(output_path)

    return {
        "filename": input_path.name,
        "status": "SUCCESS",
        "output": str(output_path),
    }


@ray.remote
def process_image(input_path, output_path):
    """Process an image with retries and database updates."""

    try:
        update_media_status(input_path, "PROCESSING")

        result = run_with_retries(
            operation_name="image_preprocessing",
            item=input_path,
            operation=lambda: process_image_once(
                input_path, output_path
            ),
        )

        update_media_status(
            input_path,
            "SUCCESS",
            result["output"],
        )

        return result

    except Exception as error:
        update_media_status(input_path, "FAILED")

        return {
            "filename": Path(input_path).name,
            "status": "FAILED",
            "error": str(error),
        }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    initialize_database()

    pending_records = get_pending_media()

    if not pending_records:
        print("No unfinished images found.")
        print("All registered images are already processed.")
        return

    print("\n--- RECOVERY CHECK ---")
    print("Unfinished images found:", len(pending_records))

    tasks = []

    for record in pending_records:
        sample_id, filename, source_path, status, _, _ = record

        input_path = Path(source_path)

        if not input_path.is_file():
            print(f"Missing input file: {input_path}")
            update_media_status(input_path, "FAILED")
            continue

        output_path = OUTPUT_DIR / f"{input_path.stem}.jpg"

        print(f"Submitting {filename} (previous status: {status})")

        tasks.append(
            process_image.remote(
                str(input_path),
                str(output_path),
            )
        )

    if not tasks:
        print("No available images to process.")
        return

    print("\nStarting Ray workers...")

    ray.init(
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
    )

    try:
        start = perf_counter()

        results = ray.get(tasks)

        elapsed = perf_counter() - start

        successful = sum(
            result["status"] == "SUCCESS"
            for result in results
        )

        failed = len(results) - successful

        for result in results:
            print(result)

        print("\n--- RECOVERY SUMMARY ---")
        print("Images submitted:", len(tasks))
        print("Successful:", successful)
        print("Failed:", failed)
        print(f"Processing time: {elapsed:.2f} seconds")
        print("Output folder:", OUTPUT_DIR)

    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
