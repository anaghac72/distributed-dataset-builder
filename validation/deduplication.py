
import hashlib
from pathlib import Path

import imagehash
from PIL import Image, UnidentifiedImageError

PROJECT_DIR = Path(__file__).resolve().parents[1]
INCOMING_DIR = PROJECT_DIR / "data" / "incoming"

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".webp"
}

# Hamming distance threshold for perceptual hashes.
# Smaller values mean images must be more similar.
SIMILARITY_THRESHOLD = 5


def calculate_sha256(file_path):
    """Calculate a hash of the complete file contents."""
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def calculate_perceptual_hash(file_path):
    """Calculate a visual fingerprint for an image."""
    with Image.open(file_path) as image:
        image.load()
        return imagehash.phash(image.convert("RGB"))


def find_duplicates():
    if not INCOMING_DIR.exists():
        print("Incoming folder does not exist.")
        return

    exact_hashes = {}
    perceptual_hashes = {}
    exact_duplicates = []
    similar_images = []
    invalid_images = []

    image_files = sorted(
        path for path in INCOMING_DIR.iterdir()
        if path.is_file()
        and path.suffix.lower() in IMAGE_EXTENSIONS
    )

    print(f"Scanning {len(image_files)} images...")

    for path in image_files:
        try:
            sha256 = calculate_sha256(path)

            if sha256 in exact_hashes:
                exact_duplicates.append(
                    (path.name, exact_hashes[sha256])
                )
            else:
                exact_hashes[sha256] = path.name

            phash = calculate_perceptual_hash(path)

            # Compare against earlier images.
            for previous_hash, previous_name in perceptual_hashes.items():
                distance = phash - previous_hash

                if (
                    0 < distance <= SIMILARITY_THRESHOLD
                    and sha256 != calculate_sha256(
                        INCOMING_DIR / previous_name
                    )
                ):
                    similar_images.append(
                        (path.name, previous_name, distance)
                    )

            perceptual_hashes[phash] = path.name

        except (
            OSError,
            ValueError,
            UnidentifiedImageError
        ) as error:
            invalid_images.append((path.name, str(error)))

    print("\n--- EXACT DUPLICATES ---")
    if exact_duplicates:
        for duplicate, original in exact_duplicates:
            print(f"{duplicate} duplicates {original}")
    else:
        print("No exact duplicates found.")

    print("\n--- VISUALLY SIMILAR IMAGES ---")
    if similar_images:
        for first, second, distance in similar_images:
            print(
                f"{first} resembles {second} "
                f"(hash distance: {distance})"
            )
    else:
        print("No visually similar images found.")

    print("\n--- INVALID IMAGES ---")
    if invalid_images:
        for filename, error in invalid_images:
            print(f"{filename}: {error}")
    else:
        print("No invalid images found.")

    print("\n--- SUMMARY ---")
    print("Images scanned:", len(image_files))
    print("Exact duplicate pairs:", len(exact_duplicates))
    print("Similar image pairs:", len(similar_images))
    print("Invalid images:", len(invalid_images))


if __name__ == "__main__":
    find_duplicates()
