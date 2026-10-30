
from pathlib import Path
from PIL import Image
import cv2

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".webp"
}

VIDEO_EXTENSIONS = {
    ".mp4", ".avi", ".mov", ".mkv"
}


def validate_media(file_path):
    """
    Check whether an image or video can be opened.
    Returns a dictionary containing validation results.
    """
    path = Path(file_path)

    if not path.exists() or not path.is_file():
        return {
            "valid": False,
            "filename": path.name,
            "message": "File does not exist."
        }

    extension = path.suffix.lower()

    try:
        if extension in IMAGE_EXTENSIONS:
            # Verify that the image file is not corrupted.
            with Image.open(path) as image:
                image.verify()

            # Reopen to confirm that image dimensions are readable.
            with Image.open(path) as image:
                width, height = image.size

            if width <= 0 or height <= 0:
                raise ValueError("Invalid image dimensions.")

            return {
                "valid": True,
                "filename": path.name,
                "media_type": "image",
                "width": width,
                "height": height,
                "message": "Image is valid."
            }

        elif extension in VIDEO_EXTENSIONS:
            video = cv2.VideoCapture(str(path))

            try:
                if not video.isOpened():
                    raise ValueError("Video cannot be opened.")

                success, frame = video.read()

                if not success or frame is None:
                    raise ValueError("Video has no readable frame.")

                height, width = frame.shape[:2]

                return {
                    "valid": True,
                    "filename": path.name,
                    "media_type": "video",
                    "width": width,
                    "height": height,
                    "message": "Video is readable."
                }
            finally:
                video.release()

        else:
            return {
                "valid": False,
                "filename": path.name,
                "message": "Unsupported file format."
            }

    except Exception as error:
        return {
            "valid": False,
            "filename": path.name,
            "message": f"Invalid or corrupted media: {error}"
        }


if __name__ == "__main__":
    project_dir = Path(__file__).resolve().parents[1]
    incoming_dir = project_dir / "data" / "incoming"

    files = [
        file for file in incoming_dir.iterdir()
        if file.is_file()
    ]

    if not files:
        print("No files found in data/incoming.")
    else:
        for file in files:
            result = validate_media(file)
            print(result)
