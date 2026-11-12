import tempfile
import unittest
from pathlib import Path

from PIL import Image

from ingestion.producer import is_valid_media


class IngestionValidationTests(unittest.TestCase):
    def test_valid_image_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            image_path = Path(directory) / "sample.png"
            Image.new("RGB", (8, 8), color="blue").save(image_path)

            self.assertTrue(is_valid_media(image_path))

    def test_corrupt_image_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            image_path = Path(directory) / "corrupt.png"
            image_path.write_bytes(b"not an image")

            self.assertFalse(is_valid_media(image_path))

    def test_unsupported_format_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            media_path = Path(directory) / "sample.gif"
            media_path.write_bytes(b"unsupported")

            self.assertFalse(is_valid_media(media_path))


if __name__ == "__main__":
    unittest.main()
