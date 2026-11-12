import unittest

import imagehash

from validation.deduplication import find_similar_hash


class PerceptualDeduplicationTests(unittest.TestCase):
    def test_same_hash_is_similar(self):
        candidate = imagehash.hex_to_hash("0" * 16)

        result = find_similar_hash(
            candidate,
            [("original.png", "0" * 16)],
        )

        self.assertEqual(result, ("original.png", 0))

    def test_near_hash_is_similar(self):
        candidate = imagehash.hex_to_hash("0" * 16)

        result = find_similar_hash(
            candidate,
            [("slightly-edited.png", "0" * 15 + "1")],
        )

        self.assertEqual(result, ("slightly-edited.png", 1))

    def test_distant_hash_is_not_similar(self):
        candidate = imagehash.hex_to_hash("0" * 16)

        result = find_similar_hash(
            candidate,
            [("different.png", "f" * 16)],
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()