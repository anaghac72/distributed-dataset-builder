import json
import math
from pathlib import Path

import cv2

TARGET_SIZE = (224, 224)
DEFAULT_MAX_FRAMES = 5


def _resize_and_crop(frame, target_size):
    target_width, target_height = target_size
    height, width = frame.shape[:2]
    if width <= 0 or height <= 0:
        raise ValueError("Video frame has invalid dimensions.")

    scale = max(target_width / width, target_height / height)
    resized_width = math.ceil(width * scale)
    resized_height = math.ceil(height * scale)
    interpolation = (
        cv2.INTER_AREA if scale < 1 else cv2.INTER_LINEAR
    )
    resized = cv2.resize(
        frame,
        (resized_width, resized_height),
        interpolation=interpolation,
    )

    left = (resized_width - target_width) // 2
    top = (resized_height - target_height) // 2
    return resized[
        top:top + target_height,
        left:left + target_width,
    ]


def process_video_once(
    input_path,
    output_dir,
    target_size=TARGET_SIZE,
    max_frames=DEFAULT_MAX_FRAMES,
):
    """Extract up to max_frames sampled frames and save a JSON manifest."""
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    if max_frames < 1:
        raise ValueError("max_frames must be at least 1.")

    capture = cv2.VideoCapture(str(input_path))
    if not capture.isOpened():
        capture.release()
        raise ValueError(f"Video cannot be opened: {input_path}")

    frame_records = []

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))

        if frame_count > 0:
            if frame_count <= max_frames:
                frame_indices = list(range(frame_count))
            elif max_frames == 1:
                frame_indices = [0]
            else:
                frame_indices = sorted({
                    round(index * (frame_count - 1) / (max_frames - 1))
                    for index in range(max_frames)
                })

            sampled_frames = []
            for frame_index in frame_indices:
                if not capture.set(cv2.CAP_PROP_POS_FRAMES, frame_index):
                    raise ValueError(
                        f"Could not seek to video frame {frame_index}."
                    )

                success, frame = capture.read()
                if not success or frame is None:
                    raise ValueError(
                        f"Could not decode video frame {frame_index}."
                    )
                sampled_frames.append((frame_index, frame))
        else:
            sampled_frames = []
            for frame_index in range(max_frames):
                success, frame = capture.read()
                if not success or frame is None:
                    break
                sampled_frames.append((frame_index, frame))

        if not sampled_frames:
            raise ValueError(f"No readable frames found: {input_path}")

        for sample_number, (frame_index, frame) in enumerate(
            sampled_frames,
            start=1,
        ):
            resized_frame = _resize_and_crop(frame, target_size)
            output_path = output_dir / f"frame_{sample_number:04d}.jpg"
            temporary_path = output_path.with_name(
                output_path.stem + ".tmp.jpg"
            )

            if not cv2.imwrite(str(temporary_path), resized_frame):
                raise OSError(f"Could not write frame: {temporary_path}")

            temporary_path.replace(output_path)
            frame_records.append({
                "frame_index": frame_index,
                "path": str(output_path),
                "width": target_size[0],
                "height": target_size[1],
            })
    finally:
        capture.release()

    manifest_path = output_dir / "manifest.json"
    temporary_manifest = manifest_path.with_suffix(".tmp")
    temporary_manifest.write_text(
        json.dumps({
            "source_video": str(input_path.resolve()),
            "source_frame_count": frame_count,
            "extracted_frame_count": len(frame_records),
            "target_size": list(target_size),
            "frames": frame_records,
        }, indent=2),
        encoding="utf-8",
    )
    temporary_manifest.replace(manifest_path)

    return {
        "manifest_path": str(manifest_path),
        "frame_count": len(frame_records),
        "frames": frame_records,
    }