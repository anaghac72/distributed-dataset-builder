
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
RECOVERY_DIR = PROJECT_DIR / "data" / "recovery"
RETRY_FILE = RECOVERY_DIR / "retry_queue.json"
ERROR_LOG = RECOVERY_DIR / "errors.jsonl"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def load_retry_queue():
    RECOVERY_DIR.mkdir(parents=True, exist_ok=True)

    if not RETRY_FILE.exists():
        return []

    try:
        return json.loads(RETRY_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        print("Warning: retry queue could not be read.")
        return []


def save_retry_queue(queue):
    RECOVERY_DIR.mkdir(parents=True, exist_ok=True)
    temporary_file = RETRY_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(queue, indent=4),
        encoding="utf-8",
    )
    temporary_file.replace(RETRY_FILE)


def log_error(operation, item, error, attempt):
    RECOVERY_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "error_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "operation": operation,
        "item": str(item),
        "attempt": attempt,
        "error": str(error),
    }

    with ERROR_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")


def add_to_retry_queue(item, operation, error):
    queue = load_retry_queue()

    existing = next(
        (
            task for task in queue
            if task["item"] == str(item)
            and task["operation"] == operation
        ),
        None,
    )

    if existing:
        existing["last_error"] = str(error)
        existing["updated_at"] = datetime.now(
            timezone.utc
        ).isoformat()
    else:
        queue.append({
            "task_id": str(uuid.uuid4()),
            "item": str(item),
            "operation": operation,
            "attempts": 0,
            "status": "PENDING",
            "last_error": str(error),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    save_retry_queue(queue)


def run_with_retries(operation_name, item, operation):
    """
    Run a callable that takes no arguments.
    Retry it up to MAX_RETRIES times after the initial attempt.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            result = operation()

            # Remove a previous queued task after success.
            queue = load_retry_queue()
            queue = [
                task for task in queue
                if not (
                    task["item"] == str(item)
                    and task["operation"] == operation_name
                )
            ]
            save_retry_queue(queue)

            return result

        except Exception as error:
            last_error = error
            log_error(operation_name, item, error, attempt)

            print(
                f"Attempt {attempt}/{MAX_RETRIES + 1} failed "
                f"for {item}: {error}"
            )

            if attempt <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)

    add_to_retry_queue(item, operation_name, last_error)

    raise RuntimeError(
        f"{operation_name} failed after "
        f"{MAX_RETRIES + 1} attempts for {item}"
    ) from last_error


if __name__ == "__main__":
    print("Testing retry manager...")

    counter = {"attempts": 0}

    def demo_operation():
        counter["attempts"] += 1

        if counter["attempts"] < 3:
            raise ValueError("Simulated temporary failure")

        return "Operation succeeded"

    result = run_with_retries(
        "demo_test",
        "sample_image.png",
        demo_operation,
    )

    print(result)
    print("Total attempts:", counter["attempts"])
    print("Retry manager test completed.")