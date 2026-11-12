import argparse
import json
import logging
import time
from pathlib import Path

from processing.dataset_sharder import build_dataset
from storage.metadata_db import get_connection, initialize_database

PROJECT_DIR = Path(__file__).resolve().parents[1]
STATE_PATH = PROJECT_DIR / "data" / "orchestration" / "sharder_state.json"
DEFAULT_POLL_SECONDS = 30

logger = logging.getLogger(__name__)


def get_latest_success_cursor():
    with get_connection() as connection:
        row = connection.execute("""
            SELECT updated_at, id
            FROM media_files
            WHERE status = 'SUCCESS'
              AND updated_at IS NOT NULL
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
        """).fetchone()

    return (row[0], row[1]) if row else None


def load_cursor(state_path=STATE_PATH):
    state_path = Path(state_path)
    if not state_path.exists():
        return None

    state = json.loads(state_path.read_text(encoding="utf-8"))
    updated_at = state.get("updated_at")
    record_id = state.get("record_id")

    if not isinstance(updated_at, str) or not isinstance(record_id, int):
        raise ValueError(f"Invalid scheduler state file: {state_path}")

    return updated_at, record_id


def save_cursor(cursor, state_path=STATE_PATH):
    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = state_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps({
            "updated_at": cursor[0],
            "record_id": cursor[1],
        }, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(state_path)


def run_scheduler_once(state_path=STATE_PATH):
    """Rebuild shards when successful media newer than the saved cursor exists."""
    initialize_database()
    current_cursor = get_latest_success_cursor()
    if current_cursor is None:
        logger.info("No successfully processed media yet.")
        return False

    previous_cursor = load_cursor(state_path)
    if previous_cursor is not None and current_cursor <= previous_cursor:
        return False

    manifest = build_dataset()
    if manifest is None:
        logger.info("No shardable samples found; scheduler will retry later.")
        return False

    save_cursor(current_cursor, state_path)
    logger.info(
        "Dataset shards rebuilt through successful media record %s.",
        current_cursor[1],
    )
    return True


def run_scheduler(poll_seconds=DEFAULT_POLL_SECONDS):
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be greater than zero.")

    logger.info(
        "Watching for successfully processed media every %s seconds.",
        poll_seconds,
    )
    while True:
        try:
            run_scheduler_once()
        except Exception:
            logger.exception(
                "Scheduled sharding failed; it will be retried next poll."
            )
        time.sleep(poll_seconds)


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild dataset shards as media processing completes."
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help="Seconds between SQLite checks (default: 30).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Check once and exit instead of polling continuously.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.once:
        run_scheduler_once()
    else:
        run_scheduler(args.poll_seconds)


if __name__ == "__main__":
    main()
