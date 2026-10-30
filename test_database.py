
from storage.metadata_db import (
    initialize_database,
    get_all_media,
    update_media_status
)

initialize_database()

records = get_all_media()

if records:
    # The source path is retrieved using a separate query.
    from storage.metadata_db import get_connection

    with get_connection() as connection:
        row = connection.execute("""
            SELECT source_path
            FROM media_files
            LIMIT 1
        """).fetchone()

    source_path = row[0]

    update_media_status(source_path, "PROCESSING")
    print("Status changed to PROCESSING")

    update_media_status(
        source_path,
        "SUCCESS",
        "data/processed/test_output.jpg"
    )
    print("Status changed to SUCCESS")

else:
    print("No records found. Run the ingestion producer first.")
