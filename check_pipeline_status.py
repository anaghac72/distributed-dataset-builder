import sqlite3

conn = sqlite3.connect("data/metadata.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT status, COUNT(*)
    FROM media_files
    GROUP BY status
""")

print("PIPELINE STATUS")

for status, count in cursor.fetchall():
    print(f"{status}: {count}")

cursor.execute("""
    SELECT filename, status
    FROM media_files
    WHERE status IN ('PROCESSING', 'RECEIVED')
""")

pending = cursor.fetchall()

print("\nPENDING RECORDS:")

if pending:
    for record in pending:
        print(record)
else:
    print("No pending records.")

conn.close()