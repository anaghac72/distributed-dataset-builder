import sqlite3

conn = sqlite3.connect("data/metadata.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT sample_id, filename, status, output_path
    FROM media_files
    WHERE status = 'FAILED'
""")

records = cursor.fetchall()

print("FAILED RECORDS:")

for record in records:
    print(record)

conn.close()