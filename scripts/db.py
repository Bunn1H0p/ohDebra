import json
import os
import psycopg2

JSONL_PATH = "input/debra.jsonl"   # <- change
SOURCE_FILE = "Dexter_1x01_-_Pilot.pdf"  # <- optional label

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

with open(JSONL_PATH, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f, start=1):
        obj = json.loads(line)
        text = obj.get("text", "")
        cur.execute(
            """
            INSERT INTO debra_lines (source_file, line_index, text, raw)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (SOURCE_FILE, idx, text, json.dumps(obj)),
        )

conn.commit()
cur.close()
conn.close()
print("Loaded:", JSONL_PATH)
