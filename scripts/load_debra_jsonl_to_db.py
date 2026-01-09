import argparse
import json
import os
import psycopg2

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to 05_debra_dialogue.jsonl")
    ap.add_argument("--source", required=True, help="Label for the episode/source (e.g. Dexter_1x01)")
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()

    with open(args.input, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            obj = json.loads(line)
            cur.execute(
                """
                INSERT INTO debra_lines (source_file, line_index, speaker, mode, text, raw)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    args.source,
                    idx,
                    obj.get("speaker"),
                    obj.get("mode"),
                    obj.get("text", ""),
                    json.dumps(obj),
                ),
            )

    conn.commit()
    cur.close()
    conn.close()
    print("Loaded:", args.input, "as source:", args.source)

if __name__ == "__main__":
    main()
