# scripts/db.py
import os
import psycopg

def get_conn():
    return psycopg.connect(os.environ["DATABASE_URL"])

def insert_debra_swear_bucket(conn, source_file, bucket, count, tokens):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into debra_swear_bucket (source_file, bucket, count, tokens)
            values (%s, %s, %s, %s)
            on conflict (source_file, bucket)
            do update set
              count = excluded.count,
              tokens = excluded.tokens;
            """,
            (source_file, bucket, count, tokens),
        )
    conn.commit()
