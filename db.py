# db.py
import os
import psycopg
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL".upper()) or os.environ.get("database_url")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# 커넥션 풀 (오토 커밋)
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"autocommit": True},
    max_size=10,
)

def fetch_all(query: str, params: tuple | dict | None = None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]

def fetch_one(query: str, params: tuple | dict | None = None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

def execute(query: str, params: tuple | dict | None = None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
