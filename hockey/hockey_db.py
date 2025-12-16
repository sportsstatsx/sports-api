# hockey/hockey_db.py
import os
from typing import Any, Mapping, Optional, Sequence

import psycopg
from psycopg_pool import ConnectionPool

# ─────────────────────────────────────────
# 하키 DB URL 읽기 (Render env: HOCKEY_DATABASE_URL)
# ─────────────────────────────────────────
HOCKEY_DATABASE_URL = (
    os.environ.get("HOCKEY_DATABASE_URL")
    or os.environ.get("HOCKEY_DATABASE_URL".upper())
    or os.environ.get("hockey_database_url")
)

if not HOCKEY_DATABASE_URL:
    raise RuntimeError("HOCKEY_DATABASE_URL is not set")

# ─────────────────────────────────────────
# 하키 전용 커넥션 풀 (오토 커밋)
# ─────────────────────────────────────────
_hockey_pool = ConnectionPool(
    conninfo=HOCKEY_DATABASE_URL,
    kwargs={"autocommit": True},
    max_size=10,
)


def hockey_fetch_all(sql: str, params: Optional[Sequence[Any]] = None) -> list[Mapping[str, Any]]:
    """
    SELECT 여러 행
    """
    with _hockey_pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def hockey_fetch_one(sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Mapping[str, Any]]:
    """
    SELECT 1행
    """
    with _hockey_pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


def hockey_execute(sql: str, params: Optional[Sequence[Any]] = None) -> None:
    """
    INSERT / UPDATE / DELETE
    """
    with _hockey_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def hockey_close_pool() -> None:
    try:
        _hockey_pool.close()
    except Exception:
        pass
