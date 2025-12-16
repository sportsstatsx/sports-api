# hockey/hockey_db.py
from __future__ import annotations

import os
from typing import Any, Mapping, Optional, Sequence

import psycopg
from psycopg_pool import ConnectionPool

HOCKEY_DATABASE_URL = os.environ.get("HOCKEY_DATABASE_URL")
if not HOCKEY_DATABASE_URL:
    raise RuntimeError("HOCKEY_DATABASE_URL is not set")

_hockey_pool = ConnectionPool(
    conninfo=HOCKEY_DATABASE_URL,
    kwargs={"autocommit": True},
    max_size=10,
)


def hockey_fetch_all(sql: str, params: Optional[Sequence[Any]] = None) -> list[Mapping[str, Any]]:
    with _hockey_pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def hockey_fetch_one(sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Mapping[str, Any]]:
    with _hockey_pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


def hockey_execute(sql: str, params: Optional[Sequence[Any]] = None) -> None:
    with _hockey_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def hockey_close_pool() -> None:
    try:
        _hockey_pool.close()
    except Exception:
        pass
