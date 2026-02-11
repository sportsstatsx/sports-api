# basketball/nba/nba_db.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row


def _nba_dsn() -> str:
    dsn = os.environ.get("NBA_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("NBA_DATABASE_URL is not set")
    return dsn


def nba_fetch_all(query: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    dsn = _nba_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def nba_fetch_one(query: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    rows = nba_fetch_all(query, params)
    return rows[0] if rows else None


def nba_execute(query: str, params: Optional[Sequence[Any]] = None) -> None:
    """
    INSERT/UPDATE/DELETE 용.
    (standings 구현에는 당장 필요 없지만, NBA 쪽 확장 시 반드시 필요해짐)
    """
    dsn = _nba_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
        conn.commit()
