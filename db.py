# db.py
import os
from typing import Any, Sequence, Mapping, Optional, List, Dict

import psycopg
from psycopg_pool import ConnectionPool

# ─────────────────────────────────────────
# DATABASE_URL 읽기
# ─────────────────────────────────────────

DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("DATABASE_URL".upper())
    or os.environ.get("database_url")
)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# ─────────────────────────────────────────
# 커넥션 풀 (오토 커밋)
# ─────────────────────────────────────────

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"autocommit": True},
    max_size=10,
)

# ─────────────────────────────────────────
# get_connection: 필요하면 with 로 직접 쓰고 싶을 때 사용
# ─────────────────────────────────────────

def get_connection():
    """
    커넥션 풀에서 커넥션을 하나 꺼내는 컨텍스트 매니저를 반환.

    사용 예:
        from db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                ...

    psycopg_pool.ConnectionPool.connection() 자체가 컨텍스트 매니저이기 때문에
    그냥 그대로 감싸서 넘겨준다.
    """
    return pool.connection()

# ─────────────────────────────────────────
# fetch_all / fetch_one / execute 헬퍼
# ─────────────────────────────────────────

ParamsType = Optional[Sequence[Any] | Mapping[str, Any]]


def fetch_all(query: str, params: ParamsType = None) -> List[Dict[str, Any]]:
    """
    SELECT 계열에서 여러 row를 dict 리스트로 받고 싶을 때 사용.
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in rows]


def fetch_one(query: str, params: ParamsType = None) -> Optional[Dict[str, Any]]:
    """
    SELECT 한 row만 필요할 때 사용.
    없으면 None 반환.
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


def execute(query: str, params: ParamsType = None) -> None:
    """
    INSERT / UPDATE / DELETE 용.
    반환값은 신경 안 쓰고, 에러만 나지 않으면 된다고 가정.
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())

def close_pool():
    try:
        pool.close()
    except Exception:
        pass

