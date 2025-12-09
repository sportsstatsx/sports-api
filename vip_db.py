# vip_db.py
import os
import psycopg2
import psycopg2.extras

VIP_DATABASE_URL = os.environ["VIP_DATABASE_URL"]


def get_vip_connection():
    """
    VIP 전용 Postgres 연결 하나 열어주는 함수
    사용이 끝나면 conn.close() 꼭 호출해야 함.
    """
    return psycopg2.connect(
        VIP_DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def vip_fetch_one(query: str, params=None):
    """
    SELECT 한 줄만 가져오기
    """
    conn = get_vip_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            row = cur.fetchone()
        return row
    finally:
        conn.close()


def vip_execute(query: str, params=None):
    """
    INSERT / UPDATE / DELETE 용
    """
    conn = get_vip_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
        conn.commit()
    finally:
        conn.close()
