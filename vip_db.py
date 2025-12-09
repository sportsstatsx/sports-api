# vip_db.py
import os
import psycopg
from psycopg.rows import dict_row

VIP_DATABASE_URL = os.environ["VIP_DATABASE_URL"]


def get_vip_connection():
    # VIP 전용 Postgres 연결
    return psycopg.connect(VIP_DATABASE_URL, row_factory=dict_row)


def vip_fetch_one(query: str, params=None):
    """
    SELECT 한 row를 하나만 가져오는 헬퍼
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
    INSERT / UPDATE / DELETE 용 헬퍼
    """
    conn = get_vip_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            conn.commit()
    finally:
        conn.close()
