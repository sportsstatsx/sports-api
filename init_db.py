# init_db.py
import os
import sys
from datetime import date
from psycopg_pool import ConnectionPool
import psycopg

# ─────────────────────────────────────────────────────
# 1) 환경 변수에서 DATABASE_URL 읽기
# ─────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL environment variable is not set.", file=sys.stderr)
    sys.exit(1)

# ─────────────────────────────────────────────────────
# 2) 커넥션 풀 생성 (psycopg_pool)
#    - Render Postgres는 sslmode=require 사용 권장
# ─────────────────────────────────────────────────────
# DATABASE_URL에 이미 sslmode=require가 포함돼 있으면 그대로 사용됩니다.
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=3,
    timeout=10,
)

# ─────────────────────────────────────────────────────
# 3) 스키마 생성 + 시드 함수
#    - 여러 번 실행해도 안전하도록 IF NOT EXISTS / ON CONFLICT DO NOTHING 사용
# ─────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fixtures (
    id            BIGSERIAL PRIMARY KEY,
    league_id     INTEGER NOT NULL,
    match_date    DATE    NOT NULL,
    home_team     TEXT    NOT NULL,
    away_team     TEXT    NOT NULL,
    home_score    INTEGER,
    away_score    INTEGER,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_fixture UNIQUE (league_id, match_date, home_team, away_team)
);
"""

# 샘플: 프리미어리그(39) 2경기
SEED_SQL = """
INSERT INTO fixtures (league_id, match_date, home_team, away_team, home_score, away_score)
VALUES
    (39, DATE '2025-11-12', 'Arsenal',   'Chelsea', NULL, NULL),
    (39, DATE '2025-11-12', 'Liverpool', 'Manchester City', NULL, NULL)
ON CONFLICT ON CONSTRAINT uq_fixture DO NOTHING;
"""

def init_db():
    print("Connecting to Postgres...")
    with pool.connection() as conn:
        # autocommit 보장 (DDL에 필요)
        conn.execute(psycopg.sql.SQL("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"))
        with conn.cursor() as cur:
            print("Creating tables if not exists...")
            cur.execute(CREATE_TABLE_SQL)

            print("Seeding sample fixtures...")
            cur.execute(SEED_SQL)

    print("✅ DB initialized and seeded.")

# ─────────────────────────────────────────────────────
# 4) 메인 실행부
#    - pool.close()를 보장하여 스레드 종료 경고 제거
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        init_db()
    finally:
        try:
            # 스레드 풀 정리 (경고 방지)
            pool.close()
        except Exception:
            # 혹시 모를 예외는 무시 (초기화는 이미 완료됨)
            pass
