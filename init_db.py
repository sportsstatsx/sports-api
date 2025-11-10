# init_db.py
import os
import sys
from psycopg_pool import ConnectionPool
import psycopg

# ─────────────────────────────────────────────────────
# 1) 환경 변수: DATABASE_URL
# ─────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL environment variable is not set.", file=sys.stderr)
    sys.exit(1)

# ─────────────────────────────────────────────────────
# 2) 커넥션 풀
# ─────────────────────────────────────────────────────
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=3,
    timeout=10,
)

# ─────────────────────────────────────────────────────
# 3) 스키마 DDL
# ─────────────────────────────────────────────────────
CREATE_TEAMS_SQL = """
CREATE TABLE IF NOT EXISTS teams (
    id         BIGSERIAL PRIMARY KEY,
    league_id  INTEGER NOT NULL,
    name       TEXT    NOT NULL,
    country    TEXT,
    short_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_team UNIQUE (league_id, name)
);
"""

CREATE_FIXTURES_SQL = """
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

# ─────────────────────────────────────────────────────
# 4) 시드 데이터 (idempotent)
# ─────────────────────────────────────────────────────
SEED_TEAMS_SQL = """
INSERT INTO teams (league_id, name, country, short_name) VALUES
    (39, 'Arsenal',           'England', 'ARS'),
    (39, 'Chelsea',           'England', 'CHE'),
    (39, 'Liverpool',         'England', 'LIV'),
    (39, 'Manchester City',   'England', 'MCI')
ON CONFLICT ON CONSTRAINT uq_team DO NOTHING;
"""

SEED_FIXTURES_SQL = """
INSERT INTO fixtures (league_id, match_date, home_team, away_team, home_score, away_score) VALUES
    (39, DATE '2025-11-12', 'Arsenal',           'Chelsea',           NULL, NULL),
    (39, DATE '2025-11-12', 'Liverpool',         'Manchester City',   NULL, NULL)
ON CONFLICT ON CONSTRAINT uq_fixture DO NOTHING;
"""

# ─────────────────────────────────────────────────────
# 5) 스키마 정규화(마이그레이션): fixtures.date → fixtures.match_date
# ─────────────────────────────────────────────────────
def normalize_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'fixtures'
        """)
        cols = {r[0] for r in cur.fetchall()}
        if "date" in cols and "match_date" not in cols:
            print("Renaming column fixtures.date -> fixtures.match_date ...")
            cur.execute("ALTER TABLE fixtures RENAME COLUMN date TO match_date;")
            print("Renamed successfully.")

# ─────────────────────────────────────────────────────
# 6) 초기화 루틴
# ─────────────────────────────────────────────────────
def init_db():
    print("Connecting to Postgres...")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            print("Creating tables (teams, fixtures) if not exists...")
            cur.execute(CREATE_TEAMS_SQL)
            cur.execute(CREATE_FIXTURES_SQL)

        # 기존 컬럼명 사용 중이면 정규화
        normalize_schema(conn)

        with conn.cursor() as cur:
            print("Seeding teams...")
            cur.execute(SEED_TEAMS_SQL)

            print("Seeding sample fixtures...")
            cur.execute(SEED_FIXTURES_SQL)

    print("✅ DB initialized and seeded.")

# ─────────────────────────────────────────────────────
# 7) 엔트리포인트
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        init_db()
    finally:
        try:
            pool.close()
        except Exception:
            pass
