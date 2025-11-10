# init_db.py
import os
import sys
from psycopg_pool import ConnectionPool
import psycopg

# ─────────────────────────────────────────────────────
# 1) ENV
# ─────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL environment variable is not set.", file=sys.stderr)
    sys.exit(1)

# ─────────────────────────────────────────────────────
# 2) POOL
# ─────────────────────────────────────────────────────
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=3,
    timeout=10,
)

# ─────────────────────────────────────────────────────
# 3) DDL (테이블)
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

CREATE_STANDINGS_SQL = """
CREATE TABLE IF NOT EXISTS standings (
    id         BIGSERIAL PRIMARY KEY,
    league_id  INTEGER NOT NULL,
    season     TEXT    NOT NULL, -- 예: '2025-26'
    team_name  TEXT    NOT NULL,
    rank       INTEGER NOT NULL,
    played     INTEGER NOT NULL,
    win        INTEGER NOT NULL,
    draw       INTEGER NOT NULL,
    loss       INTEGER NOT NULL,
    gf         INTEGER NOT NULL,
    ga         INTEGER NOT NULL,
    gd         INTEGER NOT NULL,
    points     INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_standings UNIQUE (league_id, season, team_name)
);
"""

# ─────────────────────────────────────────────────────
# 4) DDL (트리거 함수 + 트리거)
#   - 어떤 테이블이든 updated_at 컬럼이 있으면 UPDATE 시 NOW()로 갱신
# ─────────────────────────────────────────────────────
CREATE_TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

CREATE_TRIGGERS_SQL = [
    # teams
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_teams_updated_at'
      ) THEN
        CREATE TRIGGER trg_teams_updated_at
        BEFORE UPDATE ON teams
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
      END IF;
    END$$;
    """,
    # fixtures
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_fixtures_updated_at'
      ) THEN
        CREATE TRIGGER trg_fixtures_updated_at
        BEFORE UPDATE ON fixtures
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
      END IF;
    END$$;
    """,
    # standings
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_standings_updated_at'
      ) THEN
        CREATE TRIGGER trg_standings_updated_at
        BEFORE UPDATE ON standings
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
      END IF;
    END$$;
    """,
]

# ─────────────────────────────────────────────────────
# 5) 인덱스
# ─────────────────────────────────────────────────────
CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_fixtures_league_date ON fixtures (league_id, match_date);",
    "CREATE INDEX IF NOT EXISTS idx_fixtures_updated_at ON fixtures (updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_teams_league_name ON teams (league_id, name);",
    "CREATE INDEX IF NOT EXISTS idx_standings_league_season_rank ON standings (league_id, season, rank);",
]

# ─────────────────────────────────────────────────────
# 6) SEED
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

SEED_STANDINGS_SQL = """
INSERT INTO standings
(league_id, season, team_name, rank, played, win, draw, loss, gf, ga, gd, points)
VALUES
(39, '2025-26', 'Manchester City', 1, 12, 9, 2, 1, 28, 10, 18, 29),
(39, '2025-26', 'Arsenal',         2, 12, 9, 1, 2, 26, 12, 14, 28),
(39, '2025-26', 'Liverpool',       3, 12, 8, 3, 1, 27, 14, 13, 27),
(39, '2025-26', 'Chelsea',         8, 12, 5, 2, 5, 18, 16,  2, 17)
ON CONFLICT ON CONSTRAINT uq_standings DO NOTHING;
"""

# ─────────────────────────────────────────────────────
# 7) 마이그레이션: fixtures.date → fixtures.match_date
# ─────────────────────────────────────────────────────
def normalize_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fixtures'
        """)
        cols = {r[0] for r in cur.fetchall()}
        if "date" in cols and "match_date" not in cols:
            print("Renaming column fixtures.date -> fixtures.match_date ...")
            cur.execute("ALTER TABLE fixtures RENAME COLUMN date TO match_date;")
            print("Renamed successfully.")

# ─────────────────────────────────────────────────────
# 8) INIT
# ─────────────────────────────────────────────────────
def init_db():
    print("Connecting to Postgres...")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            print("Creating tables (teams, fixtures, standings) if not exists...")
            cur.execute(CREATE_TEAMS_SQL)
            cur.execute(CREATE_FIXTURES_SQL)
            cur.execute(CREATE_STANDINGS_SQL)

        # 마이그레이션 (필요 시)
        normalize_schema(conn)

        # 트리거 함수 및 트리거
        with conn.cursor() as cur:
            print("Creating trigger function set_updated_at()...")
            cur.execute(CREATE_TRIGGER_FUNCTION_SQL)
            print("Ensuring updated_at triggers exist on tables...")
            for sql in CREATE_TRIGGERS_SQL:
                cur.execute(sql)

        # 인덱스
        with conn.cursor() as cur:
            print("Creating indexes (if not exists)...")
            for sql in CREATE_INDEXES_SQL:
                cur.execute(sql)

        # 시드
        with conn.cursor() as cur:
            print("Seeding teams...")
            cur.execute(SEED_TEAMS_SQL)
            print("Seeding fixtures...")
            cur.execute(SEED_FIXTURES_SQL)
            print("Seeding standings...")
            cur.execute(SEED_STANDINGS_SQL)

    print("✅ DB initialized and seeded.")

# ─────────────────────────────────────────────────────
# 9) MAIN
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        init_db()
    finally:
        try:
            pool.close()
        except Exception:
            pass
