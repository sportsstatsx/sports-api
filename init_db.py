# init_db.py
from db import execute, fetch_one

DDL = """
CREATE TABLE IF NOT EXISTS leagues (
  id        INT PRIMARY KEY,
  name      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fixtures (
  id           SERIAL PRIMARY KEY,
  fixture_id   TEXT UNIQUE,
  league_id    INT REFERENCES leagues(id),
  date         DATE NOT NULL,
  kickoff_utc  TIMESTAMPTZ NOT NULL,
  home         TEXT NOT NULL,
  away         TEXT NOT NULL,
  status       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fixtures_league_date ON fixtures(league_id, date);
"""

def seed():
    # 프리미어리그(39)만 샘플로
    if not fetch_one("SELECT 1 FROM leagues WHERE id = %s", (39,)):
        execute("INSERT INTO leagues (id, name) VALUES (%s, %s)", (39, "Premier League"))

    # 샘플 경기 2건 (없을 때만)
    if not fetch_one("SELECT 1 FROM fixtures WHERE fixture_id = %s", ("FX12345",)):
        execute(
            """
            INSERT INTO fixtures (fixture_id, league_id, date, kickoff_utc, home, away, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            ("FX12345", 39, "2025-11-12", "2025-11-12T19:00:00Z", "Team A", "Team B", "scheduled"),
        )
    if not fetch_one("SELECT 1 FROM fixtures WHERE fixture_id = %s", ("FX12346",)):
        execute(
            """
            INSERT INTO fixtures (fixture_id, league_id, date, kickoff_utc, home, away, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            ("FX12346", 39, "2025-11-12", "2025-11-12T21:00:00Z", "Team C", "Team D", "scheduled"),
        )

def main():
    # 여러 문장을 한 번에 실행
    for stmt in filter(None, DDL.split(";")):
        s = stmt.strip()
        if s:
            execute(s + ";")
    seed()
    print("✅ DB initialized and seeded.")

if __name__ == "__main__":
    main()
