# update_live_fixtures.py
#
# 오늘(또는 지정한 날짜)의 경기들을 Api-Football에서 가져와
# Postgres matches 테이블에 upsert 하는 스크립트.
#
# 사용 예:
#   python update_live_fixtures.py            # 오늘 날짜 기준
#   python update_live_fixtures.py 2025-11-14 # 특정 날짜
#
# 필요한 환경변수:
#   APIFOOTBALL_KEY   : Api-Football API 키
#   LIVE_LEAGUES      : 라이브 갱신할 리그 ID 목록 (예: "39,140,78")
#
# 주의:
#   - matches.fixture_id 에 UNIQUE 제약이 있다고 가정하고
#     ON CONFLICT (fixture_id) DO UPDATE 를 사용한다.
#   - main.py 의 SELECT 컬럼 구조와 최대한 맞춰서 upsert 한다.

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

import requests

from db import get_db_conn

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY")
APIFOOTBALL_BASE_URL = os.getenv(
    "APIFOOTBALL_BASE_URL", "https://v3.football.api-sports.io"
)

# LIVE_LEAGUES="39,140,78" 형태
LIVE_LEAGUES_ENV = os.getenv("LIVE_LEAGUES", "")


def parse_live_leagues() -> List[int]:
    if not LIVE_LEAGUES_ENV.strip():
        return []
    out = []
    for part in LIVE_LEAGUES_ENV.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            logging.warning("잘못된 리그 ID 무시: %s", part)
    return out


def fetch_fixtures_from_apifootball(league_id: int, date_str: str) -> List[Dict[str, Any]]:
    """
    Api-Football에서 특정 리그 + 날짜의 fixtures를 가져온다.

    date_str: "YYYY-MM-DD"
    """
    if not APIFOOTBALL_KEY:
        raise RuntimeError("APIFOOTBALL_KEY 환경변수가 설정되어 있지 않습니다.")

    url = f"{APIFOOTBALL_BASE_URL}/fixtures"
    headers = {
        "x-apisports-key": APIFOOTBALL_KEY,
    }
    params = {
        "league": league_id,
        "date": date_str,
    }

    logging.info("Api-Football 호출: league=%s date=%s", league_id, date_str)
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Api-Football v3 응답 구조:
    # { "response": [ { "fixture": {...}, "league": {...}, "teams": {...}, "goals": {...}, ... } ], ... }
    fixtures = data.get("response", [])
    logging.info(" → Api-Football 응답 경기 수: %d", len(fixtures))
    return fixtures


def map_apifoot_fixture_to_row(f: Dict[str, Any]) -> Dict[str, Any]:
    """
    Api-Football fixture 한 건을 DB matches 테이블 컬럼 구조에 맞춰 매핑한다.
    main.py 의 SELECT 컬럼과 최대한 맞춘다.

    SELECT 에 나오는 컬럼:
      m.fixture_id,
      m.league_id,
      l.name AS league_name,   -- ← insert 시에는 leagues 테이블에서 관리
      m.season,
      m.date_utc,
      m.status,
      m.status_group,
      m.home_id,
      m.away_id,
      m.home_ft,
      m.away_ft
    """

    fixture = f.get("fixture", {})
    league = f.get("league", {})
    teams = f.get("teams", {})
    goals = f.get("goals", {})

    fixture_id = fixture.get("id")
    league_id = league.get("id")
    season = league.get("season")

    # fixture.date: ISO8601 문자열 (예: "2025-11-14T20:00:00+00:00")
    date_utc = fixture.get("date")

    # status & statusGroup 매핑
    status_short = fixture.get("status", {}).get("short")  # "NS", "1H", "HT", "2H", "FT" 등
    status_long = fixture.get("status", {}).get("long")    # "Not Started", "First Half, Kick Off" ...

    # status_group 은 main.py 에서 사용하는 그룹값 (UPCOMING / LIVE / FT) 으로 매핑
    status_group = "UPCOMING"
    if status_short in ("NS", "PST", "CANC", "TBD"):
        status_group = "UPCOMING"
    elif status_short in ("1H", "2H", "HT", "ET", "BT", "LIVE"):
        status_group = "INPLAY"
    elif status_short in ("FT", "AET", "PEN"):
        status_group = "FINISHED"

    home_team = teams.get("home", {})
    away_team = teams.get("away", {})

    home_id = home_team.get("id")
    away_id = away_team.get("id")

    # goals: { "home": 1, "away": 0 }  (FT 기준)
    home_ft = goals.get("home")
    away_ft = goals.get("away")

    row = {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season": season,
        "date_utc": date_utc,
        "status": status_long or status_short,
        "status_group": status_group,
        "home_id": home_id,
        "away_id": away_id,
        "home_ft": home_ft,
        "away_ft": away_ft,
    }
    return row


def upsert_fixtures(rows: List[Dict[str, Any]]):
    """
    DB matches 테이블에 upsert.
    fixture_id 에 UNIQUE 제약이 있다고 가정.
    """
    if not rows:
        return

    sql = """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft
        )
        VALUES (
            %(fixture_id)s,
            %(league_id)s,
            %(season)s,
            %(date_utc)s,
            %(status)s,
            %(status_group)s,
            %(home_id)s,
            %(away_id)s,
            %(home_ft)s,
            %(away_ft)s
        )
        ON CONFLICT (fixture_id)
        DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id      = EXCLUDED.home_id,
            away_id      = EXCLUDED.away_id,
            home_ft      = EXCLUDED.home_ft,
            away_ft      = EXCLUDED.away_ft
    """

    conn = get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        logging.info("DB upsert 완료: %d 경기", len(rows))
    finally:
        conn.close()


def main():
    if len(sys.argv) >= 2:
        date_str = sys.argv[1]
    else:
        # 기본값: 오늘 (UTC 기준 또는 운영자가 원하는 기준으로)
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    leagues = parse_live_leagues()
    if not leagues:
        logging.error("LIVE_LEAGUES 환경변수가 비어있습니다. 예: LIVE_LEAGUES=39,140,78")
        sys.exit(1)

    logging.info("라이브 업데이트 대상 날짜: %s", date_str)
    logging.info("라이브 업데이트 대상 리그: %s", leagues)

    total = 0
    for lid in leagues:
        try:
            fixtures = fetch_fixtures_from_apifootball(lid, date_str)
            mapped = [map_apifoot_fixture_to_row(f) for f in fixtures]
            upsert_fixtures(mapped)
            total += len(mapped)
        except Exception as e:
            logging.exception("리그 %s 업데이트 중 오류: %s", lid, e)
            # 계속 다음 리그 처리

        # API 과금/레이트리밋 보호용 딜레이 (필요하면 조절)
        time.sleep(1.0)

    logging.info("모든 리그 업데이트 완료. 총 경기 수: %d", total)


if __name__ == "__main__":
    main()
