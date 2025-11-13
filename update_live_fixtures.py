import os
import sys
import time
import datetime as dt
from typing import List

import requests

from db import fetch_all, execute


API_KEY = os.environ.get("APIFOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")


def parse_live_leagues(env_val: str) -> List[int]:
    ids = []
    for part in env_val.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


def get_target_date() -> str:
    """
    CLI 인자에 YYYY-MM-DD 가 들어오면 그 날짜,
    없으면 오늘(UTC)의 날짜 문자열을 반환.
    """
    if len(sys.argv) >= 2:
        return sys.argv[1]
    return dt.datetime.utcnow().strftime("%Y-%m-%d")


def map_status_group(short_code: str) -> str:
    """
    Api-Football status.short 코드를 우리 DB의 status_group 으로 변환.
    """
    s = (short_code or "").upper()

    inplay_codes = {
        "1H", "2H", "ET", "BT", "P", "LIVE", "INPLAY", "HT"
    }
    finished_codes = {
        "FT", "AET", "PEN"
    }
    upcoming_codes = {
        "NS", "TBD", "PST", "CANC", "SUSP", "INT"
    }

    if s in inplay_codes:
        return "INPLAY"
    if s in finished_codes:
        return "FINISHED"
    if s in upcoming_codes:
        return "UPCOMING"

    # 모르는 건 일단 UPCOMING 으로
    return "UPCOMING"


def fetch_fixtures_from_api(league_id: int, date_str: str):
    """
    Api-Football v3 에서 특정 리그 + 날짜 경기를 가져온다.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/fixtures"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "date": date_str,   # YYYY-MM-DD
        "timezone": "UTC",
    }

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Api-Football 응답 형식: {"response": [ ... ]}
    return data.get("response", [])


def upsert_fixture_row(row):
    """
    Api-Football 한 경기 정보를 Postgres matches/fixtures 테이블에 upsert.
    """
    fixture = row.get("fixture", {})
    league = row.get("league", {})
    teams = row.get("teams", {})
    goals = row.get("goals", {})

    fixture_id = fixture.get("id")
    if fixture_id is None:
        return

    league_id = league.get("id")
    season = league.get("season")
    date_utc = fixture.get("date")  # ISO8601, TIMESTAMPTZ 로 캐스팅됨

    status_short = (fixture.get("status") or {}).get("short", "")
    status_group = map_status_group(status_short)

    home_team = teams.get("home") or {}
    away_team = teams.get("away") or {}

    home_id = home_team.get("id")
    away_id = away_team.get("id")

    home_ft = goals.get("home")
    away_ft = goals.get("away")

    # matches 테이블 upsert
    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc,
            status, status_group,
            home_id, away_id,
            home_ft, away_ft
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id      = EXCLUDED.home_id,
            away_id      = EXCLUDED.away_id,
            home_ft      = EXCLUDED.home_ft,
            away_ft      = EXCLUDED.away_ft
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
        ),
    )

    # fixtures 테이블 upsert (요약용)
    execute(
        """
        INSERT INTO fixtures (
            fixture_id, league_id, season, date_utc,
            status, status_group
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
        ),
    )


def main():
    target_date = get_target_date()
    live_leagues = parse_live_leagues(LIVE_LEAGUES_ENV)

    if not live_leagues:
        print("LIVE_LEAGUES env 에 리그 ID 가 없습니다. 종료.", file=sys.stderr)
        return

    print(f"[update_live_fixtures] date={target_date}, leagues={live_leagues}")

    total_updated = 0

    for lid in live_leagues:
        try:
            print(f"  - league {lid}: Api-Football 호출 중...")
            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"    응답 경기 수: {len(fixtures)}")

            for row in fixtures:
                upsert_fixture_row(row)
                total_updated += 1

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[update_live_fixtures] 완료. 총 업데이트/삽입 경기 수 = {total_updated}")


if __name__ == "__main__":
    main()
