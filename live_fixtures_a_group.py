import datetime as dt
from typing import Any, Dict, List

import requests

from db import execute
from live_fixtures_common import API_KEY, map_status_group


def fetch_fixtures_from_api(league_id: int, date_str: str) -> List[Dict[str, Any]]:
    """
    Api-Football v3 에서 특정 리그 + 날짜 경기를 가져온다.
    /fixtures?league={league_id}&date={YYYY-MM-DD}
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/fixtures"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "date": date_str,
        "timezone": "UTC",
    }

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    return data.get("response", [])


def upsert_fixture_row(row: Dict[str, Any]) -> None:
    """
    한 경기 정보를 matches/fixtures 테이블에 upsert.
    (라이브 핵심: 스코어/상태/킥오프 시간)
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
    date_utc = fixture.get("date")

    status_short = (fixture.get("status") or {}).get("short", "")
    status_group = map_status_group(status_short)

    home_team = teams.get("home") or {}
    away_team = teams.get("away") or {}

    home_id = home_team.get("id")
    away_id = away_team.get("id")

    home_ft = goals.get("home")
    away_ft = goals.get("away")

    # matches
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

    # fixtures
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
