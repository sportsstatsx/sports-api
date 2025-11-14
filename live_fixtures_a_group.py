import datetime as dt
from typing import Any, Dict, List, Optional

import requests

from db import execute
from live_fixtures_common import API_KEY, map_status_group


BASE_URL = "https://v3.football.api-sports.io/fixtures"


def _get_headers() -> Dict[str, str]:
    """
    Api-Football 요청 공통 헤더.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY 환경변수가 설정되어 있지 않습니다.")
    return {"x-apisports-key": API_KEY}


def fetch_fixtures_from_api(league_id: int, date_str: str) -> List[Dict[str, Any]]:
    """
    Api-Football v3 에서 특정 리그 + 날짜 경기를 가져온다.

    - endpoint: /fixtures
    - params:
        league: 리그 ID
        date:   YYYY-MM-DD (UTC 기준)
    """
    headers = _get_headers()
    params = {
        "league": league_id,
        "date": date_str,
    }

    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    results = data.get("response", []) or []

    # 혹시라도 다른 리그가 섞여 있을 경우를 대비해 한 번 더 필터
    fixtures: List[Dict[str, Any]] = []
    for item in results:
        league = item.get("league") or {}
        if int(league.get("id") or 0) != int(league_id):
            continue
        fixtures.append(item)

    return fixtures


def _extract_fixture_basic(fixture: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Api-Football fixture 응답에서 DB에 저장할 기본 필드만 뽑는다.
    """
    fixture_block = fixture.get("fixture") or {}
    league_block = fixture.get("league") or {}

    fixture_id = fixture_block.get("id")
    if fixture_id is None:
        return None

    # UTC ISO8601 문자열 그대로 저장 (예: "2025-11-15T13:00:00+00:00")
    date_utc = fixture_block.get("date")

    status_block = fixture_block.get("status") or {}
    status_short = status_block.get("short") or "NS"
    status_group = map_status_group(status_short)

    league_id = league_block.get("id")

    # 시즌은 보통 league.season 에 들어있지만,
    # update_live_fixtures 쪽에서 이미 계산해서 넘겨줄 수 있으므로
    # 여기서는 None 허용
    season = league_block.get("season")

    return {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season": season,
        "date_utc": date_utc,
        "status": status_short,
        "status_group": status_group,
    }


def upsert_fixture_row(
    fixture: Dict[str, Any],
    league_id: int,
    season: Optional[int],
) -> None:
    """
    A그룹용: fixtures 테이블 한 경기(한 row) upsert.

    - league_id / season 은 상위 로직(update_live_fixtures) 에서 계산해서 넘겨주는 값 사용
    - odds / odds_history 는 A그룹에서 다루지 않는다. (B그룹 전용)
    """
    basic = _extract_fixture_basic(fixture)
    if basic is None:
        return

    fixture_id = basic["fixture_id"]

    # 상위에서 전달한 league_id / season 이 우선
    league_id = league_id or basic["league_id"]
    if season is None:
        season = basic["season"]

    date_utc = basic["date_utc"]
    status_short = basic["status"]
    status_group = basic["status_group"]

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
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
