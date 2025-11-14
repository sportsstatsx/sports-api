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


# ─────────────────────────────────────────
# A그룹 나머지 스키마용 upsert 구현/틀
# ─────────────────────────────────────────


def upsert_match_row(
    fixture: Dict[str, Any],
    league_id: int,
    season: Optional[int],
) -> None:
    """
    A그룹: matches 테이블 upsert 구현.

    가정한 기본 컬럼(필요하면 나중에 스키마에 맞게 수정):
      - fixture_id (PK)
      - league_id
      - season
      - date_utc
      - status
      - status_group
      - home_team_id
      - away_team_id
      - goals_home
      - goals_away
      - winner   : 'HOME' / 'AWAY' / 'DRAW' / NULL
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

    teams_block = fixture.get("teams") or {}
    home_team = teams_block.get("home") or {}
    away_team = teams_block.get("away") or {}

    home_team_id = home_team.get("id")
    away_team_id = away_team.get("id")

    goals_block = fixture.get("goals") or {}
    goals_home = goals_block.get("home")
    goals_away = goals_block.get("away")

    # winner 판정 (Api-Football 구조 기반)
    home_winner_flag = home_team.get("winner")
    away_winner_flag = away_team.get("winner")

    winner: Optional[str]
    if home_winner_flag:
        winner = "HOME"
    elif away_winner_flag:
        winner = "AWAY"
    elif (
        goals_home is not None
        and goals_away is not None
        and goals_home == goals_away
        and status_group in ("FINISHED", "AFTER")
    ):
        winner = "DRAW"
    else:
        winner = None

    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_team_id,
            away_team_id,
            goals_home,
            goals_away,
            winner
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            goals_home   = EXCLUDED.goals_home,
            goals_away   = EXCLUDED.goals_away,
            winner       = EXCLUDED.winner
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            home_team_id,
            away_team_id,
            goals_home,
            goals_away,
            winner,
        ),
    )


def upsert_match_events(
    fixture_id: int,
    events: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_events 테이블 upsert 틀.

    TODO:
      - /fixtures/events endpoint 응답(events 리스트)을 받아서
        타임라인 형태로 match_events 에 적재.
    """
    # for ev in events:
    #     event_id = ...
    #     time_minute = ...
    #     team_id = ...
    #     player_id = ...
    #     type = ...
    #     detail = ...
    #
    #     execute(
    #         "INSERT INTO match_events (...) VALUES (...) "
    #         "ON CONFLICT (...) DO UPDATE SET ...",
    #         (...,),
    #     )
    return


def upsert_match_events_raw(
    fixture_id: int,
    events: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_events_raw 테이블 upsert 틀.

    TODO:
      - Api-Football 이벤트 원본 JSON 을 거의 그대로 저장해서
        나중에 재가공/디버깅에 사용할 수 있게 한다.
    """
    # import json
    #
    # execute(
    #     "INSERT INTO match_events_raw (fixture_id, payload_json, created_at) "
    #     "VALUES (%s, %s, NOW())",
    #     (fixture_id, json.dumps(events)),
    # )
    return


def upsert_match_lineups(
    fixture_id: int,
    lineups: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_lineups 테이블 upsert 틀.

    TODO:
      - /fixtures/lineups endpoint 응답(lineups 리스트)을 받아서
        팀별 선발/벤치/포메이션 정보를 match_lineups 에 저장.
    """
    # for lineup in lineups:
    #     team_id = ...
    #     formation = ...
    #     coach_id = ...
    #     ...
    #
    #     execute(
    #         "INSERT INTO match_lineups (...) VALUES (...) "
    #         "ON CONFLICT (...) DO UPDATE SET ...",
    #         (...,),
    #     )
    return


def upsert_match_team_stats(
    fixture_id: int,
    stats: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_team_stats 테이블 upsert 틀.

    TODO:
      - /fixtures/statistics endpoint 응답(team 통계 리스트)을 받아서
        팀 단위 슈팅/점유율/코너킥 등 누적 통계를 저장.
    """
    # for s in stats:
    #     team_id = ...
    #     shots_on = ...
    #     possession = ...
    #     ...
    #
    #     execute(
    #         "INSERT INTO match_team_stats (...) VALUES (...) "
    #         "ON CONFLICT (...) DO UPDATE SET ...",
    #         (...,),
    #     )
    return


def upsert_match_player_stats(
    fixture_id: int,
    players_stats: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_player_stats 테이블 upsert 틀.

    TODO:
      - /fixtures/players endpoint 응답(선수별 스탯 리스트)을 받아서
        선수 단위 슈팅/패스/카드/평점 등을 저장.
    """
    # for row in players_stats:
    #     player_id = ...
    #     team_id = ...
    #     minutes = ...
    #     rating = ...
    #     ...
    #
    #     execute(
    #         "INSERT INTO match_player_stats (...) VALUES (...) "
    #         "ON CONFLICT (...) DO UPDATE SET ...",
    #         (...,),
    #     )
    return


def upsert_predictions(
    fixture_id: int,
    prediction: Dict[str, Any],
) -> None:
    """
    A그룹: predictions 테이블 upsert 틀.

    TODO:
      - /predictions endpoint 응답을 받아서
        승무패 확률, 스코어 예측, O/U 예측 등을 저장.
    """
    # home_win_prob = ...
    # draw_prob = ...
    # away_win_prob = ...
    # ...
    #
    # execute(
    #     "INSERT INTO predictions (...) VALUES (...) "
    #     "ON CONFLICT (fixture_id) DO UPDATE SET ...",
    #     (...,),
    # )
    return
