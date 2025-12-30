import datetime as dt
import json
from typing import Any, Dict, List, Optional

import requests

from db import execute
from live_fixtures_common import (
    API_KEY,
    map_status_group,
    now_utc,
    infer_season_for_league_and_date,
)


BASE_URL = "https://v3.football.api-sports.io/fixtures"
EVENTS_URL = "https://v3.football.api-sports.io/fixtures/events"
LINEUPS_URL = "https://v3.football.api-sports.io/fixtures/lineups"
STATS_URL = "https://v3.football.api-sports.io/fixtures/statistics"
PLAYERS_URL = "https://v3.football.api-sports.io/fixtures/players"


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
        season: infer_season_for_league_and_date() 로 계산한 시즌
    """
    headers = _get_headers()

    # ✅ 시즌 추론: DB 기반 + 연도 기반 fallback
    season = infer_season_for_league_and_date(league_id, date_str)

    params = {
        "league": league_id,
        "date": date_str,
        "season": season,
    }

    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", 0) or 0
    if results == 0:
        # 디버깅용: 앞으로 문제 생기면 errors도 같이 찍어보자
        errors = data.get("errors")
        if errors:
            print(
                f"[WARN] fixtures league={league_id}, date={date_str}, "
                f"season={season} → results=0, errors={errors}"
            )
        return []

    rows = data.get("response", []) or []

    # 혹시라도 다른 리그가 섞여 있을 경우를 대비해 한 번 더 필터
    fixtures: List[Dict[str, Any]] = []
    for item in rows:
        league = item.get("league") or {}
        if int(league.get("id") or 0) != int(league_id):
            continue
        fixtures.append(item)

    return fixtures


def fetch_events_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    """
    특정 경기(fixture_id)에 대한 이벤트 리스트를 Api-Football에서 가져온다.

    - endpoint: /fixtures/events
    - params:
        fixture: fixture_id
    """
    headers = _get_headers()
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(EVENTS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    results = data.get("response", []) or []

    events: List[Dict[str, Any]] = []
    for ev in results:
        if isinstance(ev, dict):
            events.append(ev)

    return events


def fetch_lineups_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    """
    특정 경기(fixture_id)에 대한 라인업 리스트를 Api-Football에서 가져온다.

    - endpoint: /fixtures/lineups
    - params:
        fixture: fixture_id

    일반적으로 팀당 1개씩(홈/원정) 라인업이 들어온다.
    """
    headers = _get_headers()
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(LINEUPS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    results = data.get("response", []) or []

    lineups: List[Dict[str, Any]] = []
    for row in results:
        if isinstance(row, dict):
            lineups.append(row)

    return lineups


def fetch_team_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    """
    특정 경기(fixture_id)에 대한 팀 통계 리스트를 Api-Football에서 가져온다.

    - endpoint: /fixtures/statistics
    - params:
        fixture: fixture_id

    응답 예시(대략):
      response: [
        {
          "team": {"id": 33, ...},
          "statistics": [
            {"type": "Shots on Goal", "value": 5},
            ...
          ]
        },
        { ... 원정 팀 ... }
      ]
    """
    headers = _get_headers()
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(STATS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    results = data.get("response", []) or []

    stats: List[Dict[str, Any]] = []
    for row in results:
        if isinstance(row, dict):
            stats.append(row)

    return stats


def fetch_player_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    """
    특정 경기(fixture_id)에 대한 선수별 스탯 리스트를 Api-Football에서 가져온다.

    - endpoint: /fixtures/players
    - params:
        fixture: fixture_id

    응답 예시(대략):
      response: [
        {
          "team": {...},
          "players": [
            {
              "player": {...},
              "statistics": [...]
            },
            ...
          ]
        },
        ...
      ]
    """
    headers = _get_headers()
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(PLAYERS_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()

    data = resp.json()
    results = data.get("response", []) or []

    players_stats: List[Dict[str, Any]] = []
    for row in results:
        if isinstance(row, dict):
            players_stats.append(row)

    return players_stats


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
    # 🔽🔽🔽 여기부터 추가 (기존 코드 절대 삭제 X, 그대로 두고 "덧붙이기"만)
    # Api-Football 특수 상태 보정
    # - ABD, AWD, CANC, WO : 기권/몰수/취소 → 실질적으로는 "끝난 경기" 취급
    # - PST, SUSP          : 연기/중단      → 여전히 "다가올 경기"지만, status 로 지연 표기
    if status_short in ("ABD", "AWD", "CANC", "WO"):
        # 앱에서는 status == 'ABD' 보고 "기권", "몰수승" 이런 텍스트로 표시하고
        # 그룹은 FINISHED 로 둬서 라이브/대기중 리스트에서 빠지게
        status_group = "FINISHED"
    elif status_short in ("PST", "SUSP"):
        # 지연/연기된 경기 → 시간은 지났어도 "UPCOMING" 그룹에 두고,
        # 앱에서 status 값으로 "경기 지연" 같은 텍스트 처리
        status_group = "UPCOMING"
    # 🔼🔼🔼 여기까지 추가

    # ✅ 실제 진행 시간(분): Api-Football status.elapsed
    elapsed = status_block.get("elapsed")

    league_id = league_block.get("id")
    season = league_block.get("season")

    return {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season": season,
        "date_utc": date_utc,
        "status": status_short,
        "status_group": status_group,
        # ✅ matches.elapsed 로 저장할 값
        "elapsed": elapsed,
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
# A그룹 나머지 스키마용 upsert 구현
# ─────────────────────────────────────────


def upsert_match_row(
    fixture: Dict[str, Any],
    league_id: int,
    season: Optional[int],
) -> None:
    """
    A그룹: matches 테이블 upsert 구현.

    matches 스키마:
      fixture_id   INTEGER PK
      league_id    INTEGER NOT NULL
      season       INTEGER NOT NULL
      date_utc     TEXT    NOT NULL
      status       TEXT    NOT NULL
      status_group TEXT    NOT NULL
      elapsed      INTEGER
      home_id      INTEGER NOT NULL
      away_id      INTEGER NOT NULL
      home_ft      INTEGER
      away_ft      INTEGER
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
    # ✅ 위에서 뽑은 elapsed (없으면 None → DB에서는 NULL)
    elapsed = basic.get("elapsed")

    teams_block = fixture.get("teams") or {}
    home_team = teams_block.get("home") or {}
    away_team = teams_block.get("away") or {}

    home_team_id = home_team.get("id")
    away_team_id = away_team.get("id")

    goals_block = fixture.get("goals") or {}
    goals_home = goals_block.get("home")
    goals_away = goals_block.get("away")

    fixture_block = fixture.get("fixture") or {}
    league_block = fixture.get("league") or {}
    status_block = fixture_block.get("status") or {}
    venue_block = fixture_block.get("venue") or {}
    score_block = fixture.get("score") or {}
    ht_block = score_block.get("halftime") or {}

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")

    status_long = status_block.get("long")
    status_elapsed = status_block.get("elapsed")
    status_extra = status_block.get("extra")

    home_ht = ht_block.get("home")
    away_ht = ht_block.get("away")

    venue_id = venue_block.get("id")
    venue_name = venue_block.get("name")
    venue_city = venue_block.get("city")

    league_round = league_block.get("round")

    # 안전장치 (팀 id 없으면 저장 스킵)
    if home_team_id is None or away_team_id is None:
        return

    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,

            status,
            status_group,
            elapsed,

            home_id,
            away_id,
            home_ft,
            away_ft,

            home_ht,
            away_ht,

            referee,
            fixture_timezone,
            fixture_timestamp,

            status_short,
            status_long,
            status_elapsed,
            status_extra,

            venue_id,
            venue_name,
            venue_city,

            league_round
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s
        )
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id         = EXCLUDED.league_id,
            season            = EXCLUDED.season,
            date_utc          = EXCLUDED.date_utc,

            status            = EXCLUDED.status,
            status_group      = EXCLUDED.status_group,
            elapsed           = EXCLUDED.elapsed,

            home_id           = EXCLUDED.home_id,
            away_id           = EXCLUDED.away_id,
            home_ft           = EXCLUDED.home_ft,
            away_ft           = EXCLUDED.away_ft,

            home_ht           = EXCLUDED.home_ht,
            away_ht           = EXCLUDED.away_ht,

            referee           = EXCLUDED.referee,
            fixture_timezone  = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,

            status_short      = EXCLUDED.status_short,
            status_long       = EXCLUDED.status_long,
            status_elapsed    = EXCLUDED.status_elapsed,
            status_extra      = EXCLUDED.status_extra,

            venue_id          = EXCLUDED.venue_id,
            venue_name        = EXCLUDED.venue_name,
            venue_city        = EXCLUDED.venue_city,

            league_round      = EXCLUDED.league_round
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,

            status_short,      # matches.status (기존 유지)
            status_group,
            elapsed,

            home_team_id,
            away_team_id,
            goals_home,
            goals_away,

            home_ht,
            away_ht,

            referee,
            fixture_timezone,
            fixture_timestamp,

            status_short,      # matches.status_short (신규)
            status_long,
            status_elapsed,
            status_extra,

            venue_id,
            venue_name,
            venue_city,

            league_round,
        ),
    )



def upsert_match_events(
    fixture_id: int,
    events: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_events 테이블 upsert 구현.

    match_events 스키마:
      id               BIGSERIAL PK
      fixture_id       INTEGER NOT NULL
      team_id          INTEGER
      player_id        INTEGER
      type             TEXT    NOT NULL
      detail           TEXT
      minute           INTEGER NOT NULL
      extra            INTEGER DEFAULT 0
      assist_player_id INTEGER
      assist_name      TEXT
      player_in_id     INTEGER
      player_in_name   TEXT
    """
    # 기존 이벤트 삭제 후 새로 입력(단순/안전)
    execute(
        "DELETE FROM match_events WHERE fixture_id = %s",
        (fixture_id,),
    )

    for ev in events:
        if not isinstance(ev, dict):
            continue

        time_block = ev.get("time") or {}
        minute = time_block.get("elapsed")
        if minute is None:
            # 분 정보 없으면 저장하지 않음
            continue
        extra = time_block.get("extra") or 0

        team_block = ev.get("team") or {}
        team_id = team_block.get("id")

        player_block = ev.get("player") or {}
        player_id = player_block.get("id")

        assist_block = ev.get("assist") or {}
        assist_player_id = assist_block.get("id")
        assist_name = assist_block.get("name")

        type_ = ev.get("type") or ""
        detail = ev.get("detail")

        player_in_id: Optional[int] = None
        player_in_name: Optional[str] = None
        if type_.lower() == "subst":
            player_in_id = assist_player_id
            player_in_name = assist_name

        execute(
            """
            INSERT INTO match_events (
                fixture_id,
                team_id,
                player_id,
                type,
                detail,
                minute,
                extra,
                assist_player_id,
                assist_name,
                player_in_id,
                player_in_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fixture_id,
                team_id,
                player_id,
                type_,
                detail,
                minute,
                extra,
                assist_player_id,
                assist_name,
                player_in_id,
                player_in_name,
            ),
        )


def upsert_match_events_raw(
    fixture_id: int,
    events: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_events_raw 테이블 upsert 구현.

    match_events_raw 스키마:
      fixture_id INTEGER PK
      data_json  TEXT NOT NULL
    """
    execute(
        """
        INSERT INTO match_events_raw (fixture_id, data_json)
        VALUES (%s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json = EXCLUDED.data_json
        """,
        (
            fixture_id,
            json.dumps(events),
        ),
    )

def upsert_match_fixtures_raw(fixture_id: int, fixture_obj: dict) -> None:
    """
    fixtures 원본(단일 fixture response object)을 match_fixtures_raw에 저장/갱신한다.
    - B안: HT/기타 정보는 나중에 필요할 때 raw에서 꺼내 쓰거나, 별도 컬럼에 미러링할 수 있게 원본을 보관.
    """
    import json

    # 최대한 그대로 저장(UTF-8 보존)
    raw = json.dumps(fixture_obj, ensure_ascii=False)

    execute(
        """
        INSERT INTO match_fixtures_raw (fixture_id, data_json, fetched_at, updated_at)
        VALUES (%s, %s, now(), now())
        ON CONFLICT (fixture_id) DO UPDATE
        SET data_json = EXCLUDED.data_json,
            fetched_at = now(),
            updated_at = now()
        """,
        (fixture_id, raw),
    )



def upsert_match_lineups(
    fixture_id: int,
    lineups: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_lineups 테이블 upsert 구현.

    match_lineups 스키마:
      fixture_id  INTEGER NOT NULL
      team_id     INTEGER NOT NULL
      data_json   TEXT    NOT NULL
      updated_utc TEXT
    """
    # 한 경기 라인업 전체를 다시 덮어쓴다.
    execute(
        "DELETE FROM match_lineups WHERE fixture_id = %s",
        (fixture_id,),
    )

    updated_utc = now_utc().isoformat()

    for row in lineups:
        if not isinstance(row, dict):
            continue

        team_block = row.get("team") or {}
        team_id = team_block.get("id")
        if team_id is None:
            continue

        execute(
            """
            INSERT INTO match_lineups (fixture_id, team_id, data_json, updated_utc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                data_json   = EXCLUDED.data_json,
                updated_utc = EXCLUDED.updated_utc
            """,
            (
                fixture_id,
                team_id,
                json.dumps(row),
                updated_utc,
            ),
        )


def upsert_match_team_stats(
    fixture_id: int,
    stats: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_team_stats 테이블 upsert 구현.

    match_team_stats 스키마:
      fixture_id INTEGER NOT NULL
      team_id    INTEGER NOT NULL
      name       TEXT    NOT NULL
      value      TEXT
    """
    # 한 경기 팀 통계를 통째로 다시 덮어쓴다.
    execute(
        "DELETE FROM match_team_stats WHERE fixture_id = %s",
        (fixture_id,),
    )

    for row in stats:
        if not isinstance(row, dict):
            continue

        team_block = row.get("team") or {}
        team_id = team_block.get("id")
        if team_id is None:
            continue

        stat_list = row.get("statistics") or []
        for s in stat_list:
            if not isinstance(s, dict):
                continue
            name = s.get("type")
            if not name:
                continue
            value = s.get("value")
            # NULL 도 허용되지만, 문자열로 캐스팅해서 저장해도 무방
            value_str = None
            if value is not None:
                value_str = str(value)

            execute(
                """
                INSERT INTO match_team_stats (fixture_id, team_id, name, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fixture_id, team_id, name) DO UPDATE SET
                    value = EXCLUDED.value
                """,
                (
                    fixture_id,
                    team_id,
                    name,
                    value_str,
                ),
            )


def upsert_match_player_stats(
    fixture_id: int,
    players_stats: List[Dict[str, Any]],
) -> None:
    """
    A그룹: match_player_stats 테이블 upsert 구현.

    match_player_stats 스키마:
      fixture_id INTEGER NOT NULL
      player_id  INTEGER NOT NULL
      data_json  TEXT    NOT NULL
    """
    # 한 경기 선수 스탯을 통째로 다시 덮어쓴다.
    execute(
        "DELETE FROM match_player_stats WHERE fixture_id = %s",
        (fixture_id,),
    )

    for team_block in players_stats:
        if not isinstance(team_block, dict):
            continue

        players_list = team_block.get("players") or []
        for p in players_list:
            if not isinstance(p, dict):
                continue
            player_info = p.get("player") or {}
            player_id = player_info.get("id")
            if player_id is None:
                continue

            execute(
                """
                INSERT INTO match_player_stats (fixture_id, player_id, data_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (fixture_id, player_id) DO UPDATE SET
                    data_json = EXCLUDED.data_json
                """,
                (
                    fixture_id,
                    player_id,
                    json.dumps(p),
                ),
            )


def upsert_predictions(
    fixture_id: int,
    prediction: Dict[str, Any],
) -> None:
    """
    A그룹: predictions 테이블 upsert 틀.

    predictions 스키마:
      fixture_id INTEGER PK
      data_json  TEXT NOT NULL
    """
    # TODO: /predictions 응답 구조에 맞춰 구현 예정
    return
