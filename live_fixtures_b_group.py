import sys
import json
from typing import Any, Dict, List, Optional

import requests

from db import execute, fetch_all
from live_fixtures_common import (
    API_KEY,
    now_utc,
    resolve_league_season_for_date,
)


# ─────────────────────────────────────
#  공통 유틸: 리그의 팀 리스트 가져오기
# ─────────────────────────────────────

def _get_team_ids_for_league_season(
    league_id: int,
    season: int,
) -> List[int]:
    """
    standings → matches 순으로 team_id 목록을 가져온다.

    1) standings 에 데이터가 있으면 거기서 DISTINCT team_id
    2) 없으면 matches 에서 home/away 합쳐서 DISTINCT team_id
    """
    # 1) standings 기준
    rows = fetch_all(
        """
        SELECT DISTINCT team_id
        FROM standings
        WHERE league_id = %s
          AND season = %s
        """,
        (league_id, season),
    )
    if rows:
        return [r["team_id"] for r in rows if r.get("team_id") is not None]

    # 2) standings 가 비었으면 matches 기준
    rows = fetch_all(
        """
        SELECT DISTINCT team_id FROM (
            SELECT home_id AS team_id
            FROM matches
            WHERE league_id = %s AND season = %s
            UNION
            SELECT away_id AS team_id
            FROM matches
            WHERE league_id = %s AND season = %s
        ) t
        """,
        (league_id, season, league_id, season),
    )
    return [r["team_id"] for r in rows if r.get("team_id") is not None]


# ─────────────────────────────────────
#  standings
# ─────────────────────────────────────

def fetch_standings_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    Api-Football /standings 호출.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/standings"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "season": season,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_list = data.get("response") or []
    if not resp_list:
        return []

    league_obj = resp_list[0].get("league") or {}
    raw_standings = league_obj.get("standings") or []

    flat_rows: List[Dict[str, Any]] = []
    for group_table in raw_standings:
        for team_row in group_table:
            flat_rows.append(team_row)

    return flat_rows


def upsert_standings(
    league_id: int,
    season: int,
    rows: List[Dict[str, Any]],
) -> None:
    """
    standings 테이블 upsert.
    """
    if not rows:
        print(f"    [standings] league={league_id}, season={season}: 응답 0 rows → 스킵")
        return

    now_iso = now_utc().isoformat()

    for row in rows:
        team = row.get("team") or {}
        stats_all = row.get("all") or {}
        goals = stats_all.get("goals") or {}

        team_id = team.get("id")
        if team_id is None:
            continue

        group_name = row.get("group") or "Overall"
        rank = row.get("rank")
        points = row.get("points")
        goals_diff = row.get("goalsDiff")
        played = stats_all.get("played")
        win = stats_all.get("win")
        draw = stats_all.get("draw")
        lose = stats_all.get("lose")
        goals_for = goals.get("for")
        goals_against = goals.get("against")
        form = row.get("form")
        description = row.get("description")

        execute(
            """
            INSERT INTO standings (
                league_id,
                season,
                group_name,
                rank,
                team_id,
                points,
                goals_diff,
                played,
                win,
                draw,
                lose,
                goals_for,
                goals_against,
                form,
                updated_utc,
                description
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season, group_name, team_id) DO UPDATE SET
                rank          = EXCLUDED.rank,
                points        = EXCLUDED.points,
                goals_diff    = EXCLUDED.goals_diff,
                played        = EXCLUDED.played,
                win           = EXCLUDED.win,
                draw          = EXCLUDED.draw,
                lose          = EXCLUDED.lose,
                goals_for     = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                form          = EXCLUDED.form,
                updated_utc   = EXCLUDED.updated_utc,
                description   = EXCLUDED.description
            """,
            (
                league_id,
                season,
                group_name,
                rank,
                team_id,
                points,
                goals_diff,
                played,
                win,
                draw,
                lose,
                goals_for,
                goals_against,
                form,
                now_iso,
                description,
            ),
        )


def update_standings_for_league(
    league_id: int,
    season: int,
    date_str: str,
    phase: str,
) -> None:
    """
    PREMATCH / POSTMATCH 타이밍에서 standings 를 갱신.
    phase: "PREMATCH" 또는 "POSTMATCH"
    """
    print(
        f"    [standings {phase}] league={league_id}, season={season}, "
        f"date={date_str} → Api-Football 호출"
    )
    try:
        rows = fetch_standings_from_api(league_id, season)
        print(f"    [standings {phase}] 응답 팀 수={len(rows)}")
        upsert_standings(league_id, season, rows)
    except Exception as e:
        print(
            f"    [standings {phase}] league={league_id}, season={season} 처리 중 에러: {e}",
            file=sys.stderr,
        )


# ─────────────────────────────────────
#  squads
# ─────────────────────────────────────

def fetch_squad_from_api(team_id: int) -> Optional[Dict[str, Any]]:
    """
    Api-Football Squads 엔드포인트 호출.
    https://v3.football.api-sports.io/players/squads?team={team_id}
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/players/squads"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "team": team_id,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_list = data.get("response") or []
    if not resp_list:
        return None

    # 일반적으로 team 1개만 응답이므로 첫 번째만 사용
    return resp_list[0]


def upsert_squad(
    team_id: int,
    season: int,
    squad_data: Dict[str, Any],
) -> None:
    """
    squads (
        team_id   INTEGER,
        season    INTEGER,
        data_json JSONB NOT NULL,
        PRIMARY KEY (team_id, season)
    )
    """
    json_str = json.dumps(squad_data)

    execute(
        """
        INSERT INTO squads (
            team_id,
            season,
            data_json
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (team_id, season) DO UPDATE SET
            data_json = EXCLUDED.data_json
        """,
        (team_id, season, json_str),
    )


def update_squads_for_league(
    league_id: int,
    season: int,
    phase: str,
) -> None:
    """
    해당 리그 + 시즌의 모든 팀에 대해 스쿼드 정보를 갱신.
    phase: "PREMATCH" / "POSTMATCH" (로그용)
    """
    team_ids = _get_team_ids_for_league_season(league_id, season)
    if not team_ids:
        print(
            f"    [squads {phase}] league={league_id}, season={season}: team_ids 비어있음 → 스킵"
        )
        return

    print(
        f"    [squads {phase}] league={league_id}, season={season}: "
        f"{len(team_ids)}개 팀에 대해 스쿼드 갱신"
    )

    for tid in team_ids:
        try:
            data = fetch_squad_from_api(tid)
            if not data:
                print(
                    f"      [squads {phase}] team={tid}: 응답 없음 → 스킵"
                )
                continue

            upsert_squad(tid, season, data)
        except Exception as e:
            print(
                f"      [squads {phase}] team={tid} 처리 중 에러: {e}",
                file=sys.stderr,
            )


# ─────────────────────────────────────
#  injuries (새로 추가)
# ─────────────────────────────────────

def fetch_injuries_from_api(
    league_id: int,
    season: int,
) -> List[Dict[str, Any]]:
    """
    Api-Football /injuries 호출.
    https://v3.football.api-sports.io/injuries?league={league_id}&season={season}
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/injuries"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "season": season,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    return data.get("response") or []


def upsert_injuries(
    league_id: int,
    season: int,
    rows: List[Dict[str, Any]],
    phase: str,
) -> None:
    """
    injuries (
        player_id INTEGER NOT NULL,
        team_id   INTEGER NOT NULL,
        season    INTEGER NOT NULL,
        data_json TEXT    NOT NULL,
        PRIMARY KEY (player_id, team_id, season)
    )
    """
    if not rows:
        print(
            f"    [injuries {phase}] league={league_id}, season={season}: 응답 0 rows → 스킵"
        )
        return

    count = 0
    for row in rows:
        player = row.get("player") or {}
        team = row.get("team") or {}
        league_obj = row.get("league") or {}

        player_id = player.get("id")
        team_id = team.get("id")
        # 응답에 season 이 있으면 우선 사용, 없으면 인자로 받은 season 사용
        row_season = league_obj.get("season") or season

        if player_id is None or team_id is None or row_season is None:
            continue

        json_str = json.dumps(row)

        execute(
            """
            INSERT INTO injuries (
                player_id,
                team_id,
                season,
                data_json
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (player_id, team_id, season) DO UPDATE SET
                data_json = EXCLUDED.data_json
            """,
            (player_id, team_id, row_season, json_str),
        )
        count += 1

    print(
        f"    [injuries {phase}] league={league_id}, season={season}: "
        f"{count} rows upsert"
    )


def update_injuries_for_league(
    league_id: int,
    season: int,
    phase: str,
) -> None:
    """
    PREMATCH / POSTMATCH 타이밍에서 부상 정보를 갱신.
    """
    print(
        f"    [injuries {phase}] league={league_id}, season={season} → Api-Football 호출"
    )
    try:
        rows = fetch_injuries_from_api(league_id, season)
        upsert_injuries(league_id, season, rows, phase)
    except Exception as e:
        print(
            f"    [injuries {phase}] league={league_id}, season={season} 처리 중 에러: {e}",
            file=sys.stderr,
        )


# ─────────────────────────────────────
#  B그룹 진입점: PREMATCH / POSTMATCH
#   (update_live_fixtures.py 에서 호출)
# ─────────────────────────────────────

def update_static_data_prematch_for_league(
    league_id: int,
    date_str: str,
) -> None:
    """
    B그룹 데이터(현재: standings + squads + injuries)를
    '킥오프 1시간 전' 구간에서 갱신.
    """
    season = resolve_league_season_for_date(league_id, date_str)
    if season is None:
        print(
            f"    [STATIC PREMATCH] league={league_id}, date={date_str}: "
            f"matches 에서 season 을 찾지 못해 B그룹 전체 스킵"
        )
        return

    print(f"    [STATIC PREMATCH] league={league_id}, season={season}, date={date_str}")

    # 1) standings
    update_standings_for_league(league_id, season, date_str, phase="PREMATCH")

    # 2) squads
    update_squads_for_league(league_id, season, phase="PREMATCH")

    # 3) injuries
    update_injuries_for_league(league_id, season, phase="PREMATCH")


def update_static_data_postmatch_for_league(
    league_id: int,
    date_str: str,
) -> None:
    """
    B그룹 데이터(현재: standings + squads + injuries)를
    '경기 종료 직후' 구간에서 한 번 더 갱신.
    """
    season = resolve_league_season_for_date(league_id, date_str)
    if season is None:
        print(
            f"    [STATIC POSTMATCH] league={league_id}, date={date_str}: "
            f"matches 에서 season 을 찾지 못해 B그룹 전체 스킵"
        )
        return

    print(f"    [STATIC POSTMATCH] league={league_id}, season={season}, date={date_str}")

    # 1) standings
    update_standings_for_league(league_id, season, date_str, phase="POSTMATCH")

    # 2) squads
    update_squads_for_league(league_id, season, phase="POSTMATCH")

    # 3) injuries
    update_injuries_for_league(league_id, season, phase="POSTMATCH")
