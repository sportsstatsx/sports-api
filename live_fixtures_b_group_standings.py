import sys
from typing import Any, Dict, List, Optional

import requests

from db import execute
from live_fixtures_common import (
    API_KEY,
    now_utc,
    resolve_league_season_for_date,
)


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

    standings (
        league_id     integer not null,
        season        integer not null,
        group_name    text    not null default 'Overall',
        rank          integer not null,
        team_id       integer not null,
        points        integer,
        goals_diff    integer,
        played        integer,
        win           integer,
        draw          integer,
        lose          integer,
        goals_for     integer,
        goals_against integer,
        form          text,
        updated_utc   text,
        description   text,
        PRIMARY KEY (league_id, season, group_name, team_id)
    )
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
    date_str: str,
    phase: str,
) -> None:
    """
    PREMATCH / POSTMATCH 타이밍에서 standings 를 갱신.
    phase: "PREMATCH" 또는 "POSTMATCH"
    """
    season = resolve_league_season_for_date(league_id, date_str)
    if season is None:
        print(
            f"    [standings {phase}] league={league_id}, date={date_str}: "
            f"matches 에서 season 을 찾지 못해 스킵"
        )
        return

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


def update_static_data_prematch_for_league(
    league_id: int,
    date_str: str,
) -> None:
    """
    B그룹 데이터(현재는 standings만)를 '킥오프 1시간 전'에 갱신.
    이후 team_season_stats, squads, players, injuries, transfers, toplists, venues 도
    이 함수 내부에 순서대로 추가 예정.
    """
    print(f"    [STATIC PREMATCH] league={league_id}, date={date_str}")
    update_standings_for_league(league_id, date_str, phase="PREMATCH")


def update_static_data_postmatch_for_league(
    league_id: int,
    date_str: str,
) -> None:
    """
    B그룹 데이터(현재는 standings만)를 '경기 종료 직후'에 갱신.
    이후 team_season_stats, toplists 등도 이 함수에 추가 예정.
    """
    print(f"    [STATIC POSTMATCH] league={league_id}, date={date_str}")
    update_standings_for_league(league_id, date_str, phase="POSTMATCH")
