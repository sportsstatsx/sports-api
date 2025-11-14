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
        print(f"    [standings] league={league_id}, season={season}: 응답 0 rows → 스킱")
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
#  team_season_stats
# ─────────────────────────────────────

def fetch_team_statistics_from_api(
    league_id: int,
    season: int,
    team_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Api-Football /teams/statistics 호출.
    https://v3.football.api-sports.io/teams/statistics?league={league}&season={season}&team={team_id}

    응답 전체(response 객체)를 그대로 반환한다.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/teams/statistics"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "league": league_id,
        "season": season,
        "team": team_id,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_obj = data.get("response")
    if not resp_obj:
        return None

    # /teams/statistics 는 response 가 dict 1개 형태
    return resp_obj


def upsert_team_season_stats(
    league_id: int,
    season: int,
    team_id: int,
    stats_data: Dict[str, Any],
    phase: str,
) -> None:
    """
    team_season_stats (
        league_id INTEGER NOT NULL,
        season    INTEGER NOT NULL,
        team_id   INTEGER NOT NULL,
        name      TEXT    NOT NULL,
        value     TEXT,
        PRIMARY KEY (league_id, season, team_id, name)
    )

    우선은 전체 statistics JSON 을 name='full_json' 한 줄로 저장한다.
    (나중에 원하면 세부 필드별로 쪼개는 형태로 리팩터링 가능)
    """
    json_str = json.dumps(stats_data)

    execute(
        """
        INSERT INTO team_season_stats (
            league_id,
            season,
            team_id,
            name,
            value
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (league_id, season, team_id, name) DO UPDATE SET
            value = EXCLUDED.value
        """,
        (league_id, season, team_id, "full_json", json_str),
    )


def update_team_season_stats_for_league(
    league_id: int,
    season: int,
    phase: str,
) -> None:
    """
    해당 리그 + 시즌의 모든 팀에 대해 /teams/statistics 를 호출하여
    team_season_stats 테이블(full_json) 을 갱신.
    """
    team_ids = _get_team_ids_for_league_season(league_id, season)
    if not team_ids:
        print(
            f"    [team_season_stats {phase}] league={league_id}, season={season}: "
            f"team_ids 비어있음 → 스킵"
        )
        return

    print(
        f"    [team_season_stats {phase}] league={league_id}, season={season}: "
        f"{len(team_ids)}개 팀에 대해 팀 시즌 스탯 갱신"
    )

    updated = 0
    for tid in team_ids:
        try:
            stats = fetch_team_statistics_from_api(league_id, season, tid)
            if not stats:
                continue
            upsert_team_season_stats(league_id, season, tid, stats, phase)
            updated += 1
        except Exception as e:
            print(
                f"      [team_season_stats {phase}] team_id={tid} 처리 중 에러: {e}",
                file=sys.stderr,
            )

    print(
        f"    [team_season_stats {phase}] league={league_id}, season={season}: "
        f"{updated}개 팀 upsert"
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
        data_json TEXT NOT NULL,
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
#  injuries
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
#  predictions
# ─────────────────────────────────────

def fetch_prediction_for_fixture(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    /predictions?fixture={fixture_id} 호출해서 첫 번째 응답을 반환.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/predictions"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_list = data.get("response") or []
    if not resp_list:
        return None
    return resp_list[0]


def upsert_prediction(
    fixture_id: int,
    prediction: Dict[str, Any],
    phase: str,
) -> None:
    """
    predictions (
        fixture_id INTEGER PRIMARY KEY,
        data_json  TEXT NOT NULL
    )
    """
    json_str = json.dumps(prediction)

    execute(
        """
        INSERT INTO predictions (
            fixture_id,
            data_json
        )
        VALUES (%s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json = EXCLUDED.data_json
        """,
        (fixture_id, json_str),
    )


def update_predictions_for_league(
    league_id: int,
    date_str: str,
    phase: str,
) -> None:
    """
    해당 리그 + 날짜의 모든 경기 fixture_id 에 대해 predictions 를 갱신.
    """
    rows = fetch_all(
        """
        SELECT fixture_id
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        """,
        (league_id, date_str),
    )

    fixture_ids = [r["fixture_id"] for r in rows if r.get("fixture_id") is not None]
    if not fixture_ids:
        print(
            f"    [predictions {phase}] league={league_id}, date={date_str}: "
            f"해당 날짜 경기 없음 → 스킵"
        )
        return

    print(
        f"    [predictions {phase}] league={league_id}, date={date_str}: "
        f"{len(fixture_ids)}개 경기 예측 갱신"
    )

    updated = 0
    for fid in fixture_ids:
        try:
            pred = fetch_prediction_for_fixture(fid)
            if not pred:
                continue
            upsert_prediction(fid, pred, phase)
            updated += 1
        except Exception as e:
            print(
                f"      [predictions {phase}] fixture_id={fid} 처리 중 에러: {e}",
                file=sys.stderr,
            )

    print(
        f"    [predictions {phase}] league={league_id}, date={date_str}: "
        f"{updated}개 fixture upsert"
    )


# ─────────────────────────────────────
#  odds + odds_history
# ─────────────────────────────────────

def fetch_odds_for_fixture(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    /odds?fixture={fixture_id} 호출해서 첫 번째 응답을 반환.
    """
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_KEY env 가 설정되어 있지 않습니다.")

    url = "https://v3.football.api-sports.io/odds"
    headers = {
        "x-apisports-key": API_KEY,
    }
    params = {
        "fixture": fixture_id,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    resp_list = data.get("response") or []
    if not resp_list:
        return None
    return resp_list[0]


def upsert_odds_and_history(
    fixture_id: int,
    odds_payload: Dict[str, Any],
    phase: str,
) -> None:
    """
    odds / odds_history 두 테이블을 동시에 갱신.

    - odds         : (fixture_id, bookmaker, market, selection) 단위로 upsert
    - odds_history : 매번 INSERT 하여 스냅샷 누적
    """
    updated_at = odds_payload.get("update")
    bookmakers = odds_payload.get("bookmakers") or []

    count = 0

    for bm in bookmakers:
        bookmaker_name = bm.get("name") or str(bm.get("id") or "")
        bets = bm.get("bets") or []

        for bet in bets:
            market = bet.get("name") or str(bet.get("id") or "")
            values = bet.get("values") or []

            for v in values:
                selection = v.get("value")
                odd = v.get("odd")

                data_json = json.dumps({
                    "fixture_id": fixture_id,
                    "bookmaker": bookmaker_name,
                    "market": market,
                    "selection": selection,
                    "odd": odd,
                    "updated_at": updated_at,
                    "raw": v,
                })

                # odds (현재 값 유지용)
                execute(
                    """
                    INSERT INTO odds (
                        fixture_id,
                        bookmaker,
                        market,
                        selection,
                        odd,
                        updated_at,
                        data_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fixture_id, bookmaker, market, selection) DO UPDATE SET
                        odd        = EXCLUDED.odd,
                        updated_at = EXCLUDED.updated_at,
                        data_json  = EXCLUDED.data_json
                    """,
                    (
                        fixture_id,
                        bookmaker_name,
                        market,
                        selection,
                        odd,
                        updated_at,
                        data_json,
                    ),
                )

                # odds_history (스냅샷 누적)
                execute(
                    """
                    INSERT INTO odds_history (
                        fixture_id,
                        bookmaker,
                        market,
                        selection,
                        odd,
                        updated_at,
                        data_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        fixture_id,
                        bookmaker_name,
                        market,
                        selection,
                        odd,
                        updated_at,
                        data_json,
                    ),
                )

                count += 1

    print(
        f"    [odds {phase}] fixture_id={fixture_id}: {count} rows (odds/odds_history) 처리"
    )


def update_odds_for_league(
    league_id: int,
    date_str: str,
    phase: str,
) -> None:
    """
    해당 리그 + 날짜의 모든 경기 fixture_id 에 대해
    odds / odds_history 를 갱신.
    """
    rows = fetch_all(
        """
        SELECT fixture_id
        FROM matches
        WHERE league_id = %s
          AND SUBSTRING(date_utc FROM 1 FOR 10) = %s
        """,
        (league_id, date_str),
    )

    fixture_ids = [r["fixture_id"] for r in rows if r.get("fixture_id") is not None]
    if not fixture_ids:
        print(
            f"    [odds {phase}] league={league_id}, date={date_str}: "
            f"해당 날짜 경기 없음 → 스킵"
        )
        return

    print(
        f"    [odds {phase}] league={league_id}, date={date_str}: "
        f"{len(fixture_ids)}개 경기 배당 갱신"
    )

    handled = 0
    for fid in fixture_ids:
        try:
            payload = fetch_odds_for_fixture(fid)
            if not payload:
                continue
            upsert_odds_and_history(fid, payload, phase)
            handled += 1
        except Exception as e:
            print(
                f"      [odds {phase}] fixture_id={fid} 처리 중 에러: {e}",
                file=sys.stderr,
            )

    print(
        f"    [odds {phase}] league={league_id}, date={date_str}: "
        f"{handled}개 fixture 처리 완료"
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
    B그룹 데이터(standings + team_season_stats + squads + injuries + predictions + odds)를
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

    # 2) team_season_stats
    update_team_season_stats_for_league(league_id, season, phase="PREMATCH")

    # 3) squads
    update_squads_for_league(league_id, season, phase="PREMATCH")

    # 4) injuries
    update_injuries_for_league(league_id, season, phase="PREMATCH")

    # 5) predictions
    update_predictions_for_league(league_id, date_str, phase="PREMATCH")

    # 6) odds + odds_history
    update_odds_for_league(league_id, date_str, phase="PREMATCH")


def update_static_data_postmatch_for_league(
    league_id: int,
    date_str: str,
) -> None:
    """
    B그룹 데이터(standings + team_season_stats + squads + injuries + predictions + odds)를
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

    # 2) team_season_stats
    update_team_season_stats_for_league(league_id, season, phase="POSTMATCH")

    # 3) squads
    update_squads_for_league(league_id, season, phase="POSTMATCH")

    # 4) injuries
    update_injuries_for_league(league_id, season, phase="POSTMATCH")

    # 5) predictions
    update_predictions_for_league(league_id, date_str, phase="POSTMATCH")

    # 6) odds + odds_history
    update_odds_for_league(league_id, date_str, phase="POSTMATCH")
