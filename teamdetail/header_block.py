# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List, Optional
import json

from psycopg2.extras import RealDictCursor

from db import get_db


FINAL_STATUSES = ("FT", "AET", "PEN")


def _safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur if cur is not None else default


def _fetch_team_and_league(cur, team_id: int, league_id: int):
    team_row: Optional[Dict[str, Any]] = None
    league_row: Optional[Dict[str, Any]] = None

    cur.execute(
        "SELECT id, name, country, logo FROM teams WHERE id = %s",
        (team_id,),
    )
    team_row = cur.fetchone()

    cur.execute(
        "SELECT id, name, country, logo FROM leagues WHERE id = %s",
        (league_id,),
    )
    league_row = cur.fetchone()

    return team_row, league_row


def _fetch_team_season_stats(cur, team_id: int, season: int) -> List[Dict[str, Any]]:
    """
    해당 팀의 시즌별 리그/대륙컵 스탯 (team_season_stats.full_json) 전체 가져오기.
    한 팀이 리그 + 챔스 둘 다 뛰면 row가 2개 있을 수 있음.
    """
    cur.execute(
        """
        SELECT
            tss.league_id,
            tss.season,
            tss.full_json,
            l.name AS league_name
        FROM team_season_stats AS tss
        JOIN leagues AS l ON l.id = tss.league_id
        WHERE tss.team_id = %s
          AND tss.season = %s
        """,
        (team_id, season),
    )
    rows = cur.fetchall() or []
    for r in rows:
        if isinstance(r.get("full_json"), str):
            r["full_json"] = json.loads(r["full_json"])
    return rows


def _build_domestic_and_continental_info(
    stats_rows: List[Dict[str, Any]],
    target_league_id: int,
):
    """
    - domestic: 요청에서 들어온 league_id 와 같은 row
    - continental: 나머지 row 중 첫 번째 (예: 챔스, 유로파 등)
    """
    domestic = {
        "league_name": None,
        "matches": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
    }
    continental = {
        "league_name": None,
        "matches": 0,
    }

    for row in stats_rows:
        league_id = row["league_id"]
        league_name = row.get("league_name")
        js = row["full_json"]

        fixtures = js.get("fixtures", {})
        played_total = _safe_get(fixtures, "played", "total", default=0)
        wins_total = _safe_get(fixtures, "wins", "total", default=0)
        draws_total = _safe_get(fixtures, "draws", "total", default=0)
        loses_total = _safe_get(fixtures, "loses", "total", default=0)

        goals = js.get("goals", {})
        gf_total = _safe_get(goals, "for", "total", "total", default=0)
        ga_total = _safe_get(goals, "against", "total", "total", default=0)

        if league_id == target_league_id:
            domestic.update(
                {
                    "league_name": league_name,
                    "matches": int(played_total or 0),
                    "wins": int(wins_total or 0),
                    "draws": int(draws_total or 0),
                    "losses": int(loses_total or 0),
                    "goals_for": int(gf_total or 0),
                    "goals_against": int(ga_total or 0),
                }
            )
        else:
            # 대륙컵 (챔스/유로파 등) – 여러 개가 있어도 일단 첫 번째만 사용
            if continental["league_name"] is None:
                continental.update(
                    {
                        "league_name": league_name,
                        "matches": int(played_total or 0),
                    }
                )

    return domestic, continental


def _build_recent_form(
    cur,
    team_id: int,
    season: int,
    limit: int = 10,
) -> List[str]:
    """
    matches 테이블에서 해당 시즌, 해당 팀의 최근 경기들을 가져와서
    ["W", "D", "L", ...] 리스트로 만든다.

    - 리그/대륙컵 모두 포함
    - 가장 오른쪽이 가장 최근 경기가 되도록 (오래된 → 최신 순서로 리턴)
    """
    cur.execute(
        """
        SELECT
            date_utc,
            home_id,
            away_id,
            home_ft,
            away_ft,
            status
        FROM matches
        WHERE season = %s
          AND (home_id = %s OR away_id = %s)
          AND status = ANY(%s)
        ORDER BY date_utc DESC
        LIMIT %s
        """,
        (season, team_id, team_id, list(FINAL_STATUSES), limit),
    )
    rows = cur.fetchall() or []

    codes: List[str] = []
    for r in rows:
        home_ft = r.get("home_ft")
        away_ft = r.get("away_ft")
        if home_ft is None or away_ft is None:
            continue

        home_id = r.get("home_id")
        away_id = r.get("away_id")

        if home_ft == away_ft:
            code = "D"
        else:
            is_home = team_id == home_id
            team_goals = home_ft if is_home else away_ft
            opp_goals = away_ft if is_home else home_ft
            code = "W" if (team_goals or 0) > (opp_goals or 0) else "L"

        codes.append(code)

    # DB 에서 최신 → 오래된 순으로 가져왔으니, 화면은 왼쪽=오래된, 오른쪽=최신으로 맞추기 위해 역순
    return list(reversed(codes))


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 상단 헤더 영역에 쓸 정보.
    - 리그/대륙컵 스탯: team_season_stats.full_json
    - 최근 폼: matches 테이블에서 최근 10경기 결과
    """
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        team_row, league_row = _fetch_team_and_league(cur, team_id, league_id)
        stats_rows = _fetch_team_season_stats(cur, team_id, season)
        domestic, continental = _build_domestic_and_continental_info(
            stats_rows, league_id
        )
        recent_form = _build_recent_form(cur, team_id, season, limit=10)

        team_name = (team_row or {}).get("name")
        team_logo = (team_row or {}).get("logo")
        league_name = (league_row or {}).get("name")

        played = domestic["matches"]
        wins = domestic["wins"]
        draws = domestic["draws"]
        losses = domestic["losses"]
        goals_for = domestic["goals_for"]
        goals_against = domestic["goals_against"]
        goal_diff = goals_for - goals_against

        header: Dict[str, Any] = {
            "team_id": team_id,
            "league_id": league_id,
            "season": season,
            "team_name": team_name,
            "team_short_name": team_name,  # 필요하면 나중에 축약 로직 추가
            "team_logo": team_logo,
            "league_name": league_name,
            "season_label": str(season),
            "position": None,  # standings_block 에서 채우는게 더 자연스러움
            "played": played,
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_diff": goal_diff,
            "recent_form": recent_form,
            # 매치 수 요약(카드 왼쪽 텍스트용)
            "domestic_league_name": domestic["league_name"],
            "domestic_matches": domestic["matches"],
            "continental_league_name": continental["league_name"],
            "continental_matches": continental["matches"],
        }

        return header

    finally:
        cur.close()
