# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List, Optional
import json

from db import get_db   # 기존 구조 그대로 사용


FINAL_STATUSES = ("FT", "AET", "PEN")


def _safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur if cur is not None else default


def _fetch_team_and_league(cur, team_id: int, league_id: int):
    # 팀 정보
    cur.execute("SELECT id, name, country, logo FROM teams WHERE id=%s", (team_id,))
    row = cur.fetchone()
    team_row = None
    if row:
        team_row = {
            "id": row[0],
            "name": row[1],
            "country": row[2],
            "logo": row[3],
        }

    # 리그 정보
    cur.execute("SELECT id, name, country, logo FROM leagues WHERE id=%s", (league_id,))
    row = cur.fetchone()
    league_row = None
    if row:
        league_row = {
            "id": row[0],
            "name": row[1],
            "country": row[2],
            "logo": row[3],
        }

    return team_row, league_row


def _fetch_team_season_stats(cur, team_id: int, season: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT tss.league_id, tss.season, tss.full_json, l.name
        FROM team_season_stats AS tss
        JOIN leagues AS l ON l.id = tss.league_id
        WHERE tss.team_id=%s
          AND tss.season=%s
        """,
        (team_id, season),
    )
    rows = cur.fetchall() or []
    stats_list = []

    for r in rows:
        league_id = r[0]
        full_json = r[2]
        league_name = r[3]

        if isinstance(full_json, str):
            full_json = json.loads(full_json)

        stats_list.append(
            {
                "league_id": league_id,
                "full_json": full_json,
                "league_name": league_name,
            }
        )

    return stats_list


def _build_recent_form(cur, team_id: int, season: int, limit: int = 10) -> List[str]:
    cur.execute(
        """
        SELECT date_utc, home_id, away_id, home_ft, away_ft, status
        FROM matches
        WHERE season=%s
          AND (home_id=%s OR away_id=%s)
          AND status = ANY(%s)
        ORDER BY date_utc DESC
        LIMIT %s
        """,
        (season, team_id, team_id, FINAL_STATUSES, limit),
    )
    rows = cur.fetchall() or []

    codes = []

    for r in rows:
        home_id = r[1]
        away_id = r[2]
        home_ft = r[3]
        away_ft = r[4]

        if home_ft is None or away_ft is None:
            continue

        # 무승부
        if home_ft == away_ft:
            codes.append("D")
            continue

        is_home = team_id == home_id
        team_goals = home_ft if is_home else away_ft
        opp_goals = away_ft if is_home else home_ft

        codes.append("W" if team_goals > opp_goals else "L")

    return list(reversed(codes))   # 화면 왼→오래된 / 오른→최신


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()

    try:
        team_row, league_row = _fetch_team_and_league(cur, team_id, league_id)
        stats_rows = _fetch_team_season_stats(cur, team_id, season)
        recent_form = _build_recent_form(cur, team_id, season)

        # 기본값 준비
        played = wins = draws = losses = 0
        goals_for = goals_against = 0
        domestic_league_name = None
        continental_league_name = None
        continental_matches = 0

        # 스탯 정리
        for row in stats_rows:
            js = row["full_json"]
            fixtures = js.get("fixtures", {})
            played_total = _safe_get(fixtures, "played", "total", default=0)
            wins_total = _safe_get(fixtures, "wins", "total", default=0)
            draws_total = _safe_get(fixtures, "draws", "total", default=0)
            loses_total = _safe_get(fixtures, "loses", "total", default=0)
            gf_total = _safe_get(js, "goals", "for", "total", "total", default=0)
            ga_total = _safe_get(js, "goals", "against", "total", "total", default=0)

            if row["league_id"] == league_id:
                domestic_league_name = row["league_name"]
                played = int(played_total or 0)
                wins = int(wins_total or 0)
                draws = int(draws_total or 0)
                losses = int(loses_total or 0)
                goals_for = int(gf_total or 0)
                goals_against = int(ga_total or 0)
            else:
                if continental_league_name is None:
                    continental_league_name = row["league_name"]
                    continental_matches = int(played_total or 0)

        goal_diff = goals_for - goals_against

        return {
            "team_id": team_id,
            "league_id": league_id,
            "season": season,
            "team_name": (team_row or {}).get("name"),
            "team_short_name": (team_row or {}).get("name"),
            "team_logo": (team_row or {}).get("logo"),
            "league_name": (league_row or {}).get("name"),
            "season_label": str(season),
            "position": None,

            "played": played,
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_diff": goal_diff,

            "recent_form": recent_form,

            "domestic_league_name": domestic_league_name,
            "domestic_matches": played,
            "continental_league_name": continental_league_name,
            "continental_matches": continental_matches,
        }

    finally:
        cur.close()
