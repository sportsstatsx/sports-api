# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List
import json

from db import fetch_all  # ✅ 프로젝트 공통 DB 헬퍼


def _safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur if cur is not None else default


def _fetch_team(team_id: int) -> Dict[str, Any] | None:
    rows = fetch_all(
        "SELECT id, name, country, logo FROM teams WHERE id=%s",
        (team_id,),
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "id": row["id"],
        "name": row["name"],
        "country": row["country"],
        "logo": row["logo"],
    }


def _fetch_league(league_id: int) -> Dict[str, Any] | None:
    rows = fetch_all(
        "SELECT id, name, country, logo FROM leagues WHERE id=%s",
        (league_id,),
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "id": row["id"],
        "name": row["name"],
        "country": row["country"],
        "logo": row["logo"],
    }


def _fetch_team_season_stats(team_id: int, season: int) -> List[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 name='full_json' 인 row들만 가져와서 파싱.
    한 팀이 리그 + 챔스 둘 다 뛰면 row가 2개 있을 수 있음.
    """
    rows = fetch_all(
        """
        SELECT tss.league_id,
               tss.value,
               l.name AS league_name
        FROM team_season_stats AS tss
        JOIN leagues AS l ON l.id = tss.league_id
        WHERE tss.team_id = %s
          AND tss.season  = %s
          AND tss.name    = 'full_json'
        """,
        (team_id, season),
    )

    results: List[Dict[str, Any]] = []
    for r in rows:
        js = r["value"]
        if isinstance(js, str):
            js = json.loads(js)
        results.append(
            {
                "league_id": r["league_id"],
                "league_name": r["league_name"],
                "full_json": js,
            }
        )
    return results


def _build_recent_form(team_id: int, season: int, limit: int = 10) -> List[str]:
    """
    matches 테이블에서 해당 시즌, 해당 팀의 최근 경기들을 가져와서
    ["W", "D", "L", ...] 리스트로 만든다.

    - 리그/대륙컵 모두 포함
    - 종료된 경기(home_ft/away_ft 있는 것만)
    - 화면은 왼쪽=오래된, 오른쪽=최신이 되도록 역순 리턴
    """
    rows = fetch_all(
        """
        SELECT home_id, away_id, home_ft, away_ft
        FROM matches
        WHERE season = %s
          AND (home_id = %s OR away_id = %s)
          AND home_ft IS NOT NULL
          AND away_ft IS NOT NULL
        ORDER BY date_utc DESC
        LIMIT %s
        """,
        (season, team_id, team_id, limit),
    )

    codes: List[str] = []

    for r in rows:
        home_id = r["home_id"]
        away_id = r["away_id"]
        home_ft = r["home_ft"]
        away_ft = r["away_ft"]

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

    # DB에서 최신 → 오래된 순으로 가져왔으니, 화면은 오래된 → 최신 순서가 되도록 역순
    return list(reversed(codes))


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 상단 헤더 영역에 쓸 정보.

    - 팀/리그 기본 정보: teams / leagues
    - 리그/대륙컵 시즌 통계: team_season_stats (full_json)
    - 최근 폼: matches 에서 최근 10경기 (리그+대륙컵 합산)

    ✅ 하이브리드 보정(완전체):
    - team_season_stats(full_json) 기반 played가 덜 갱신된 경우가 있어서
      matches 테이블 완료경기 COUNT가 더 크면 max로 보정한다.
    """
    team_row = _fetch_team(team_id)
    league_row = _fetch_league(league_id)
    stats_rows = _fetch_team_season_stats(team_id, season)
    recent_form = _build_recent_form(team_id, season, limit=10)

    # 기본값
    played = wins = draws = losses = 0
    goals_for = goals_against = 0
    domestic_league_name = None
    continental_league_name = None
    continental_matches = 0
    continental_league_id = None  # ✅ 추가: 대륙컵 리그 id 보관

    # team_season_stats 에서 리그 / 대륙컵 분리
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
            # 요청 들어온 리그
            domestic_league_name = row["league_name"]
            played = int(played_total or 0)
            wins = int(wins_total or 0)
            draws = int(draws_total or 0)
            losses = int(loses_total or 0)
            goals_for = int(gf_total or 0)
            goals_against = int(ga_total or 0)
        else:
            # 그 외 리그 하나를 "대륙컵" 쪽으로 사용
            if continental_league_name is None:
                continental_league_id = row["league_id"]  # ✅ 추가
                continental_league_name = row["league_name"]
                continental_matches = int(played_total or 0)

    # ✅ 리그명 방어(혹시 stats_rows가 비었을 때)
    if domestic_league_name is None:
        domestic_league_name = (league_row or {}).get("name")

    # ─────────────────────────────────────────
    # ✅ ALWAYS reconcile by matches COUNT (중요!)
    # - team_season_stats가 6으로 덜 갱신되어도,
    #   matches COUNT가 8이면 played를 8로 올린다.
    # ─────────────────────────────────────────
    try:
        # (1) 현재 league_id (요청 리그) 완료경기 수
        rows = fetch_all(
            """
            SELECT COUNT(*) AS cnt
            FROM matches
            WHERE league_id = %s
              AND season    = %s
              AND (home_id = %s OR away_id = %s)
              AND (
                lower(coalesce(status_group,'')) = 'finished'
                OR coalesce(status,'') IN ('FT','AET','PEN')
                OR coalesce(status_short,'') IN ('FT','AET','PEN')
              )
              AND home_ft IS NOT NULL
              AND away_ft IS NOT NULL
            """,
            (league_id, season, team_id, team_id),
        )
        dom_cnt = 0
        if rows:
            dom_cnt = int(rows[0].get("cnt") or 0)

        if dom_cnt > played:
            played = dom_cnt  # ✅ 핵심: max 보정

        # (2) 대륙컵도 동일하게 보정 (있을 때만)
        if continental_league_id is not None:
            rows2 = fetch_all(
                """
                SELECT COUNT(*) AS cnt
                FROM matches
                WHERE league_id = %s
                  AND season    = %s
                  AND (home_id = %s OR away_id = %s)
                  AND (
                    lower(coalesce(status_group,'')) = 'finished'
                    OR coalesce(status,'') IN ('FT','AET','PEN')
                    OR coalesce(status_short,'') IN ('FT','AET','PEN')
                  )
                  AND home_ft IS NOT NULL
                  AND away_ft IS NOT NULL
                """,
                (continental_league_id, season, team_id, team_id),
            )
            cont_cnt = 0
            if rows2:
                cont_cnt = int(rows2[0].get("cnt") or 0)

            if cont_cnt > continental_matches:
                continental_matches = cont_cnt

    except Exception:
        # COUNT 실패해도 기존 값 유지
        pass

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


