from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all
from services.insights import (
    enrich_overall_outcome_and_combos,
    enrich_overall_shooting_efficiency,
    enrich_overall_timing,
    enrich_overall_firstgoal_momentum,
    enrich_overall_goals_by_time,
    enrich_overall_discipline_setpieces,
)


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 형태로 정규화한다.
    """
    if not date_str:
        return datetime.now().date().isoformat()

    s = date_str.strip()
    if len(s) >= 10:
        only_date = s[:10]
        try:
            dt = datetime.fromisoformat(only_date)
            return dt.date().isoformat()
        except Exception:
            return only_date
    return s


# ─────────────────────────────────────
#  1) 홈 상단 리그 탭
# ─────────────────────────────────────

def get_home_leagues(date_str: str) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.league_id,
            l.name  AS league_name,
            l.country,
            l.logo,
            m.season
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE m.date_utc::date = %s
        GROUP BY m.league_id, l.name, l.country, l.logo, m.season
        ORDER BY l.country NULLS LAST, l.name
        """,
        (norm_date,),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "league_id": r["league_id"],
                "league_name": r["league_name"],
                "country": r.get("country"),
                "logo": r.get("logo"),
                "season": r["season"],
            }
        )
    return result


# ─────────────────────────────────────
#  2) 홈: 매치데이 디렉터리
# ─────────────────────────────────────

def get_home_league_directory(date_str: Optional[str]) -> Dict[str, Any]:
    """
    전체 리그에 대해 사용 가능한 매치데이(날짜 목록)를 돌려준다.

    - items: [{ "date": "YYYY-MM-DD", "matches": <경기 수> }, ...]
    - current_date: 요청 date_str 에 가장 가까운 매치데이
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.date_utc::date AS match_date,
            COUNT(*)          AS matches
        FROM matches m
        GROUP BY match_date
        ORDER BY match_date ASC
        """,
    )

    items: List[Dict[str, Any]] = []
    target = datetime.fromisoformat(norm_date).date()
    nearest: Optional[date_cls] = None

    for r in rows:
        md: date_cls = r["match_date"]
        items.append(
            {
                "date": md.isoformat(),
                "matches": r["matches"],
            }
        )
        if nearest is None:
            nearest = md
        else:
            if abs(md - target) < abs(nearest - target):
                nearest = md

    current_date = nearest.isoformat() if nearest is not None else norm_date
    return {
        "current_date": current_date,
        "items": items,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────

def _find_matchday(date_str: str, league_id: Optional[int], *, direction: str) -> Optional[str]:
    """
    direction:
      - "next" : date_str 이후(포함) 첫 매치데이
      - "prev" : date_str 이전(포함) 마지막 매치데이
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_parts: List[str] = [
        "m.date_utc::date >= %s" if direction == "next" else "m.date_utc::date <= %s"
    ]

    if league_id and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    order = "ASC" if direction == "next" else "DESC"

    sql = f"""
        SELECT
            m.date_utc::date AS match_date
        FROM matches m
        WHERE {' AND '.join(where_parts)}
        GROUP BY match_date
        ORDER BY match_date {order}
        LIMIT 1
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    match_date = rows[0]["match_date"]
    return str(match_date)


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 + Insights Overall
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    가장 최신 season 한 줄을 가져오고, 거기에 insights_overall.* 지표를
    추가/보정해서 반환한다.
    """
    rows = fetch_all(
        """
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id   = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, team_id),
    )
    if not rows:
        return None

    row = rows[0]

    # value(JSON)를 파싱
    raw_value = row.get("value")
    if isinstance(raw_value, str):
        try:
            stats = json.loads(raw_value)
        except Exception:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # insights_overall 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    # 시즌 정수로 변환
    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    # ─ 실제 계산은 아래 insights 모듈에게 위임 ─
    enrich_overall_shooting_efficiency(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_outcome_and_combos(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
    )

    # 아직은 빈 껍데기지만, 구조만 잡아두기
    enrich_overall_timing(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
    )
    enrich_overall_firstgoal_momentum(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
    )
    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
    )
    enrich_overall_discipline_setpieces(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
    )

    # 최종 반환
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }


# ─────────────────────────────────────
#  5) 팀 정보
# ─────────────────────────────────────

def get_team_info(team_id: int) -> Optional[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
            id,
            name,
            country,
            logo
        FROM teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not rows:
        return None
    return rows[0]
