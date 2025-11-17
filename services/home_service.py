from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all

from .insights.insights_overall_shooting_efficiency import (
    enrich_overall_shooting_efficiency,
)
from .insights.insights_overall_outcome_totals import (
    enrich_overall_outcome_totals,
)
from .insights.insights_overall_goalsbytime import (
    enrich_overall_goals_by_time,
)
from .insights.insights_overall_timing import (
    enrich_overall_timing,
)
from .insights.insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from .insights.insights_overall_resultscombos_draw import (
    enrich_overall_results_combos_draw,
)
from .insights.insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)


# ─────────────────────────────────────
#  공통 유틸
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    date_str 가 None 이거나 빈 문자열이면 오늘 날짜(UTC 기준)를 YYYY-MM-DD 로 반환.
    아니면 그대로 YYYY-MM-DD 로 파싱 후 반환.
    """
    if not date_str:
        return datetime.utcnow().date().isoformat()
    try:
        # 이미 YYYY-MM-DD 형식이라고 가정
        return datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    except ValueError:
        # 형식이 안 맞으면 그냥 오늘 날짜로 fallback
        return datetime.utcnow().date().isoformat()


# ─────────────────────────────────────
#  1) 홈 화면: 리그 리스트
# ─────────────────────────────────────

def get_home_leagues(date_str: Optional[str] = None) -> Dict[str, Any]:
    """
    홈 화면 상단 '리그 목록' 영역.
    주어진 날짜(또는 오늘)에 실제로 경기가 있는 리그만 반환.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            l.id   AS league_id,
            l.name AS league_name,
            l.logo AS league_logo,
            l.country AS country_name,
            COUNT(*) AS match_count
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE m.date_utc::date = %s
        GROUP BY l.id, l.name, l.logo, l.country
        ORDER BY l.country, l.name
        """,
        (norm_date,),
    )

    leagues: List[Dict[str, Any]] = []
    for r in rows:
        leagues.append(
            {
                "league_id": r["league_id"],
                "name": r["league_name"],
                "logo": r["league_logo"],
                "country": r["country_name"],
                "match_count": r["match_count"],
            }
        )

    return {
        "date": norm_date,
        "leagues": leagues,
    }


# ─────────────────────────────────────
#  2) 홈 화면: 특정 리그의 매치 디렉터리
# ─────────────────────────────────────

def get_home_league_directory(league_id: int, date_str: Optional[str]) -> Dict[str, Any]:
    """
    특정 리그의 주어진 날짜(date_str)에 대한 매치 디렉터리 정보.

    ✅ 변경 사항
    - 기존: matches + teams 만 조회 (레드카드 정보 없음)
    - 변경: match_events 를 LEFT JOIN 해서
      홈/원정 레드카드 개수를 같이 집계해서 내려줌.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.round,
            m.date_utc,
            m.status_short,
            m.status_group,
            m.home_id,
            th.name   AS home_name,
            th.logo   AS home_logo,
            m.away_id,
            ta.name   AS away_name,
            ta.logo   AS away_logo,
            m.home_ft,
            m.away_ft,
            COALESCE(
                SUM(
                    CASE
                        WHEN lower(e.type)   = 'card'
                         AND lower(e.detail) = 'red card'
                         AND e.team_id       = m.home_id
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS home_red_cards,
            COALESCE(
                SUM(
                    CASE
                        WHEN lower(e.type)   = 'card'
                         AND lower(e.detail) = 'red card'
                         AND e.team_id       = m.away_id
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        LEFT JOIN match_events e
          ON e.fixture_id = m.fixture_id
         AND e.minute IS NOT NULL
        WHERE m.league_id = %s
          AND m.date_utc::date = %s
        GROUP BY
            m.fixture_id,
            m.league_id,
            m.season,
            m.round,
            m.date_utc,
            m.status_short,
            m.status_group,
            m.home_id,
            th.name,
            th.logo,
            m.away_id,
            ta.name,
            ta.logo,
            m.home_ft,
            m.away_ft
        ORDER BY m.date_utc ASC, m.fixture_id ASC
        """,
        (league_id, norm_date),
    )

    fixtures: List[Dict[str, Any]] = []
    season: Optional[int] = None
    round_name: Optional[str] = None

    for r in rows:
        season = season or r["season"]
        round_name = round_name or r["round"]

        fixtures.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "round": r["round"],
                "date_utc": r["date_utc"].isoformat() if r["date_utc"] else None,
                "status_short": r["status_short"],
                "status_group": r["status_group"],
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "goals": r["home_ft"],
                    "red_cards": r["home_red_cards"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "goals": r["away_ft"],
                    "red_cards": r["away_red_cards"],
                },
            }
        )

    return {
        "league_id": league_id,
        "date": norm_date,
        "season": season,
        "round": round_name,
        "fixtures": fixtures,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────

def _find_matchday(date_str: str, league_id: Optional[int], direction: str) -> Optional[str]:
    """
    direction: 'next' or 'prev'
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_clause = "m.date_utc::date <> %s"

    if league_id:
        where_clause += " AND m.league_id = %s"
        params.append(league_id)

    rows = fetch_all(
        f"""
        SELECT DISTINCT m.date_utc::date AS match_date
        FROM matches m
        WHERE {where_clause}
        """,
        tuple(params),
    )

    if not rows:
        return None

    target = datetime.strptime(norm_date, "%Y-%m-%d").date()
    nearest: Optional[date_cls] = None

    for r in rows:
        md: date_cls = r["match_date"]
        if direction == "next":
            if md > target and (nearest is None or md < nearest):
                nearest = md
        else:
            if md < target and (nearest is None or md > nearest):
                nearest = md

    if not nearest:
        return None
    return nearest.isoformat()


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 (Insights 탭)
# ─────────────────────────────────────

def get_team_season_stats(league_id: int, team_id: int) -> Dict[str, Any]:
    """
    team_season_stats 테이블에서 JSON을 읽어와,
    일부 지표(Insights Overall 등)는 서버에서 다시 계산해 넣고 반환.
    """
    row = fetch_all(
        """
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, team_id),
    )

    if not row:
        return {
            "league_id": league_id,
            "team_id": team_id,
            "season": None,
            "name": None,
            "value": {},
        }

    row = row[0]
    season = row["season"]
    name = row["name"]
    raw_value = row["value"]

    # value(JSON) 파싱
    if isinstance(raw_value, str):
        try:
            stats = json.loads(raw_value)
        except json.JSONDecodeError:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # insights_overall 블록 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    # ✅ 서버에서 다시 계산하는 지표들
    #    (Outcome & Totals, Timing, First Goal/Momentum, Results Combos & Draw,
    #     Goals by Time, Shooting & Efficiency, Discipline & Set Pieces)
    matches_total_api = stats.get("matches_total_api", 0) or 0

    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_timing(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_firstgoal_momentum(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_results_combos_draw(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_shooting_efficiency(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    enrich_overall_discipline_setpieces(
        stats,
        insights,
        league_id=league_id,
        season_int=season,
        team_id=team_id,
        matches_total_api=matches_total_api,
    )

    return {
        "league_id": league_id,
        "team_id": team_id,
        "season": season,
        "name": name,
        "value": stats,
    }
