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
from .insights.insights_overall_timing import enrich_overall_timing
from .insights.insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from .insights.insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 로 정규화한다.
    """
    if not date_str:
        # 오늘 날짜
        return datetime.utcnow().date().isoformat()

    if isinstance(date_str, date_cls):
        return date_str.isoformat()

    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()


# ─────────────────────────────────────
#  1) 홈 화면: 리그 목록
# ─────────────────────────────────────

def get_home_leagues(
    date_str: Optional[str],
    league_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.
    league_ids 가 주어지면 해당 리그들만 필터링.
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_clause = "m.date_utc::date = %s"

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clause += f" AND m.league_id IN ({placeholders})"
        params.extend(league_ids)

    rows = fetch_all(
        f"""
        SELECT
            m.league_id,
            l.name    AS league_name,
            l.country AS country,
            l.logo    AS league_logo,
            m.season
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.id      -- ✅ 올바른 PK 컬럼 이름
        WHERE {where_clause}
        GROUP BY
            m.league_id,
            l.name,
            l.country,
            l.logo,
            m.season
        ORDER BY
            l.country,
            l.name
        """,
        tuple(params),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "league_id": r["league_id"],
                "name": r["league_name"],
                "country": r["country"],
                "logo": r["league_logo"],
                "season": r["season"],
            }
        )
    return result


# ─────────────────────────────────────
#  2) 홈 화면: 특정 리그의 매치 디렉터리
# ─────────────────────────────────────

def get_home_league_directory(league_id: int, date_str: Optional[str]) -> Dict[str, Any]:
    """
    특정 리그의 주어진 날짜(date_str)에 대한 매치 디렉터리 정보.
    Postgres matches 스키마(라운드/short 컬럼 없음)에 맞춰서,
    각 경기의 홈/원정 레드카드 개수까지 함께 내려준다.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            NULL::text AS round,              -- ✅ matches에는 round 컬럼이 없으니 NULL alias 로 맞춰줌
            m.date_utc,
            m.status AS status_short,         -- ✅ status_short 대신 status 컬럼을 그대로 alias
            m.status_group,
            m.home_id,
            th.name   AS home_name,
            th.logo   AS home_logo,
            m.away_id,
            ta.name   AS away_name,
            ta.logo   AS away_logo,
            m.home_ft,
            m.away_ft,
            -- ✅ 홈 팀 레드카드 개수
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id    = m.home_id
                  AND lower(e.type)   = 'card'
                  AND lower(e.detail) = 'red card'
            ) AS home_red_cards,
            -- ✅ 원정 팀 레드카드 개수
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id    = m.away_id
                  AND lower(e.type)   = 'card'
                  AND lower(e.detail) = 'red card'
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        WHERE m.league_id = %s
          AND m.date_utc::date = %s
        ORDER BY m.date_utc ASC, m.fixture_id ASC
        """,
        (league_id, norm_date),
    )

    fixtures: List[Dict[str, Any]] = []
    season: Optional[int] = None
    round_name: Optional[str] = None

    for r in rows:
        season = season or r["season"]
        round_name = round_name or r["round"]  # 위에서 NULL::text AS round 로 alias 맞춰서 KeyError 안 남

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
                    "red_cards": r["home_red_cards"],   # 👈 새로 추가
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "goals": r["away_ft"],
                    "red_cards": r["away_red_cards"],   # 👈 새로 추가
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

    params: List[Any] = []
    where_clause = "1=1"
    if league_id and league_id > 0:
        where_clause += " AND m.league_id = %s"
        params.append(league_id)

    rows = fetch_all(
        f"""
        SELECT
            m.date_utc::date AS match_date,
            COUNT(*)         AS matches
        FROM matches m
        WHERE {where_clause}
        GROUP BY match_date
        ORDER BY match_date ASC
        """,
        tuple(params),
    )

    target = datetime.fromisoformat(norm_date).date()
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
#  4) 팀 시즌 스탯 + Insights Overall (섹션별 모듈 위임)
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    가장 최신 season 한 줄을 가져오고,
    stats["value"] 안의 insights_overall 블록을
    섹션별 모듈(enrich_overall_*)을 통해 채워서 반환한다.
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
    raw_value = row.get("value")

    # value(JSON) 파싱
    if isinstance(raw_value, str):
        try:
            stats: Dict[str, Any] = json.loads(raw_value)
        except Exception:
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

    # ✅ 서버에서 다시 계산하는 지표인데,
    #    원래 JSON 안에서 null 로 들어온 값은 미리 지워준다.
    #    (그래야 setdefault 에 막히지 않고 새 값으로 채워짐)
    for k in [
        "win_pct",
        "btts_pct",
        "team_over05_pct",
        "team_over15_pct",
        "over15_pct",
        "over25_pct",
        "clean_sheet_pct",
        "no_goals_pct",
        "win_and_over25_pct",
        "lose_and_btts_pct",
        "goal_diff_avg",
    ]:
        if k in insights and insights[k] is None:
            del insights[k]

    # fixtures.played.total (API에서 온 경기수) 추출
    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    # 시즌 값
    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    if season_int is not None:
        # ─────────────────────────────
        # Shooting & Efficiency
        # ─────────────────────────────
        try:
            enrich_overall_shooting_efficiency(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                matches_total_api=matches_total_api,
            )
        except Exception:
            # 한 섹션 계산 실패해도 전체 응답은 유지
            pass

        # ─────────────────────────────
        # Outcome & Totals + Result Combos & Draw
        # ─────────────────────────────
        try:
            enrich_overall_outcome_totals(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # ─────────────────────────────
        # Goals by Time (For / Against)
        # ─────────────────────────────
        try:
            enrich_overall_goals_by_time(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # ─────────────────────────────
        # Discipline & Set Pieces (코너/옐/레드 per match)
        # ─────────────────────────────
        try:
            enrich_overall_discipline_setpieces(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                matches_total_api=matches_total_api,
            )
        except Exception:
            pass

        # ─────────────────────────────
        # Timing
        # ─────────────────────────────
        try:
            enrich_overall_timing(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # ─────────────────────────────
        # First Goal & Momentum
        # ─────────────────────────────
        try:
            enrich_overall_firstgoal_momentum(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

    # 최종 반환 구조는 기존과 동일하게 유지
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }


# ─────────────────────────────────────
#  5) 팀 기본 정보
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
