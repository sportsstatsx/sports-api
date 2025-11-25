# matchdetail/insights_block.py

from __future__ import annotations
from typing import Any, Dict, Optional

from services.insights.insights_overall_outcome_totals import enrich_overall_outcome_totals
from services.insights.insights_overall_timing import enrich_overall_timing
from services.insights.insights_overall_firstgoal_momentum import enrich_overall_firstgoal_momentum
from services.insights.insights_overall_shooting_efficiency import enrich_overall_shooting_efficiency
from services.insights.insights_overall_discipline_setpieces import enrich_overall_discipline_setpieces
from services.insights.insights_overall_goalsbytime import enrich_overall_goals_by_time
from services.insights.insights_overall_resultscombos_draw import enrich_overall_resultscombos_draw
from services.insights.utils import parse_last_n


# ─────────────────────────────────────
#  안전한 int 변환
# ─────────────────────────────────────
def _extract_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


# ─────────────────────────────────────
#  header 구조 그대로 파싱
# ─────────────────────────────────────
def _get_meta_from_header(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    header 스키마에 100% 맞게 파싱:
      - league_id → header["league_id"]
      - season → header["season"]
      - home_team_id → header["home"]["id"]
      - away_team_id → header["away"]["id"]
    """
    league_id = _extract_int(header.get("league_id"))
    season = _extract_int(header.get("season"))

    # home/away 구조는 정확히 header["home"]["id"]
    home_block = header.get("home") or {}
    away_block = header.get("away") or {}

    home_team_id = _extract_int(home_block.get("id"))
    away_team_id = _extract_int(away_block.get("id"))

    return {
        "league_id": league_id,
        "season_int": season,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
    }


def _get_last_n_from_header(header: Dict[str, Any]) -> int:
    filters = header.get("filters") or {}
    raw_last_n = filters.get("last_n") or header.get("last_n")
    return parse_last_n(raw_last_n)


# ─────────────────────────────────────
#  한 팀(홈/원정) 계산
# ─────────────────────────────────────
def _build_side_insights(*, league_id: int, season_int: int, team_id: int, last_n: int):
    stats: Dict[str, Any] = {}
    insights: Dict[str, Any] = {}

    enrich_overall_outcome_totals(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    enrich_overall_timing(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    enrich_overall_firstgoal_momentum(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    enrich_overall_shooting_efficiency(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    enrich_overall_discipline_setpieces(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    enrich_overall_goals_by_time(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    enrich_overall_resultscombos_draw(
        stats, insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
    )

    return insights


# ─────────────────────────────────────
#  전체 insights 블록 생성
# ─────────────────────────────────────
def build_insights_overall_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not header:
        return None

    meta = _get_meta_from_header(header)

    league_id = meta["league_id"]
    season_int = meta["season_int"]
    home_team_id = meta["home_team_id"]
    away_team_id = meta["away_team_id"]

    # 값 못 찾으면 None
    if None in (league_id, season_int, home_team_id, away_team_id):
        return None

    last_n = _get_last_n_from_header(header)

    home_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_int,
        team_id=home_team_id,
        last_n=last_n,
    )
    away_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_int,
        team_id=away_team_id,
        last_n=last_n,
    )

    return {
        "league_id": league_id,
        "season": season_int,
        "last_n": last_n,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home": home_ins,
        "away": away_ins,
    }
