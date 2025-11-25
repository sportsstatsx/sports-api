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
#  Header → 공통 메타 추출
# ─────────────────────────────────────

def _get_meta_from_header(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    header 블록에서 league_id / season / home_team_id / away_team_id 추출.
    값이 없으면 None.
    """
    league_id = header.get("league_id")
    season = header.get("season")
    home_team_id = header.get("home_team_id")
    away_team_id = header.get("away_team_id")

    league_id_int = None
    season_int = None
    home_id_int = None
    away_id_int = None

    try:
        if league_id is not None:
            league_id_int = int(league_id)
    except (TypeError, ValueError):
        league_id_int = None

    try:
        if season is not None:
            season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    try:
        if home_team_id is not None:
            home_id_int = int(home_team_id)
    except (TypeError, ValueError):
        home_id_int = None

    try:
        if away_team_id is not None:
            away_id_int = int(away_team_id)
    except (TypeError, ValueError):
        away_id_int = None

    return {
        "league_id": league_id_int,
        "season_int": season_int,
        "home_team_id": home_id_int,
        "away_team_id": away_id_int,
    }


def _get_last_n_from_header(header: Dict[str, Any]) -> int:
    """
    header 에 last_n 문자열(예: 'Last 5') 이 있으면 파싱해서 int 로 반환.
    없으면 기본 10.
    """
    raw = header.get("last_n")
    n = parse_last_n(raw)
    if not n:
        n = 10
    return n


# ─────────────────────────────────────
#  한 팀(home/away) Insights 계산
# ─────────────────────────────────────

def _build_side_insights(
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: int,
) -> Dict[str, Any]:
    """
    한 팀(홈/원정)에 대한 전체 insights를 계산해서 합쳐서 반환.
    stats / insights 딕셔너리를 shared 버퍼로 사용하고,
    각 섹션별 enrich 함수가 거기에 값을 채워 넣는 구조.
    """
    stats: Dict[str, Any] = {}
    insights: Dict[str, Any] = {}

    # Outcome & Totals
    enrich_overall_outcome_totals(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
        matches_total_api=0,
    )

    # Results Combos & Draw
    enrich_overall_resultscombos_draw(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
        matches_total_api=0,
    )

    # Timing
    enrich_overall_timing(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # First Goal / Momentum
    enrich_overall_firstgoal_momentum(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # Shooting & Efficiency
    enrich_overall_shooting_efficiency(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # Discipline & Set Pieces
    enrich_overall_discipline_setpieces(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # Goals by Time
    enrich_overall_goals_by_time(
        stats=stats,
        insights=insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    return insights


# ─────────────────────────────────────
#  전체 insights 블록 생성
# ─────────────────────────────────────

def build_insights_overall_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail 번들에서 사용할 insights_overall 블록을 생성한다.
    (home/away 각각에 대해 _build_side_insights 호출)
    """
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
