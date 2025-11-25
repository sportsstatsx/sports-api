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
#  last_n 파싱
# ─────────────────────────────────────

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
    league_id: Optional[int],
    season_int: Optional[int],
    team_id: Optional[int],
    last_n: int,
) -> Dict[str, Any]:
    """
    한 팀(홈/원정)에 대한 전체 insights 계산.
    league_id / season_int / team_id 가 하나라도 None 이면
    그냥 빈 dict 반환해서 앱이 죽지 않게 한다.
    """
    if league_id is None or team_id is None:
        return {}

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
    절대 None 을 반환하지 않고, 최소한 빈 home/away 객체라도 내려보낸다.
    """
    if not header:
        return None

    # header 구조에 맞게 값 가져오기
    league_id = header.get("league_id")
    season = header.get("season")

    home = header.get("home") or {}
    away = header.get("away") or {}

    home_team_id = home.get("id")
    away_team_id = away.get("id")

    # 안전하게 int 캐스팅 (실패해도 None 으로 두고 빈 dict 반환)
    league_id_int: Optional[int]
    season_int: Optional[int]
    home_id_int: Optional[int]
    away_id_int: Optional[int]

    try:
        league_id_int = int(league_id) if league_id is not None else None
    except (TypeError, ValueError):
        league_id_int = None

    try:
        season_int = int(season) if season is not None else None
    except (TypeError, ValueError):
        season_int = None

    try:
        home_id_int = int(home_team_id) if home_team_id is not None else None
    except (TypeError, ValueError):
        home_id_int = None

    try:
        away_id_int = int(away_team_id) if away_team_id is not None else None
    except (TypeError, ValueError):
        away_id_int = None

    last_n = _get_last_n_from_header(header)

    # 실제 계산 (값이 None 이면 _build_side_insights 내부에서 빈 dict 반환)
    home_ins = _build_side_insights(
        league_id=league_id_int,
        season_int=season_int,
        team_id=home_id_int,
        last_n=last_n,
    )
    away_ins = _build_side_insights(
        league_id=league_id_int,
        season_int=season_int,
        team_id=away_id_int,
        last_n=last_n,
    )

    # ✅ 여기서는 절대 None 안 돌려줌
    return {
        "league_id": league_id_int,
        "season": season_int,
        "last_n": last_n,
        "home_team_id": home_id_int,
        "away_team_id": away_id_int,
        "home": home_ins,
        "away": away_ins,
    }
