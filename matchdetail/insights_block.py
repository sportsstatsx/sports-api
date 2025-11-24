# matchdetail/insights_block.py

from __future__ import annotations

from typing import Any, Dict, Optional

from services.insights.insights_overall_outcome_totals import (
    enrich_overall_outcome_totals,
)
from services.insights.insights_overall_timing import enrich_overall_timing
from services.insights.insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from services.insights.insights_overall_shooting_efficiency import (
    enrich_overall_shooting_efficiency,
)
from services.insights.insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)
from services.insights.insights_overall_goalsbytime import (
    enrich_overall_goals_by_time,
)
from services.insights.insights_overall_resultscombos_draw import (
    enrich_overall_resultscombos_draw,
)
from services.insights.utils import parse_last_n


def _extract_int(value: Any) -> Optional[int]:
    """헤더에서 넘어오는 값이 str/int 섞여 있어도 안전하게 int로 변환."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_team_ids_from_header(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """header 구조 변화에 어느 정도 대응할 수 있게 방어적으로 파싱."""
    teams = header.get("teams") or {}

    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None

    # 1) header["teams"]["home"]["id"] / ["away"]["id"] 패턴 시도
    if isinstance(teams, dict):
        home_team_id = _extract_int((teams.get("home") or {}).get("id"))
        away_team_id = _extract_int((teams.get("away") or {}).get("id"))

    # 2) fallback: header["home_team_id"] / ["away_team_id"]
    if home_team_id is None:
        home_team_id = _extract_int(header.get("home_team_id"))
    if away_team_id is None:
        away_team_id = _extract_int(header.get("away_team_id"))

    return {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
    }


def _get_league_and_season_from_header(
    header: Dict[str, Any]
) -> Dict[str, Optional[int]]:
    league_id = header.get("league_id")
    if league_id is None:
        league = header.get("league") or {}
        league_id = league.get("id")

    season = header.get("season_int")
    if season is None:
        season = header.get("season")

    return {
        "league_id": _extract_int(league_id),
        "season_int": _extract_int(season),
    }


def _get_last_n_from_header(header: Dict[str, Any]) -> int:
    """
    헤더 안에 필터 정보가 있다면 last_n (정수) 로 변환.
    없으면 0 (시즌 전체)로 처리.
    """
    filters = header.get("filters") or {}
    # 키 이름이 last_n / lastN 둘 중 무엇이든 최대한 대응
    raw_last_n = (
        filters.get("last_n")
        or filters.get("lastN")
        or header.get("last_n")
        or header.get("lastN")
    )
    return parse_last_n(raw_last_n)


def _build_side_insights(
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: int,
) -> Dict[str, Any]:
    """
    한 팀(홈/원정)에 대해 overall insights 를 계산해서 dict 로 리턴.
    stats 딕셔너리는 섹션들에서 공통으로 사용할 메타/필터 저장소로 활용 가능.
    """
    stats: Dict[str, Any] = {}
    insights: Dict[str, Any] = {}

    # Outcome & Totals / Goal Diff / Clean Sheet / No Goals / 일부 Result Combos
    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    # Timing (득점/실점 시간대)
    enrich_overall_timing(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # First Goal / Momentum
    enrich_overall_firstgoal_momentum(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # Shooting & Efficiency
    enrich_overall_shooting_efficiency(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    # Discipline & Set Pieces
    enrich_overall_discipline_setpieces(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    # Goals by Time
    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    # Result Combos & Draw (현재는 별도 작업 없음, 훅만 남겨둠)
    enrich_overall_resultscombos_draw(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
    )

    return insights


def build_insights_overall_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    매치 디테일 번들에서 사용할 전체 Insights Overall 블록 생성.

    입력: header_block 에서 만들어준 header 딕셔너리
    출력: league/season/필터 + 홈/원정 인사이트가 들어있는 딕셔너리

      예시:
      {
          "league_id": 39,
          "season": 2025,
          "last_n": 10,
          "home_team_id": 40,
          "away_team_id": 33,
          "home": { ... },
          "away": { ... },
      }
    """
    if not header:
        return None

    meta = _get_league_and_season_from_header(header)
    league_id = meta["league_id"]
    season_int = meta["season_int"]

    teams = _get_team_ids_from_header(header)
    home_team_id = teams["home_team_id"]
    away_team_id = teams["away_team_id"]

    # 필수 값이 하나라도 없으면 인사이트 블록을 만들 수 없으므로 None
    if league_id is None or season_int is None:
        return None
    if home_team_id is None or away_team_id is None:
        return None

    last_n = _get_last_n_from_header(header)

    home_insights = _build_side_insights(
        league_id=league_id,
        season_int=season_int,
        team_id=home_team_id,
        last_n=last_n,
    )
    away_insights = _build_side_insights(
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
        "home": home_insights,
        "away": away_insights,
    }
