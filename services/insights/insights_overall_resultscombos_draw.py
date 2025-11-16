# services/insights/insights_overall_resultscombos_draw.py
from __future__ import annotations

from typing import Any, Dict, Optional


def enrich_overall_resultscombos_draw(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
) -> None:
    """
    Insights Overall - Results Combos & Draw.

    현재 구현에서는 Outcome & Totals 모듈(enrich_overall_outcome_totals)이
    win_and_over25_pct / lose_and_btts_pct 까지 처리하고 있기 때문에,
    이 함수는 별도 작업을 하지 않는다.

    나중에 '무승부 + 언더' 같은 추가 콤보 지표를 넣고 싶으면
    이 함수 안에서 구현하면 된다.
    """
    return
