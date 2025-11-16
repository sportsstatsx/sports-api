from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_timing(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Insights Overall - Timing 섹션 계산용.

    예: 전반/후반 득점·실점, 0–15분, 80+분 득·실점 비율 등.

    지금은 서버에서 Timing을 직접 계산하지 않고,
    team_season_stats.value.insights_overall 에 이미 들어 있는 값을
    그대로 사용하는 상태일 수 있다.

    다음 단계에서:
      - match_events 기반 Timing 계산 로직을 구현해서
        insights["score_1h_pct"], "concede_1h_pct", "score_0_15_pct" 등
        관련 키들을 이 함수 안에서 채우는 구조로 옮길 예정.
    """
    # TODO: match_events 기반 Timing 계산 로직을 구현하고 여기로 넣기
    return
