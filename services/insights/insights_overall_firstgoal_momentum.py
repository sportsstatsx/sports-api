from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_firstgoal_momentum(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Insights Overall - First Goal / Momentum 섹션 계산용.

    예:
      - first_to_score_pct
      - first_conceded_pct
      - when_leading_win_pct / draw_pct / loss_pct
      - when_trailing_win_pct / draw_pct / loss_pct

    다음 단계에서:
      - match_events 및 전체 스코어(gf/ga)를 이용해
        선제골/선제 실점 여부와 리드/열세 상황의 최종 결과 비율을
        이 함수 안에서 계산하도록 구현할 예정.
    """
    # TODO: match_events 기반 First Goal / Momentum 계산 로직을 여기로 옮기기
    return
