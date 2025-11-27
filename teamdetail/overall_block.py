# src/teamdetail/overall_block.py

from __future__ import annotations
from typing import Dict, Any


def build_overall_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 에서 전반적인 인사이트(Outcome & Totals, Goals, Goals by time 등)를
    보여줄 때 사용할 블록.

    나중에:
      - 기존 /api/home/team_insights_overall 에서 사용하던 내부 로직을
        재사용하거나,
      - 같은 계산 유틸을 호출해서 team 단위 통계를 뽑을 거야.
    """

    return {
        # 샘플 수 (경기 개수) 정보
        "events_sample": {
            "total_matches": 0,
            "per_team_matches": 0,
        },
        # 득점/실점 평균
        "goals": {
            "avg_for": 0.0,
            "avg_against": 0.0,
        },
        # 시간대별 득실점 (0~90분, 10개 버킷 예시)
        "goals_by_time": {
            "for_buckets": [0] * 10,
            "against_buckets": [0] * 10,
        },
        # 나중에 Outcome/Total, Shooting & Efficiency 등 추가 가능
    }
