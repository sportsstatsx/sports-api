# services/insights/__init__.py
from __future__ import annotations

"""
Insights 패키지.

여기서는 패키지 마커 역할만 하고,
개별 모듈은 필요한 곳(예: home_service)에서 직접 import 한다.
"""

__all__ = [
    "insights_overall_shooting_efficiency",
    "insights_overall_outcome_totals",
    "insights_overall_goalsbytime",
    "insights_overall_timing",
    "insights_overall_firstgoal_momentum",
    "insights_overall_discipline_setpieces",
]
