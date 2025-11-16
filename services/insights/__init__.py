# services/insights/__init__.py
"""
services/insights 패키지

MatchDetail → Insights 탭에서 사용하는
Overall 섹션 관련 계산들을 모은 곳.
"""

from .insights_overall_outcome_totals import enrich_overall_outcome_and_combos
from .insights_overall_shooting_effiency import enrich_overall_shooting_efficiency
from .insights_overall_timing import enrich_overall_timing
from .insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from .insights_overall_goalsbytime import enrich_overall_goals_by_time
from .insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)

__all__ = [
    "enrich_overall_outcome_and_combos",
    "enrich_overall_shooting_efficiency",
    "enrich_overall_timing",
    "enrich_overall_firstgoal_momentum",
    "enrich_overall_goals_by_time",
    "enrich_overall_discipline_setpieces",
]
