# services/insights/football_insights_overall.py
"""
⚠️ 임시 브릿지(빌드 복구용)

현재 home_service.py / bundle_service.py 가 이 모듈을 import 하도록 변경된 상태인데,
GitHub에 파일이 없어서 배포가 깨지고 있음.

일단 이 파일로 "기존 분리 구현"을 그대로 재-export 해서 서비스부터 살린 뒤,
다음 단계에서 실제로 '한 파일로 완전 병합' + 기존 파일 삭제를 진행한다.
"""

from __future__ import annotations

# re-export: home_service가 쓰는 함수들
from .insights_overall_shooting_efficiency import enrich_overall_shooting_efficiency
from .insights_overall_outcome_totals import enrich_overall_outcome_totals
from .insights_overall_goalsbytime import enrich_overall_goals_by_time
from .insights_overall_timing import enrich_overall_timing
from .insights_overall_firstgoal_momentum import enrich_overall_firstgoal_momentum
from .insights_overall_discipline_setpieces import enrich_overall_discipline_setpieces

# re-export: matchdetail/insights_block이 쓰는 유틸
from .utils import normalize_comp, parse_last_n

# re-export: bundle_service가 쓰는 entrypoint
from matchdetail.insights_block import build_insights_overall_block
