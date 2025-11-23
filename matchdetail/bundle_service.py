# services/matchdetail/bundle_service.py

from typing import Any, Dict, Optional

from services.matchdetail.header_block import build_header_block
from services.matchdetail.form_block import build_form_block
from services.matchdetail.timeline_block import build_timeline_block
from services.matchdetail.lineups_block import build_lineups_block
from services.matchdetail.stats_block import build_stats_block
from services.matchdetail.h2h_block import build_h2h_block
from services.matchdetail.standings_block import build_standings_block
from services.matchdetail.insights_block import build_insights_overall_block
from services.matchdetail.ai_predictions_block import build_ai_predictions_block


def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
) -> Optional[Dict[str, Any]]:
    """
    매치디테일 번들의 진입점 (sync 버전).
    여기서는 각 블록 빌더들을 호출만 하고,
    실제 쿼리/계산은 각 *_block.py 에서 처리한다.
    """

    # 1) header: fixture + 팀 정보 + 킥오프 + 스코어 + 상태
    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if header is None:
        return None

    # 2) 나머지 블록은 header에 들어있는 정보(home_id, away_id 등)를 활용
    form = build_form_block(header)
    timeline = build_timeline_block(header)
    lineups = build_lineups_block(header)
    stats = build_stats_block(header)
    h2h = build_h2h_block(header)
    standings = build_standings_block(header)
    insights_overall = build_insights_overall_block(header)
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    return {
        "header": header,
        "form": form,
        "timeline": timeline,
        "lineups": lineups,
        "stats": stats,
        "h2h": h2h,
        "standings": standings,
        "insights_overall": insights_overall,
        "ai_predictions": ai_predictions,
    }
