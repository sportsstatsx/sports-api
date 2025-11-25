# matchdetail/bundle_service.py

from typing import Any, Dict, Optional

from .header_block import build_header_block
from .form_block import build_form_block
from .timeline_block import build_timeline_block
from .lineups_block import build_lineups_block
from .stats_block import build_stats_block
from .h2h_block import build_h2h_block
from .standings_block import build_standings_block
from .insights_block import build_insights_overall_block
from .ai_predictions_block import build_ai_predictions_block


def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
) -> Optional[Dict[str, Any]]:
    """
    Match Detail 화면에서 사용하는 번들 JSON 전체를 만든다.

    - header
    - form
    - timeline
    - lineups
    - stats
    - h2h
    - standings
    - insights_overall
    - ai_predictions
    """

    # 1) 헤더 블록 (fixture_id / league_id / season 기반)
    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if not header:
        return None

    # 2) Form (최근 경기 성적 등)
    form = build_form_block(header)

    # 3) Timeline
    timeline = build_timeline_block(header)

    # 4) Lineups
    lineups = build_lineups_block(header)

    # 5) Stats
    stats = build_stats_block(header)

    # 6) H2H
    h2h = build_h2h_block(header)

    # 7) Standings
    standings = build_standings_block(header)

    # 8) Insights Overall (Outcome & Totals / Timing / Goals by Time 등)
    insights_overall = build_insights_overall_block(header)

    # 9) AI Predictions (Insights를 일부 활용)
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    # 10) 최종 번들
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
