# hockey/teamdetail/hockey_team_detail_bundle_service.py

from typing import Dict, Any

from hockey.teamdetail.blocks.hockey_header_block import build_hockey_header_block
from hockey.teamdetail.blocks.hockey_standing_block import build_hockey_standing_block
from hockey.teamdetail.blocks.hockey_recent_results_block import build_hockey_recent_results_block
from hockey.teamdetail.blocks.hockey_upcoming_block import build_hockey_upcoming_block


def build_hockey_team_detail_bundle(
    *,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    축구 team_detail_bundle 과 JSON 구조 100% 동일
    """

    header = build_hockey_header_block(
        team_id=team_id,
        league_id=league_id,
        season=season,
    )

    standings = build_hockey_standing_block(
        team_id=team_id,
        league_id=league_id,
        season=season,
    )

    recent_results = build_hockey_recent_results_block(
        team_id=team_id,
        league_id=league_id,
        season=season,
        limit=10,
    )

    upcoming_fixtures = build_hockey_upcoming_block(
        team_id=team_id,
        league_id=league_id,
        season=season,
        limit=10,
    )

    return {
        "header": header,
        "standings": standings,
        "recent_results": recent_results,
        "upcoming_fixtures": upcoming_fixtures,
    }
