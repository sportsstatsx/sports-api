# src/teamdetail/bundle_service.py

from __future__ import annotations

from typing import Dict, Any

from teamdetail.header_block import build_header_block
from teamdetail.overall_block import build_overall_block
from teamdetail.recent_results_block import build_recent_results_block
from teamdetail.standing_block import build_standing_block
from teamdetail.upcoming_block import build_upcoming_block


def get_team_detail_bundle(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    Team Detail 화면에서 한 번만 호출하는 번들 빌더.

    지금은 A방식 틀만 만드는 단계라
    각 블록은 일단 빈 껍데기(dict)만 리턴하게 해두고,
    이후 단계에서 하나씩 DB 쿼리/로직을 채워 넣을 거야.
    """

    header = build_header_block(team_id=team_id, league_id=league_id, season=season)
    overall = build_overall_block(team_id=team_id, league_id=league_id, season=season)
    recent_results = build_recent_results_block(
        team_id=team_id, league_id=league_id, season=season
    )
    standing = build_standing_block(team_id=team_id, league_id=league_id, season=season)
    upcoming = build_upcoming_block(team_id=team_id, league_id=league_id, season=season)

    bundle: Dict[str, Any] = {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        # 각 섹션 블록
        "header": header,
        "overall": overall,
        "recent_results": recent_results,
        "standing_this_team": standing,
        "upcoming_fixtures": upcoming,
    }

    return bundle
