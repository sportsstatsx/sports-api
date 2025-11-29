# leaguedetail/bundle_service.py
from __future__ import annotations

from typing import Any, Dict, Optional

from leaguedetail.results_block import build_results_block
from leaguedetail.fixtures_block import build_fixtures_block
from leaguedetail.standings_block import build_standings_block
from leaguedetail.seasons_block import (
    build_seasons_block,
    resolve_season_for_league,
)


def get_league_detail_bundle(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면에서 한 번만 호출하는 번들 빌더.

    - league_id: 리그 ID (필수)
    - season: 쿼리에서 넘어온 시즌 (없으면 DB에서 최신 시즌 선택)
    """
    # 1) 시즌 결정 (없으면 최신 시즌)
    resolved_season = resolve_season_for_league(league_id=league_id, season=season)

    # 2) 블록별 데이터 조립
    seasons_block = build_seasons_block(league_id=league_id)
    results_block = build_results_block(league_id=league_id, season=resolved_season)
    fixtures_block = build_fixtures_block(league_id=league_id, season=resolved_season)
    standings_block = build_standings_block(league_id=league_id, season=resolved_season)

    return {
        "league_id": league_id,
        "season": resolved_season,
        "results_block": results_block,
        "fixtures_block": fixtures_block,
        "standings_block": standings_block,
        "seasons_block": seasons_block,
    }
