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

    ✅ 기존에 이미 잘 되던 구조는 그대로 유지하되,
       앱에서 바로 쓰기 편한 평탄화 필드(league_name, standings, seasons, season_champions, league_logo)를 추가로 내려준다.
    """
    # 1) 시즌 결정 (없으면 최신 시즌)
    resolved_season = resolve_season_for_league(league_id=league_id, season=season)

    # 2) 블록별 데이터 조립 (기존 구조 유지)
    seasons_block = build_seasons_block(league_id=league_id)
    results_block = build_results_block(league_id=league_id, season=resolved_season)
    fixtures_block = build_fixtures_block(league_id=league_id, season=resolved_season)
    standings_block = build_standings_block(league_id=league_id, season=resolved_season)

    # 3) 평탄화용 필드 준비 (새로 추가)
    league_name: Optional[str] = None
    league_logo: Optional[str] = None
    standings_rows: Any = []

    if isinstance(standings_block, dict):
        # leaguedetail/standings_block.py 에서 league_name / rows / league_logo 형태로 내려준다고 가정
        league_name = standings_block.get("league_name")
        league_logo = standings_block.get("league_logo")
        standings_rows = standings_block.get("rows", []) or []
    else:
        standings_rows = []

    seasons_list: Any = []
    season_champions: Any = []

    if isinstance(seasons_block, dict):
        # build_seasons_block 결과가 {"seasons": [...], "season_champions": [...]} 형태라고 가정
        seasons_list = seasons_block.get("seasons", []) or []
        season_champions = seasons_block.get("season_champions", []) or []
    elif isinstance(seasons_block, list):
        # 혹시 리스트 형태면 그대로 사용
        seasons_list = seasons_block
        season_champions = []
    else:
        seasons_list = []
        season_champions = []

    # 4) 최종 번들
    return {
        "league_id": league_id,
        "season": resolved_season,

        # 🔹 새로 추가된 평탄화 필드
        "league_name": league_name,
        "league_logo": standings_block.get("league_logo") if isinstance(standings_block, dict) else None,
        "standings": standings_rows,
        "seasons": seasons_list,
        "season_champions": season_champions,

        # 🔹 기존에 이미 사용하던(또는 나중에 쓸 수 있는) 블록 구조는 그대로 유지
        "results_block": results_block,
        "fixtures_block": fixtures_block,
        "standings_block": standings_block,
        "seasons_block": seasons_block,
    }

