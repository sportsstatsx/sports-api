from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_one
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
       앱에서 바로 쓰기 편한 평탄화 필드
       (league_name, league_logo, standings, seasons, season_champions)를 추가로 내려준다.
    """
    # 1) 시즌 결정 (없으면 최신 시즌)
    resolved_season = resolve_season_for_league(league_id=league_id, season=season)

    # 2) 블록별 데이터 조립 (기존 구조 유지)
    seasons_block = build_seasons_block(league_id=league_id)
    results_block = build_results_block(league_id=league_id, season=resolved_season)
    fixtures_block = build_fixtures_block(league_id=league_id, season=resolved_season)
    standings_block = build_standings_block(league_id=league_id, season=resolved_season)

    # 3) 평탄화용 필드 준비
    league_name: Optional[str] = None
    league_logo: Optional[str] = None
    standings_rows: Any = []

    if isinstance(standings_block, dict):
        league_name = standings_block.get("league_name")
        league_logo = standings_block.get("league_logo")
        standings_rows = standings_block.get("rows", []) or []
    else:
        standings_rows = []

    seasons_list: Any = []
    season_champions: Any = []

    if isinstance(seasons_block, dict):
        seasons_list = seasons_block.get("seasons", []) or []
        season_champions = seasons_block.get("season_champions", []) or []
    elif isinstance(seasons_block, list):
        seasons_list = seasons_block
        season_champions = []
    else:
        seasons_list = []
        season_champions = []

    # 3-1) standings_block 에 league_logo 가 없으면 → leagues 테이블에서 logo 가져오기
    if not league_logo:
        row = fetch_one(
            """
            SELECT logo
            FROM leagues
            WHERE id = %s
            LIMIT 1
            """,
            (league_id,),
        )
        if row:
            logo_from_db = row.get("logo")
            if logo_from_db:
                league_logo = logo_from_db

    # 3-2) 시즌 챔피언에 team_logo 채워넣기
    # standings_rows 에는 team_id / team_logo 가 들어 있으므로,
    # 같은 team_id 를 가진 챔피언에게 team_logo 를 복사해준다.
    if isinstance(season_champions, list) and isinstance(standings_rows, list):
        # team_id → team_logo 매핑 생성
        logo_by_team_id: Dict[int, str] = {}
        for row in standings_rows:
            if not isinstance(row, dict):
                continue
            tid = row.get("team_id")
            tlogo = row.get("team_logo")
            if tid is not None and tlogo:
                logo_by_team_id[int(tid)] = tlogo

        enriched_champions: list[Any] = []
        for champ in season_champions:
            if not isinstance(champ, dict):
                enriched_champions.append(champ)
                continue

            tid = champ.get("team_id")
            existing_logo = champ.get("team_logo")
            logo = existing_logo

            if not logo and tid is not None:
                logo = logo_by_team_id.get(int(tid))

            if logo and logo != existing_logo:
                new_champ = dict(champ)
                new_champ["team_logo"] = logo
                enriched_champions.append(new_champ)
            else:
                enriched_champions.append(champ)

        season_champions = enriched_champions

    # 4) 최종 번들
    return {
        "league_id": league_id,
        "season": resolved_season,

        # 🔹 새로 추가된 평탄화 필드
        "league_name": league_name,
        "league_logo": league_logo,
        "standings": standings_rows,
        "seasons": seasons_list,
        "season_champions": season_champions,

        # 🔹 기존 블록 구조도 그대로 유지
        "results_block": results_block,
        "fixtures_block": fixtures_block,
        "standings_block": standings_block,
        "seasons_block": seasons_block,
    }
