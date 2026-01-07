# hockey/leaguedetail/hockey_bundle_service.py
from __future__ import annotations

from typing import Any, Dict, Optional

from hockey.hockey_db import hockey_fetch_one
from hockey.services.hockey_standings_service import hockey_get_standings

from hockey.leaguedetail.hockey_seasons_block import (
    resolve_season_for_league,
    build_hockey_seasons_block,
)
from hockey.leaguedetail.hockey_results_block import build_hockey_results_block
from hockey.leaguedetail.hockey_fixtures_block import build_hockey_fixtures_block


def _get_league_meta(league_id: int) -> tuple[Optional[str], Optional[str]]:
    """
    hockey_leagues 스키마가 확실치 않아서:
    - name/logo 컬럼이 있으면 그걸 쓰고
    - 없으면 raw_json에서 name/logo를 뽑는다(있다고 가정)
    """
    # 1) name/logo 컬럼 시도
    row = None
    try:
        row = hockey_fetch_one(
            """
            SELECT name, logo
            FROM hockey_leagues
            WHERE id = %s
            LIMIT 1
            """,
            (league_id,),
        )
        if row:
            name = (row.get("name") or "").strip() or None
            logo = (row.get("logo") or "").strip() or None
            return name, logo
    except Exception:
        pass

    # 2) raw_json fallback
    try:
        row = hockey_fetch_one(
            """
            SELECT
              COALESCE(NULLIF(TRIM(raw_json->>'name'), ''), NULL) AS name,
              COALESCE(NULLIF(TRIM(raw_json->>'logo'), ''), NULL) AS logo
            FROM hockey_leagues
            WHERE id = %s
            LIMIT 1
            """,
            (league_id,),
        )
        if row:
            return row.get("name"), row.get("logo")
    except Exception:
        pass

    return None, None


def get_hockey_league_detail_bundle(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    하키 리그디테일 번들:
    - results_block / fixtures_block / standings_block / seasons_block 유지
    - 앱에서 편하게 쓰도록 flatten 필드도 추가
      (league_name, league_logo, standings, seasons, season_champions,
       standingsConferences, standingsGroups)
    """
    resolved_season = resolve_season_for_league(league_id=league_id, season=season)

    seasons_block = build_hockey_seasons_block(league_id=league_id)
    results_block = build_hockey_results_block(league_id=league_id, season=resolved_season)
    fixtures_block = build_hockey_fixtures_block(league_id=league_id, season=resolved_season)

    # ✅ standings 재사용 (이미 구현된 라우터/서비스)
    standings_block = hockey_get_standings(league_id=league_id, season=resolved_season)

    league_name, league_logo = _get_league_meta(league_id)

    standings_rows: Any = []
    standings_conferences: Any = []
    standings_groups: Any = []

    # hockey_get_standings 리턴 형태를 최대한 유연하게 처리
    if isinstance(standings_block, dict):
        # 보통: {"ok":True, "league_id":..., "season":..., "rows":[...], "context_options":{...}}
        standings_rows = standings_block.get("rows", []) or []
        ctx = standings_block.get("context_options", {}) or {}
        standings_conferences = ctx.get("conferences", []) or []
        standings_groups = ctx.get("groups", []) or []

        # standings_block 안에 league_name/logo가 있으면 우선 사용
        league_name = standings_block.get("league_name") or league_name
        league_logo = standings_block.get("league_logo") or league_logo

    seasons_list: Any = []
    season_champions: Any = []
    if isinstance(seasons_block, dict):
        seasons_list = seasons_block.get("seasons", []) or []
        season_champions = seasons_block.get("season_champions", []) or []

    return {
        "league_id": league_id,
        "season": resolved_season,

        # flatten
        "league_name": league_name,
        "league_logo": league_logo,
        "standings": standings_rows,
        "seasons": seasons_list,
        "season_champions": season_champions,
        "standingsConferences": standings_conferences,
        "standingsGroups": standings_groups,

        # blocks 유지(앱 파서 호환)
        "results_block": results_block,
        "fixtures_block": fixtures_block,
        "standings_block": standings_block,
        "seasons_block": seasons_block,
    }
