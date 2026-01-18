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
    ✅ 완전무결 시즌 처리:
    - resolved_season: resolve_season_for_league()가 검증/보정까지 책임
    - season_label: DB의 (min_dt, max_dt) 연도로 자동 생성
      * 캘린더 시즌: "2026"
      * 스플릿 시즌: "2025-2026"
    """
    resolved_season = resolve_season_for_league(league_id=league_id, season=season)

    def _year_of(v: Any) -> Optional[int]:
        if v is None:
            return None
        y = getattr(v, "year", None)
        if y is not None:
            try:
                return int(y)
            except Exception:
                return None
        if isinstance(v, str) and len(v) >= 4 and v[:4].isdigit():
            try:
                return int(v[:4])
            except Exception:
                return None
        return None

    season_label: Optional[str] = None
    if resolved_season is not None:
        row = fetch_one(
            """
            SELECT
              MIN(date_utc::timestamptz) AS min_dt,
              MAX(date_utc::timestamptz) AS max_dt
            FROM matches
            WHERE league_id = %s
              AND season = %s
            """,
            (league_id, resolved_season),
        )
        if row:
            min_y = _year_of(row.get("min_dt"))
            max_y = _year_of(row.get("max_dt"))
            if min_y is not None and max_y is not None:
                season_label = str(min_y) if min_y == max_y else f"{min_y}-{max_y}"

        if not season_label:
            season_label = str(resolved_season)

    seasons_block = build_seasons_block(league_id=league_id)
    results_block = build_results_block(league_id=league_id, season=resolved_season)
    fixtures_block = build_fixtures_block(league_id=league_id, season=resolved_season)
    standings_block = build_standings_block(league_id=league_id, season=resolved_season)

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

    if isinstance(season_champions, list) and isinstance(standings_rows, list):
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

    ctx_opts = standings_block.get("context_options", {}) if isinstance(standings_block, dict) else {}

    return {
        "league_id": league_id,
        "season": resolved_season,
        "season_label": season_label,

        "league_name": league_name,
        "league_logo": league_logo,
        "standings": standings_rows,
        "seasons": seasons_list,
        "season_champions": season_champions,

        "standingsConferences": (ctx_opts.get("conferences", []) or []),
        "standingsGroups": (ctx_opts.get("groups", []) or []),

        "results_block": results_block,
        "fixtures_block": fixtures_block,
        "standings_block": standings_block,
        "seasons_block": seasons_block,
    }




