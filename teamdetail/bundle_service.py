# src/teamdetail/bundle_service.py

from __future__ import annotations

from typing import Dict, Any, Optional

from db import fetch_all  # ✅ 추가: DB 헬퍼 사용
from teamdetail.header_block import build_header_block
from teamdetail.overall_block import build_overall_block
from teamdetail.recent_results_block import build_recent_results_block
from teamdetail.standing_block import build_standing_block
from teamdetail.upcoming_block import build_upcoming_block


def _fetch_team_country(team_id: int) -> Optional[str]:
    try:
        rows = fetch_all(
            "SELECT country FROM teams WHERE id=%s",
            (team_id,),
        )
        if rows:
            c = (rows[0].get("country") or "").strip()
            return c or None
    except Exception:
        pass
    return None


def _pick_domestic_league_id(team_id: int, league_id: int, season: int) -> int:
    """
    스탠딩은 항상 '국내리그(domestic)' 기준으로 고정하기 위한 league_id 선택기.

    규칙(안전/단순):
    - 같은 시즌에 해당 팀이 참가한 league_id들 중,
      leagues.country == teams.country 인 리그를 domestic으로 간주
    - 여러 개면: (1) 현재 league_id면 우선, (2) 아니면 가장 많이 경기한 리그(played max)
    - 못 찾으면: 기존 league_id 그대로 반환
    """
    team_country = _fetch_team_country(team_id)
    if not team_country:
        return league_id

    try:
        rows = fetch_all(
            """
            SELECT
              tss.league_id,
              COALESCE(l.country, '') AS league_country,
              COALESCE(
                (tss.value::jsonb #>> '{fixtures,played,total}')::int,
                0
              ) AS played
            FROM team_season_stats tss
            JOIN leagues l ON l.id = tss.league_id
            WHERE tss.team_id = %s
              AND tss.season  = %s
              AND tss.name    = 'full_json'
            """,
            (team_id, season),
        )
    except Exception:
        rows = []

    # domestic 후보만 필터
    domestic = []
    for r in rows:
        lid = int(r.get("league_id") or 0)
        lc = (r.get("league_country") or "").strip()
        played = int(r.get("played") or 0)
        if lid <= 0:
            continue
        if lc and lc == team_country:
            domestic.append((lid, played))

    if not domestic:
        return league_id

    # 1) 현재 league_id가 domestic 후보면 그대로
    for lid, _ in domestic:
        if lid == league_id:
            return league_id

    # 2) 아니면 played가 가장 큰 domestic 리그 선택
    domestic.sort(key=lambda x: x[1], reverse=True)
    return domestic[0][0]


def get_team_detail_bundle(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    Team Detail 화면에서 한 번만 호출하는 번들 빌더.

    ✅ 목표:
    - standing_this_team 은 어떤 진입이든 '국내리그(domestic)' 기준으로 고정
    - 나머지(최근/예정 전체경기)는 다음 단계에서 분리 적용
    """

    header = build_header_block(team_id=team_id, league_id=league_id, season=season)

    # ✅ standing만 domestic league_id로 고정
    standing_league_id = _pick_domestic_league_id(team_id=team_id, league_id=league_id, season=season)

    overall = build_overall_block(team_id=team_id, league_id=league_id, season=season)
    recent_results = build_recent_results_block(
        team_id=team_id, league_id=league_id, season=season
    )
    standing = build_standing_block(team_id=team_id, league_id=standing_league_id, season=season)
    upcoming = build_upcoming_block(team_id=team_id, league_id=league_id, season=season)

    bundle: Dict[str, Any] = {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "header": header,
        "overall": overall,
        "recent_results": recent_results,
        "standing_this_team": standing,
        "upcoming_fixtures": upcoming,
    }

    return bundle
