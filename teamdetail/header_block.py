# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List

import json
from db import fetch_all  # matchdetail 쪽에서 쓰는 DB 헬퍼와 동일하게 사용


def _default_header(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    기본 스켈레톤. DB 조회 실패해도 이 구조는 항상 유지.
    """
    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,

        "team_name": None,
        "team_short_name": None,
        "team_logo": None,

        # 👇 UI에서 바로 쓰는 리그 이름 (항상 '국내 리그' 기준으로 채울 것)
        "league_name": None,
        "season_label": str(season),

        "position": None,
        "played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        "goal_diff": 0,

        # 최근 10경기 (왼쪽이 예전, 오른쪽이 최신)
        "recent_form": [],

        # 👇 매치 수 분리 정보
        "domestic_league_id": None,
        "domestic_league_name": None,
        "domestic_matches": 0,

        "continental_league_id": None,
        "continental_league_name": None,
        "continental_matches": 0,
    }


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 상단 헤더 영역에 쓸 정보.

    - teams               : 팀명 / 로고 / 국가
    - team_season_stats   : 시즌별 리그/컵 스탯 (full_json)
    - leagues             : 각 대회의 이름/국가
    - matches             : 실제 경기 결과 → cross-comp 최근 폼
    """
    header: Dict[str, Any] = _default_header(team_id, league_id, season)

    # ─────────────────────────────────────
    # 1) 팀 기본 정보 (이름 / 로고 / 국가)
    # ─────────────────────────────────────
    team_country: str | None = None
    try:
        rows = fetch_all(
            "SELECT name, country, logo FROM teams WHERE id = %s",
            (team_id,),
        )
        row = rows[0] if rows else None
        if row:
            header["team_name"] = row.get("name")
            header["team_short_name"] = row.get("name")  # 나중에 별도 단축명 생기면 수정
            header["team_logo"] = row.get("logo")
            team_country = (row.get("country") or "").strip() or None
    except Exception as e:
        print(f"[teamdetail.header_block] team query failed: {e}")

    # ─────────────────────────────────────
    # 2) 이 시즌에 이 팀이 참가한 모든 대회 stats + 리그 정보
    #    (라리가 / 프리미어리그 / 챔스 / 유로파 … 전부)
    # ─────────────────────────────────────
    stats_rows: List[dict] = []
    try:
        stats_rows = fetch_all(
            """
            SELECT
              tss.league_id,
              l.name    AS league_name,
              l.country AS league_country,
              tss.value
            FROM team_season_stats AS tss
            JOIN leagues AS l
              ON tss.league_id = l.id
            WHERE tss.season  = %s
              AND tss.team_id = %s
              AND tss.name    = 'full_json'
            """,
            (season, team_id),
        )
    except Exception as e:
        print(f"[teamdetail.header_block] team_season_stats query failed: {e}")

    # 국내 리그(라리가/프리미어 등) 후보 & 대륙컵(챔스/유로파 등) 후보
    # → "해당 국가 + 가장 많이 뛴 대회"를 메인 domestic 으로 본다.
    domestic_best: tuple[dict, int, dict] | None = None  # (row, played, parsed_json)
    continental_best: tuple[dict, int, dict] | None = None

    for row in stats_rows or []:
        raw_json = row.get("value")
        if not isinstance(raw_json, str):
            continue

        try:
            data = json.loads(raw_json)
        except Exception:
            continue

        fixtures = data.get("fixtures") or {}
        played_total = ((fixtures.get("played") or {}).get("total")) or 0
        try:
            played_int = int(played_total)
        except Exception:
            played_int = 0

        league_country = (row.get("league_country") or "").strip() or None

        # 국내 vs 대륙/국제 대회 판별
        is_domestic = bool(team_country and league_country and (team_country == league_country))
        is_continental = not is_domestic  # 나머지는 전부 대륙/국제 대회로 취급

        if is_domestic:
            # 가장 많이 뛴 국내 대회를 "메인 리그"로 사용 (라리가 / 프리미어 등)
            if domestic_best is None or played_int > domestic_best[1]:
                domestic_best = (row, played_int, data)

        if is_continental:
            # 가장 많이 뛴 대륙컵 하나만 잡아준다 (챔스/유로파 등)
            if continental_best is None or played_int > continental_best[1]:
                continental_best = (row, played_int, data)

    # ─────────────────────────────────────
    # 2-1) 메인 국내 리그 정보 → 헤더 기본값 채우기
    #      (팀디테일 상단 리그 이름은 항상 이 값 기준)
    # ─────────────────────────────────────
    if domestic_best is not None:
        row, played_int, data = domestic_best

        header["domestic_league_id"] = row.get("league_id")
        header["domestic_league_name"] = row.get("league_name")
        header["league_name"] = row.get("league_name")  # UI에서 쓰는 리그 이름
        header["played"] = played_int
        header["domestic_matches"] = played_int

        fixtures = data.get("fixtures") or {}
        wins_total = ((fixtures.get("wins") or {}).get("total")) or 0
        draws_total = ((fixtures.get("draws") or {}).get("total")) or 0
        loses_total = ((fixtures.get("loses") or {}).get("total")) or 0

        goals = data.get("goals") or {}
        goals_for_total = (
            ((goals.get("for") or {}).get("total") or {}).get("total")
        ) or 0
        goals_against_total = (
            ((goals.get("against") or {}).get("total") or {}).get("total")
        ) or 0

        try:
            header["wins"] = int(wins_total)
        except Exception:
            header["wins"] = 0
        try:
            header["draws"] = int(draws_total)
        except Exception:
            header["draws"] = 0
        try:
            header["losses"] = int(loses_total)
        except Exception:
            header["losses"] = 0
        try:
            gf = int(goals_for_total)
        except Exception:
            gf = 0
        try:
            ga = int(goals_against_total)
        except Exception:
            ga = 0

        header["goals_for"] = gf
        header["goals_against"] = ga
        header["goal_diff"] = gf - ga

    # ─────────────────────────────────────
    # 2-2) 대륙컵(챔스/유로파 등) 정보
    # ─────────────────────────────────────
    if continental_best is not None:
        row, played_int, _data = continental_best
        header["continental_league_id"] = row.get("league_id")
        header["continental_league_name"] = row.get("league_name")
        header["continental_matches"] = played_int

    # ─────────────────────────────────────
    # 3) 최근 10경기 폼 (대회 구분 없이, season 안에서)
    #    오른쪽이 가장 최근 경기가 되도록 순서 정리
    # ─────────────────────────────────────
    try:
        match_rows = fetch_all(
            """
            SELECT
              fixture_id,
              league_id,
              date_utc,
              home_id,
              away_id,
              home_ft,
              away_ft
            FROM matches
            WHERE season = %s
              AND (home_id = %s OR away_id = %s)
              AND status_group = 'finished'
            ORDER BY date_utc DESC
            LIMIT 10
            """,
            (season, team_id, team_id),
        )

        recent_codes_desc: List[str] = []  # [가장 최신, ..., 예전]
        for m in match_rows or []:
            home_id = m.get("home_id")
            away_id = m.get("away_id")
            home_ft = m.get("home_ft")
            away_ft = m.get("away_ft")

            if home_ft is None or away_ft is None:
                continue

            try:
                h = int(home_ft)
                a = int(away_ft)
            except Exception:
                continue

            if team_id == home_id:
                code = "W" if h > a else ("D" if h == a else "L")
            elif team_id == away_id:
                code = "W" if a > h else ("D" if a == h else "L")
            else:
                continue

            recent_codes_desc.append(code)

        # 왼쪽이 예전, 오른쪽이 최신이 되도록 뒤집어서 내려준다.
        header["recent_form"] = list(reversed(recent_codes_desc))
    except Exception as e:
        print(f"[teamdetail.header_block] recent_form (matches) query failed: {e}")

    return header
