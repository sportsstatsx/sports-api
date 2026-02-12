from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from basketball.nba.nba_db import nba_fetch_all


def nba_get_fixtures_by_utc_range(
    utc_start: datetime,
    utc_end: datetime,
    leagues: List[str],
    league: Optional[str],
) -> List[Dict[str, Any]]:
    """
    utc_start ~ utc_end 범위의 NBA 경기 조회 (정식 매치리스트용)

    - nba_games + nba_teams 조인
    - league 정보는 nba_leagues(raw_json)에서 가능한 범위로 보강(없으면 fallback)
    - score/clock은 nba_games.raw_json에서 안전하게 추출
    - 하키 fixtures 응답 포맷과 최대한 유사하게 내려줌 (앱 변환 용이)
    """

    def _to_iso_z(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        try:
            return (
                dt.astimezone(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            return str(dt)

    params: List[Any] = [utc_start, utc_end]
    where_clauses: List[str] = ["(g.date_start_utc >= %s AND g.date_start_utc < %s)"]

    # league 필터 (nba_games.league = text)
    if leagues:
        placeholders = ", ".join(["%s"] * len(leagues))
        where_clauses.append(f"g.league IN ({placeholders})")
        params.extend(leagues)
    elif league:
        where_clauses.append("g.league = %s")
        params.append(league)

    where_sql = " AND ".join(where_clauses)

    # ⚠️ NBA 스키마 확정(네가 준 결과):
    # nba_games: id, league(text), season, stage(int), status_long(text), status_short(int),
    #          date_start_utc(timestamptz), home_team_id, visitor_team_id, arena_*, raw_json(jsonb)
    # nba_teams: id, name, nickname, code, city, logo, raw_json
    # nba_leagues: id(text), raw_json
    #
    # 점수/클락은 raw_json 구조가 케이스가 있을 수 있어 여러 경로를 COALESCE로 방어함.
    sql = f"""
        SELECT
            g.id AS game_id,
            g.league,
            g.season,
            g.stage,
            g.status_short,
            g.status_long,
            g.date_start_utc AS date_utc,

            g.arena_name,
            g.arena_city,
            g.arena_state,

            th.id AS home_id,
            th.name AS home_name,
            th.logo AS home_logo,
            th.code AS home_code,
            th.city AS home_city,
            th.nickname AS home_nickname,

            tv.id AS away_id,
            tv.name AS away_name,
            tv.logo AS away_logo,
            tv.code AS away_code,
            tv.city AS away_city,
            tv.nickname AS away_nickname,

            -- league meta (가능한 경우만)
            (l.raw_json::jsonb ->> 'name') AS league_name,
            (l.raw_json::jsonb ->> 'logo') AS league_logo,
            (l.raw_json::jsonb ->> 'country') AS league_country,

            -- clock (API-Sports basketball: status.clock)
            (g.raw_json::jsonb -> 'status' ->> 'clock') AS clock,

            -- scores: 여러 구조 방어
            COALESCE(
              CASE
                WHEN (g.raw_json::jsonb -> 'scores' -> 'home' ->> 'points') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'scores' -> 'home' ->> 'points')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'scores' -> 'home' ->> 'total') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'scores' -> 'home' ->> 'total')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'score'  -> 'home' ->> 'total') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'score'  -> 'home' ->> 'total')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'score'  ->> 'home') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'score'  ->> 'home')::int
              END
            ) AS home_score,

            COALESCE(
              CASE
                WHEN (g.raw_json::jsonb -> 'scores' -> 'visitors' ->> 'points') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'scores' -> 'visitors' ->> 'points')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'scores' -> 'visitors' ->> 'total') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'scores' -> 'visitors' ->> 'total')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'score'  -> 'away' ->> 'total') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'score'  -> 'away' ->> 'total')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'score'  ->> 'away') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'score'  ->> 'away')::int
              END,
              CASE
                WHEN (g.raw_json::jsonb -> 'score'  -> 'visitors' ->> 'total') ~ '^[0-9]+$'
                THEN (g.raw_json::jsonb -> 'score'  -> 'visitors' ->> 'total')::int
              END
            ) AS away_score


        FROM nba_games g
        JOIN nba_teams th ON th.id = g.home_team_id
        JOIN nba_teams tv ON tv.id = g.visitor_team_id
        LEFT JOIN nba_leagues l ON l.id = g.league
        WHERE {where_sql}
        ORDER BY g.date_start_utc ASC
    """

    rows = nba_fetch_all(sql, tuple(params))

    fixtures: List[Dict[str, Any]] = []
    for r in rows:
        dt_iso = _to_iso_z(r.get("date_utc"))

        # 하키와 키 맞추기: status는 하키는 문자열이었지만,
        # NBA는 status_short(int)가 확정이라 그대로 내림.
        status_short = r.get("status_short")
        status_long = (r.get("status_long") or "").strip()

        clock_text = (r.get("clock") or "").strip() or None

        fixtures.append(
            {
                "game_id": r["game_id"],
                "league": r.get("league"),
                "season": r.get("season"),
                "stage": r.get("stage"),

                "date_utc": dt_iso,

                "status": status_short,
                "status_long": status_long,
                "clock": clock_text,
                "timer": clock_text,  # 하키 형식 호환

                # 라운드/주차 개념은 NBA에서 week 컬럼이 없으므로 null
                "week": None,
                "round_raw": None,
                "round_chip": None,

                "arena": {
                    "name": r.get("arena_name"),
                    "city": r.get("arena_city"),
                    "state": r.get("arena_state"),
                },

                "league_info": {
                    "id": r.get("league"),
                    "name": r.get("league_name") or r.get("league") or "NBA",
                    "logo": r.get("league_logo"),
                    "country": r.get("league_country"),
                },

                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r.get("home_logo"),
                    "code": r.get("home_code"),
                    "city": r.get("home_city"),
                    "nickname": r.get("home_nickname"),
                    "score": r.get("home_score"),
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r.get("away_logo"),
                    "code": r.get("away_code"),
                    "city": r.get("away_city"),
                    "nickname": r.get("away_nickname"),
                    "score": r.get("away_score"),
                },
            }
        )

    return fixtures
