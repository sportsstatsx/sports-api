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

            -- ✅ (추가) status 전체 (halftime 플래그 포함)
            (g.raw_json::jsonb -> 'status') AS status_obj,

            -- ✅ (추가) scores 전체 (linescore로 완료 쿼터 수 계산)
            (g.raw_json::jsonb -> 'scores') AS scores_obj,


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

        status_obj = r.get("status_obj") or {}
        scores_obj = r.get("scores_obj") or {}

        # ✅ halftime 플래그 (너가 확인한 그대로 status_obj에 있음)
        halftime = False
        try:
            halftime = bool(status_obj.get("halftime") is True)
        except Exception:
            halftime = False

        # ✅ linescore로 완료된 쿼터 수 계산 ("" 제외)
        def _count_filled_linescore(team_scores: dict) -> int:
            ls = (team_scores or {}).get("linescore") or []
            if not isinstance(ls, list):
                return 0
            return sum(1 for x in ls if str(x).strip() != "")

        home_ls = _count_filled_linescore((scores_obj or {}).get("home") or {})
        away_ls = _count_filled_linescore((scores_obj or {}).get("visitors") or {})
        completed_q = max(home_ls, away_ls)  # 0~4

        # ✅ clock 없으면 브레이크/쿼터전환/하프타임으로 간주 (API-Sports 특성)
        clock_missing = (clock_text is None) or (clock_text.strip() == "")

        timer_text = None

        if status_short == 2:  # In Play
            if clock_missing:
                # ✅ 16333 케이스: halftime=true + clock=null + completed_q=2
                # => "2Q End Break"
                if halftime and completed_q == 2:
                    timer_text = "2Q End Break"
                # ✅ 쿼터 전환 브레이크: completed_q가 1~3이면 "nQ End Break"
                elif completed_q in (1, 2, 3):
                    timer_text = f"{completed_q}Q End Break"
                else:
                    timer_text = None
            else:
                # ✅ clock이 있으면: linescore 기준 보정 (+1 금지)
                # API-Sports NBA는 현재 쿼터도 linescore에 값이 들어오는 케이스가 있음
                # → completed_q 그대로 현재 쿼터로 사용
                current_q = min(4, max(1, completed_q))
                timer_text = f"Q{current_q} {clock_text}"

        fixtures.append(
            {
                "game_id": r["game_id"],
                "league": r.get("league"),
                "season": r.get("season"),
                "stage": r.get("stage"),

                "date_utc": dt_iso,

                "status": status_short,
                "status_long": status_long,

                # clock 원문은 유지
                "clock": clock_text,

                # ✅ 핵심: 앱이 읽는 timeText/timer에 브레이크 문자열 생성
                "timer": timer_text,

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
