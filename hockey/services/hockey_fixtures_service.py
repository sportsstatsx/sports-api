from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all


def hockey_get_fixtures_by_utc_range(
    utc_start: datetime,
    utc_end: datetime,
    league_ids: List[int],
    league_id: Optional[int],
) -> List[Dict[str, Any]]:
    """
    utc_start ~ utc_end 범위의 하키 경기 조회 (정식 매치리스트용)

    - hockey_games + hockey_teams + hockey_leagues 조인
    - score는 raw_json(scores.home/away)에서 우선 추출 (없으면 null)
    """

    params: List[Any] = [utc_start, utc_end]
    where_clauses: List[str] = ["(g.game_date::timestamptz >= %s AND g.game_date::timestamptz < %s)"]

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clauses.append(f"g.league_id IN ({placeholders})")
        params.extend(league_ids)
    elif league_id is not None and league_id > 0:
        where_clauses.append("g.league_id = %s")
        params.append(league_id)

    where_sql = " AND ".join(where_clauses)

    # ✅ 핵심: teams/leagues 테이블이 있으니 정식 JOIN 구조로 간다.
    # 점수는 raw_json 기반으로 최대한 안전하게 추출 (raw_json이 text여도 ::jsonb 캐스팅)
    sql = f"""
        SELECT
            g.id AS game_id,
            g.league_id,
            g.season,
            g.game_date AS date_utc,
            g.status,
            g.status_long,
            g.live_timer,

            l.id AS league_id2,
            l.name AS league_name,
            l.logo AS league_logo,
            c.name AS league_country,

            th.id AS home_id,
            th.name AS home_name,
            th.logo AS home_logo,

            ta.id AS away_id,
            ta.name AS away_name,
            ta.logo AS away_logo,

            COALESCE(
                NULLIF((g.score_json ->> 'home'), '')::int,
                CASE
                    WHEN g.raw_json IS NULL THEN NULL
                    ELSE NULLIF((g.raw_json::jsonb -> 'scores' ->> 'home'), '')::int
                END
            ) AS home_score,

            COALESCE(
                NULLIF((g.score_json ->> 'away'), '')::int,
                CASE
                    WHEN g.raw_json IS NULL THEN NULL
                    ELSE NULLIF((g.raw_json::jsonb -> 'scores' ->> 'away'), '')::int
                END
            ) AS away_score


        FROM hockey_games g
        JOIN hockey_teams th ON th.id = g.home_team_id
        JOIN hockey_teams ta ON ta.id = g.away_team_id
        JOIN hockey_leagues l ON l.id = g.league_id
        LEFT JOIN hockey_countries c ON c.id = l.country_id
        WHERE {where_sql}
        ORDER BY g.game_date ASC
    """

    rows = hockey_fetch_all(sql, tuple(params))

    fixtures: List[Dict[str, Any]] = []
    for r in rows:
        # ✅ date_utc를 ISO8601(Z) 문자열로 고정 (matchdetail과 동일)
        dt = r.get("date_utc")
        if dt is not None:
            try:
                dt_iso = (
                    dt.astimezone(timezone.utc)
                      .replace(microsecond=0)
                      .isoformat()
                      .replace("+00:00", "Z")
                )
            except Exception:
                dt_iso = str(dt)
        else:
            dt_iso = None

                # ✅ 종료 정규화:
        # API-Sports가 "AOT(After Over Time)" / "AP(After Penalties)"로 멈춰있어도
        # 우리 앱 UX에서는 "종료"로 취급해야 함.
        raw_status = (r.get("status") or "").strip().upper()
        raw_status_long = (r.get("status_long") or "").strip()
        live_timer = (r.get("live_timer") or "").strip()

        norm_status = raw_status
        norm_status_long = raw_status_long

        if raw_status in ("AOT", "AP"):
            norm_status = "FT"
            # status_long은 굳이 바꿀 필요 없지만, 앱에서 "진행중"처럼 보이는 원인이면 Finished로 통일
            if not norm_status_long or norm_status_long in ("After Over Time", "After Penalties"):
                norm_status_long = "Finished"

        # ✅ LIVE(진행중)면 status_long에 timer 붙이기
        clock_text = ""
        if live_timer:
            if ":" in live_timer:
                clock_text = live_timer
            else:
                try:
                    clock_text = f"{int(live_timer):02d}:00"
                except Exception:
                    clock_text = live_timer

        status_long_out = norm_status_long
        if norm_status in ("P1", "P2", "P3", "OT", "SO") and clock_text:
            status_long_out = f"{norm_status_long} {clock_text}"

        fixtures.append(
            {
                "game_id": r["game_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": dt_iso,
                "status": norm_status,
                "status_long": status_long_out,
                "clock": clock_text or None,
                "timer": live_timer or None,
                "league": {
                    "id": r["league_id2"],
                    "name": r["league_name"],
                    "logo": r["league_logo"],
                    "country": r["league_country"],
                },
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "score": r["home_score"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "score": r["away_score"],
                },
            }
        )


    return fixtures
