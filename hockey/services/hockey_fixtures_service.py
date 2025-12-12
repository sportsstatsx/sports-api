# hockey/services/hockey_fixtures_service.py
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
    where_clauses: List[str] = ["(g.game_date::timestamptz BETWEEN %s AND %s)"]

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

            l.id AS league_id2,
            l.name AS league_name,
            l.logo AS league_logo,
            l.country AS league_country,

            th.id AS home_id,
            th.name AS home_name,
            th.logo AS home_logo,

            ta.id AS away_id,
            ta.name AS away_name,
            ta.logo AS away_logo,

            CASE
                WHEN g.raw_json IS NULL THEN NULL
                ELSE NULLIF((g.raw_json::jsonb -> 'scores' ->> 'home'), '')::int
            END AS home_score,

            CASE
                WHEN g.raw_json IS NULL THEN NULL
                ELSE NULLIF((g.raw_json::jsonb -> 'scores' ->> 'away'), '')::int
            END AS away_score

        FROM hockey_games g
        JOIN hockey_teams th ON th.id = g.home_team_id
        JOIN hockey_teams ta ON ta.id = g.away_team_id
        JOIN hockey_leagues l ON l.id = g.league_id
        WHERE {where_sql}
        ORDER BY g.game_date ASC
    """

    rows = hockey_fetch_all(sql, tuple(params))

    fixtures: List[Dict[str, Any]] = []
    for r in rows:
        fixtures.append(
            {
                "game_id": r["game_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": r["date_utc"],
                "status": r["status"],
                "status_long": r["status_long"],
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
