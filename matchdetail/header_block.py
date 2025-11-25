# matchdetail/header_block.py

from typing import Any, Dict, Optional
from db import fetch_one


def build_header_block(
    fixture_id: int,
    league_id: int,
    season: int,
) -> Optional[Dict[str, Any]]:
    """
    matches 테이블 + teams + leagues 를 이용해서
    매치디테일 상단에 필요한 정보(header 블록)를 만든다.

    컬럼 구성은 main.py 의 /api/fixtures 쿼리와 최대한 맞춘다.
    """

    row = fetch_one(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.date_utc,
            m.status_group,
            m.status,
            m.elapsed,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            th.name  AS home_name,
            ta.name  AS away_name,
            th.logo  AS home_logo,
            ta.logo  AS away_logo,
            l.name   AS league_name,
            l.logo   AS league_logo,
            l.country AS league_country,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail = 'Red Card'
            ) AS home_red_cards,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail = 'Red Card'
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        WHERE m.fixture_id = %s
          AND m.league_id  = %s
          AND m.season     = %s
        """,
        (fixture_id, league_id, season),
    )

    if row is None:
        return None

    return {
        "fixture_id": row["fixture_id"],
        "league_id": row["league_id"],
        "season": row["season"],
        "kickoff_utc": row["date_utc"],
        "status_group": row["status_group"],
        "status": row["status"],
        "minute": row["elapsed"],

        "league": {
            "name": row.get("league_name"),
            "logo": row.get("league_logo"),
            "country": row.get("league_country"),
        },

        "home": {
            "id": row["home_id"],
            "name": row["home_name"],
            "short_name": row["home_name"],
            "logo": row["home_logo"],
            "score": row["home_ft"],
            "red_cards": row["home_red_cards"],
        },
        "away": {
            "id": row["away_id"],
            "name": row["away_name"],
            "short_name": row["away_name"],
            "logo": row["away_logo"],
            "score": row["away_ft"],
            "red_cards": row["away_red_cards"],
        },

        # 🔥 필터 블록 추가 (기존 로직 보존)
        "filters": {
            "last_n": "Last 10",   # 기본값
            "comp": "All",         # 기본값
        },
    }
