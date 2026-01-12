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

    ✅ 목표:
    - /api/fixtures 의 key들과 최대한 호환되게 확장
    - timeline_block 등에서 쓰는 elapsed 키를 제공
    - 기존 앱 호환을 위해 기존 키(kickoff_utc, minute, home.score 등)도 유지
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
            m.status_long,
            m.league_round,
            m.venue_name,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.home_ht,
            m.away_ht,
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

    elapsed = row.get("elapsed")
    kickoff = row.get("date_utc")

    home_ft = row.get("home_ft")
    away_ft = row.get("away_ft")
    home_ht = row.get("home_ht")
    away_ht = row.get("away_ht")

    return {
        "fixture_id": row["fixture_id"],
        "league_id": row["league_id"],
        "season": row["season"],

        # ✅ fixtures 호환 키
        "date_utc": kickoff,
        "elapsed": elapsed,
        "status_long": row.get("status_long"),
        "league_round": row.get("league_round"),
        "venue_name": row.get("venue_name"),

        # ✅ 기존 앱 호환 키(유지)
        "kickoff_utc": kickoff,
        "status_group": row.get("status_group"),
        "status": row.get("status"),
        "minute": elapsed,

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

            # ✅ fixtures 호환
            "ft": home_ft,
            "ht": home_ht,

            # ✅ 기존 앱 호환(유지)
            "score": home_ft,
            "red_cards": row["home_red_cards"],
        },
        "away": {
            "id": row["away_id"],
            "name": row["away_name"],
            "short_name": row["away_name"],
            "logo": row["away_logo"],

            # ✅ fixtures 호환
            "ft": away_ft,
            "ht": away_ht,

            # ✅ 기존 앱 호환(유지)
            "score": away_ft,
            "red_cards": row["away_red_cards"],
        },

        "filters": {
            "last_n": "Last 10",
            "comp": "All",
        },
    }
