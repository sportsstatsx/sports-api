# matchdetail/header_block.py

from typing import Any, Dict, Optional
from db import fetch_one

# ✅ 매치카드와 동일 기준
_RED_DETAIL_SQL = "('Red Card','Second Yellow card','Second Yellow Card')"


def build_header_block(
    fixture_id: int,
    league_id: int,
    season: int,
) -> Optional[Dict[str, Any]]:
    """
    matches + teams + leagues + match_fixtures_raw(+ optional match_live_state)
    기준으로 match detail header 생성.

    목표:
    - /api/fixtures 와 최대한 동일한 기준
    - ScoreBlock 이 매치카드와 같은 상태/점수/카드 데이터를 사용할 수 있게 맞춤
    """

    # ✅ optional table 존재 확인 (fixtures API 와 동일 컨셉)
    mls_ok = False
    try:
        chk = fetch_one("SELECT to_regclass('public.match_live_state') AS t", ())
        mls_ok = bool(chk and chk.get("t"))
    except Exception:
        mls_ok = False

    if mls_ok:
        home_red_sql = f"""
            COALESCE(
                mls.home_red,
                (
                    SELECT COUNT(*)
                    FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.home_id
                      AND e.type = 'Card'
                      AND e.detail IN {_RED_DETAIL_SQL}
                )
            ) AS home_red_cards
        """
        away_red_sql = f"""
            COALESCE(
                mls.away_red,
                (
                    SELECT COUNT(*)
                    FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.team_id = m.away_id
                      AND e.type = 'Card'
                      AND e.detail IN {_RED_DETAIL_SQL}
                )
            ) AS away_red_cards
        """
        mls_join = "LEFT JOIN match_live_state mls ON mls.fixture_id = m.fixture_id"
    else:
        home_red_sql = f"""
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.home_id
                  AND e.type = 'Card'
                  AND e.detail IN {_RED_DETAIL_SQL}
            ) AS home_red_cards
        """
        away_red_sql = f"""
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id = m.away_id
                  AND e.type = 'Card'
                  AND e.detail IN {_RED_DETAIL_SQL}
            ) AS away_red_cards
        """
        mls_join = ""

    row = fetch_one(
        f"""
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

            th.name AS home_name,
            ta.name AS away_name,
            th.logo AS home_logo,
            ta.logo AS away_logo,

            l.name AS league_name,
            l.logo AS league_logo,
            l.country AS league_country,
            c.flag AS league_country_flag,

            (rf.data_json::jsonb->'score'->'extratime'->>'home') AS home_et,
            (rf.data_json::jsonb->'score'->'extratime'->>'away') AS away_et,
            (rf.data_json::jsonb->'score'->'penalty'->>'home') AS home_pen,
            (rf.data_json::jsonb->'score'->'penalty'->>'away') AS away_pen,

            {home_red_sql},
            {away_red_sql}

        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        JOIN leagues l ON l.id = m.league_id
        LEFT JOIN countries c
          ON LOWER(TRIM(c.name)) = LOWER(TRIM(l.country))
        LEFT JOIN match_fixtures_raw rf
          ON rf.fixture_id = m.fixture_id
        {mls_join}
        WHERE m.fixture_id = %s
          AND m.league_id = %s
          AND m.season = %s
        LIMIT 1
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

    home_et = int(row["home_et"]) if row.get("home_et") not in (None, "") else None
    away_et = int(row["away_et"]) if row.get("away_et") not in (None, "") else None
    home_pen = int(row["home_pen"]) if row.get("home_pen") not in (None, "") else None
    away_pen = int(row["away_pen"]) if row.get("away_pen") not in (None, "") else None

    return {
        "fixture_id": row["fixture_id"],
        "league_id": row["league_id"],
        "season": row["season"],

        # ✅ fixtures 호환 키
        "date_utc": kickoff,
        "elapsed": elapsed,
        "status_group": row.get("status_group"),
        "status": row.get("status"),
        "status_long": row.get("status_long"),
        "league_round": row.get("league_round"),
        "venue_name": row.get("venue_name"),

        "league_name": row.get("league_name"),
        "league_logo": row.get("league_logo"),
        "league_country": row.get("league_country"),
        "league_country_flag": row.get("league_country_flag"),

        # ✅ 기존 앱 호환 키 유지
        "kickoff_utc": kickoff,
        "minute": elapsed,

        "league": {
            "name": row.get("league_name"),
            "logo": row.get("league_logo"),
            "country": row.get("league_country"),
            "country_flag": row.get("league_country_flag"),
        },

        "home": {
            "id": row["home_id"],
            "name": row["home_name"],
            "short_name": row["home_name"],
            "logo": row["home_logo"],

            "ft": home_ft,
            "ht": home_ht,
            "et": home_et,
            "pen": home_pen,

            # 기존 앱 호환
            "score": home_ft,
            "red_cards": row["home_red_cards"],
        },

        "away": {
            "id": row["away_id"],
            "name": row["away_name"],
            "short_name": row["away_name"],
            "logo": row["away_logo"],

            "ft": away_ft,
            "ht": away_ht,
            "et": away_et,
            "pen": away_pen,

            # 기존 앱 호환
            "score": away_ft,
            "red_cards": row["away_red_cards"],
        },

        "filters": {
            "last_n": "Last 10",
            "comp": "All",
        },
    }
        },
    }
