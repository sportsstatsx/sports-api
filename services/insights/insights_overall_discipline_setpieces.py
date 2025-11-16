# services/insights/insights_overall_discipline_setpieces.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all, fetch_one
from .utils import fmt_avg, pct_int


def enrich_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
) -> None:
    """
    Discipline & Set Pieces 섹션.

    로컬 SQLite InsightsOverallDao 와 동일한 개념으로,
    실제 이벤트가 존재하는 경기들만 모아서

      - corners_per_match
      - yellow_per_match
      - red_per_match
      - opp_red_* (상대 레드 이후 영향)
      - own_red_* (자팀 레드 이후 영향)

    을 per match 기준으로 계산한다.

    ✅ 중요:
      - 분모는 "실제 샘플이 있는 경기수" 를 사용한다.
        (API fixtures.played.total 은 사용하지 않음)
      - 홈/원정도 각각 실제 출전 경기수로 나눈다.
    """
    if season_int is None:
        return

    # ─────────────────────────────────────
    # 1) 코너 / 옐로 / 레드 카드 수집
    # ─────────────────────────────────────
    disc_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'corner%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS corners,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'yellow%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS yellows,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'red%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS reds
        FROM matches m
        LEFT JOIN match_team_stats mts
          ON mts.fixture_id = m.fixture_id
         AND mts.team_id   = %s
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        GROUP BY m.fixture_id, m.home_id, m.away_id
        """,
        (team_id, league_id, season_int, team_id, team_id),
    )

    if not disc_rows:
        return

    # 경기 수 (실제 stats 샘플이 있는 경기 기준)
    tot_matches = 0
    home_matches = 0
    away_matches = 0

    # 총합 (T/H/A)
    sum_corners_t = sum_corners_h = sum_corners_a = 0
    sum_yellows_t = sum_yellows_h = sum_yellows_a = 0
    sum_reds_t = sum_reds_h = sum_reds_a = 0

    for dr in disc_rows:
        home_id = dr["home_id"]
        away_id = dr["away_id"]
        is_home = home_id == team_id
        is_away = away_id == team_id
        if not (is_home or is_away):
            # 방어코드: 이 팀 한정으로 쿼리했기 때문에 원래는 안 들어와야 함
            continue

        corners = dr["corners"] or 0
        yellows = dr["yellows"] or 0
        reds = dr["reds"] or 0

        # 전체 경기 카운트
        tot_matches += 1
        sum_corners_t += corners
        sum_yellows_t += yellows
        sum_reds_t += reds

        # 홈/원정 분리
        if is_home:
            home_matches += 1
            sum_corners_h += corners
            sum_yellows_h += yellows
            sum_reds_h += reds
        else:
            away_matches += 1
            sum_corners_a += corners
            sum_yellows_a += yellows
            sum_reds_a += reds

    # ✅ 로컬 DB와 동일하게 "실제 샘플이 있는 경기 수"를 분모로 사용
    eff_tot = tot_matches or 0
    eff_home = home_matches or 0
    eff_away = away_matches or 0

    def avg_for(v_t: int, v_h: int, v_a: int, d_t: int, d_h: int, d_a: int):
        return (
            fmt_avg(v_t, d_t) if d_t > 0 else 0.0,
            fmt_avg(v_h, d_h) if d_h > 0 else 0.0,
            fmt_avg(v_a, d_a) if d_a > 0 else 0.0,
        )

    # 코너, 옐로, 레드 평균 산출
    c_tot, c_h, c_a = avg_for(
        sum_corners_t,
        sum_corners_h,
        sum_corners_a,
        eff_tot,
        eff_home,
        eff_away,
    )
    y_tot, y_h, y_a = avg_for(
        sum_yellows_t,
        sum_yellows_h,
        sum_yellows_a,
        eff_tot,
        eff_home,
        eff_away,
    )
    r_tot, r_h, r_a = avg_for(
        sum_reds_t,
        sum_reds_h,
        sum_reds_a,
        eff_tot,
        eff_home,
        eff_away,
    )

    # ─────────────────────────────────────
    # 2) 레드 카드 이후 영향 (Opp / Own)
    #    → 로컬 SQLite CTE 구조 그대로 포팅
    # ─────────────────────────────────────
    red_row = fetch_one(
        """
        WITH labeled AS (
          SELECT
            m.fixture_id,
            CASE WHEN %s = m.home_id THEN 'H' ELSE 'A' END AS venue
          FROM matches m
          WHERE m.league_id = %s
            AND m.season    = %s
            AND (%s = m.home_id OR %s = m.away_id)
            AND (
                  lower(m.status_group) IN ('finished','ft','fulltime')
               OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
            )
        ),
        goal_events AS (
          SELECT
            e.fixture_id,
            e.minute,
            CASE WHEN e.team_id = %s THEN 1 ELSE 0 END AS is_for
          FROM match_events e
          JOIN labeled l ON l.fixture_id = e.fixture_id
          WHERE lower(e.type) = 'goal'
            AND e.minute IS NOT NULL
        ),
        opp_red AS (
          SELECT
            l.fixture_id,
            l.venue,
            MIN(e.minute) AS red_minute
          FROM match_events e
          JOIN labeled l ON l.fixture_id = e.fixture_id
          WHERE lower(e.type) IN ('card','red card')
            AND (lower(e.detail) LIKE '%%red%%' OR lower(e.type)='red card')
            AND e.team_id <> %s
            AND e.minute IS NOT NULL
          GROUP BY l.fixture_id, l.venue
        ),
        goals_after_opp_red AS (
          SELECT
            orr.fixture_id,
            orr.venue,
            SUM(CASE WHEN ge.is_for=1 AND ge.minute > orr.red_minute THEN 1 ELSE 0 END) AS gf_after_red
          FROM opp_red orr
          LEFT JOIN goal_events ge ON ge.fixture_id = orr.fixture_id
          GROUP BY orr.fixture_id, orr.venue
        ),
        opp_red_summary AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.gf_after_red,0) >= 1 THEN 1 ELSE 0 END) AS scored_after,
            AVG(COALESCE(gar.gf_after_red,0)::numeric) AS avg_after
          FROM opp_red orr
          LEFT JOIN goals_after_opp_red gar ON gar.fixture_id = orr.fixture_id
        ),
        opp_red_home AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.gf_after_red,0) >= 1 THEN 1 ELSE 0 END) AS scored_after,
            AVG(COALESCE(gar.gf_after_red,0)::numeric) AS avg_after
          FROM opp_red orr
          LEFT JOIN goals_after_opp_red gar ON gar.fixture_id = orr.fixture_id
          WHERE orr.venue = 'H'
        ),
        opp_red_away AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.gf_after_red,0) >= 1 THEN 1 ELSE 0 END) AS scored_after,
            AVG(COALESCE(gar.gf_after_red,0)::numeric) AS avg_after
          FROM opp_red orr
          LEFT JOIN goals_after_opp_red gar ON gar.fixture_id = orr.fixture_id
          WHERE orr.venue = 'A'
        ),
        own_red AS (
          SELECT
            l.fixture_id,
            l.venue,
            MIN(e.minute) AS red_minute
          FROM match_events e
          JOIN labeled l ON l.fixture_id = e.fixture_id
          WHERE lower(e.type) IN ('card','red card')
            AND (lower(e.detail) LIKE '%%red%%' OR lower(e.type)='red card')
            AND e.team_id = %s
            AND e.minute IS NOT NULL
          GROUP BY l.fixture_id, l.venue
        ),
        goals_after_own_red AS (
          SELECT
            orr.fixture_id,
            orr.venue,
            SUM(CASE WHEN ge.is_for=0 AND ge.minute > orr.red_minute THEN 1 ELSE 0 END) AS ga_after_red
          FROM own_red orr
          LEFT JOIN goal_events ge ON ge.fixture_id = orr.fixture_id
          GROUP BY orr.fixture_id, orr.venue
        ),
        own_red_summary AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.ga_after_red,0) >= 1 THEN 1 ELSE 0 END) AS conceded_after,
            AVG(COALESCE(gar.ga_after_red,0)::numeric) AS avg_after
          FROM own_red orr
          LEFT JOIN goals_after_own_red gar ON gar.fixture_id = orr.fixture_id
        ),
        own_red_home AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.ga_after_red,0) >= 1 THEN 1 ELSE 0 END) AS conceded_after,
            AVG(COALESCE(gar.ga_after_red,0)::numeric) AS avg_after
          FROM own_red orr
          LEFT JOIN goals_after_own_red gar ON gar.fixture_id = orr.fixture_id
          WHERE orr.venue = 'H'
        ),
        own_red_away AS (
          SELECT
            COUNT(*) AS red_matches,
            SUM(CASE WHEN COALESCE(gar.ga_after_red,0) >= 1 THEN 1 ELSE 0 END) AS conceded_after,
            AVG(COALESCE(gar.ga_after_red,0)::numeric) AS avg_after
          FROM own_red orr
          LEFT JOIN goals_after_own_red gar ON gar.fixture_id = orr.fixture_id
          WHERE orr.venue = 'A'
        )
        SELECT
          COALESCE((SELECT red_matches    FROM opp_red_summary), 0)  AS opp_red_matches_total,
          COALESCE((SELECT red_matches    FROM opp_red_home),    0)  AS opp_red_matches_home,
          COALESCE((SELECT red_matches    FROM opp_red_away),    0)  AS opp_red_matches_away,
          COALESCE((SELECT scored_after   FROM opp_red_summary), 0)  AS opp_scored_after_total,
          COALESCE((SELECT scored_after   FROM opp_red_home),    0)  AS opp_scored_after_home,
          COALESCE((SELECT scored_after   FROM opp_red_away),    0)  AS opp_scored_after_away,
          COALESCE((SELECT avg_after      FROM opp_red_summary), 0)  AS opp_avg_after_total,
          COALESCE((SELECT avg_after      FROM opp_red_home),    0)  AS opp_avg_after_home,
          COALESCE((SELECT avg_after      FROM opp_red_away),    0)  AS opp_avg_after_away,

          COALESCE((SELECT red_matches    FROM own_red_summary), 0)  AS own_red_matches_total,
          COALESCE((SELECT red_matches    FROM own_red_home),    0)  AS own_red_matches_home,
          COALESCE((SELECT red_matches    FROM own_red_away),    0)  AS own_red_matches_away,
          COALESCE((SELECT conceded_after FROM own_red_summary), 0)  AS own_conceded_after_total,
          COALESCE((SELECT conceded_after FROM own_red_home),    0)  AS own_conceded_after_home,
          COALESCE((SELECT conceded_after FROM own_red_away),    0)  AS own_conceded_after_away,
          COALESCE((SELECT avg_after      FROM own_red_summary), 0)  AS own_avg_after_total,
          COALESCE((SELECT avg_after      FROM own_red_home),    0)  AS own_avg_after_home,
          COALESCE((SELECT avg_after      FROM own_red_away),    0)  AS own_avg_after_away;
        """,
        (
            team_id,         # CASE WHEN %s = m.home_id
            league_id,       # m.league_id = %s
            season_int,      # m.season = %s
            team_id,         # (%s = m.home_id ...
            team_id,         # OR %s = m.away_id)
            team_id,         # goal_events: e.team_id = %s
            team_id,         # opp_red: e.team_id <> %s
            team_id,         # own_red: e.team_id = %s
        ),
    )

    if red_row:
        opp_red_tot = red_row["opp_red_matches_total"] or 0
        opp_red_home = red_row["opp_red_matches_home"] or 0
        opp_red_away = red_row["opp_red_matches_away"] or 0

        opp_scored_tot = red_row["opp_scored_after_total"] or 0
        opp_scored_home = red_row["opp_scored_after_home"] or 0
        opp_scored_away = red_row["opp_scored_after_away"] or 0

        opp_avg_tot = float(red_row["opp_avg_after_total"] or 0.0)
        opp_avg_home = float(red_row["opp_avg_after_home"] or 0.0)
        opp_avg_away = float(red_row["opp_avg_after_away"] or 0.0)

        own_red_tot = red_row["own_red_matches_total"] or 0
        own_red_home = red_row["own_red_matches_home"] or 0
        own_red_away = red_row["own_red_matches_away"] or 0

        own_conc_tot = red_row["own_conceded_after_total"] or 0
        own_conc_home = red_row["own_conceded_after_home"] or 0
        own_conc_away = red_row["own_conceded_after_away"] or 0

        own_avg_tot = float(red_row["own_avg_after_total"] or 0.0)
        own_avg_home = float(red_row["own_avg_after_home"] or 0.0)
        own_avg_away = float(red_row["own_avg_after_away"] or 0.0)
    else:
        # 샘플이 전혀 없는 경우
        opp_red_tot = opp_red_home = opp_red_away = 0
        opp_scored_tot = opp_scored_home = opp_scored_away = 0
        opp_avg_tot = opp_avg_home = opp_avg_away = 0.0

        own_red_tot = own_red_home = own_red_away = 0
        own_conc_tot = own_conc_home = own_conc_away = 0
        own_avg_tot = own_avg_home = own_avg_away = 0.0

    # 퍼센트는 로컬 DAO 와 동일하게 정수 % 로 계산
    opp_pct_tot = pct_int(opp_scored_tot, opp_red_tot) if opp_red_tot > 0 else 0
    opp_pct_home = pct_int(opp_scored_home, opp_red_home) if opp_red_home > 0 else 0
    opp_pct_away = pct_int(opp_scored_away, opp_red_away) if opp_red_away > 0 else 0

    own_pct_tot = pct_int(own_conc_tot, own_red_tot) if own_red_tot > 0 else 0
    own_pct_home = pct_int(own_conc_home, own_red_home) if own_red_home > 0 else 0
    own_pct_away = pct_int(own_conc_away, own_red_away) if own_red_away > 0 else 0

    # 평균 골은 소수 둘째 자리까지
    opp_avg_tot = round(opp_avg_tot, 2) if opp_avg_tot > 0 else 0.0
    opp_avg_home = round(opp_avg_home, 2) if opp_avg_home > 0 else 0.0
    opp_avg_away = round(opp_avg_away, 2) if opp_avg_away > 0 else 0.0

    own_avg_tot = round(own_avg_tot, 2) if own_avg_tot > 0 else 0.0
    own_avg_home = round(own_avg_home, 2) if own_avg_home > 0 else 0.0
    own_avg_away = round(own_avg_away, 2) if own_avg_away > 0 else 0.0

    # ─────────────────────────────────────
    # 3) insights_overall JSON 에 기록
    # ─────────────────────────────────────
    insights["corners_per_match"] = {
        "total": c_tot,
        "home": c_h,
        "away": c_a,
    }
    insights["yellow_per_match"] = {
        "total": y_tot,
        "home": y_h,
        "away": y_a,
    }
    insights["red_per_match"] = {
        "total": r_tot,
        "home": r_h,
        "away": r_a,
    }

    # 상대 레드 카드 이후
    insights["opp_red_sample"] = int(opp_red_tot)
    insights["opp_red_scored_pct"] = {
        "total": int(opp_pct_tot),
        "home": int(opp_pct_home),
        "away": int(opp_pct_away),
    }
    insights["opp_red_goals_after_avg"] = {
        "total": float(opp_avg_tot),
        "home": float(opp_avg_home),
        "away": float(opp_avg_away),
    }

    # 자팀 레드 카드 이후
    insights["own_red_sample"] = int(own_red_tot)
    insights["own_red_conceded_pct"] = {
        "total": int(own_pct_tot),
        "home": int(own_pct_home),
        "away": int(own_pct_away),
    }
    insights["own_red_goals_after_avg"] = {
        "total": float(own_avg_tot),
        "home": float(own_avg_home),
        "away": float(own_avg_away),
    }
