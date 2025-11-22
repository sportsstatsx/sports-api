from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all
from .utils import fmt_avg


def _pct_int(total: int, hit: int) -> int:
    """
    분모 total, 히트 hit  →  정수 퍼센트 (0~100)
    total <= 0 이면 0으로.
    """
    if total <= 0:
        return 0
    return round(hit * 100.0 / total)


def enrich_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
    last_n: Optional[int] = None,  # 🔹 추가: Last N 필터 (없으면 시즌 전체)
) -> None:
    """
    Discipline & Set Pieces 섹션.

    - 코너 / 옐로 / 레드카드 평균 (경기당)
    - 상대 레드카드 이후 우리가 득점한 비율 / 평균 득점
    - 우리 레드카드 이후 우리가 실점한 비율 / 평균 실점

    ✅ 시즌 값(season_int)이 None이면 아무것도 하지 않고 리턴.
    """
    if season_int is None:
        return

        # Competition 필터 + Last N 에서 사용할 league_id 집합 결정
    league_ids_for_query: List[Any]
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if filters and isinstance(filters, Dict):
        target_ids = filters.get("target_league_ids_last_n")

    if last_n and last_n > 0 and isinstance(target_ids, list):
        league_ids_for_query = []
        for v in target_ids:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                continue
        # 혹시라도 잘못된 값만 들어오면 베이스 리그 한 개로 폴백
        if not league_ids_for_query:
            league_ids_for_query = [league_id]
    else:
        # 시즌 전체(Last N 없음) 이거나 필터 정보가 없으면 기존처럼 베이스 리그만 사용
        league_ids_for_query = [league_id]


    # ─────────────────────────────────────────
    # 1) 코너 / 옐로 / 레드 합계 및 경기 수
    #    - last_n 이 있으면 "최근 N경기"만 사용
    # ─────────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
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
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        GROUP BY m.fixture_id, m.home_id, m.away_id, m.date_utc
        ORDER BY m.date_utc DESC
    """

    params = [team_id, *league_ids_for_query, season_int, team_id, team_id]

    # 🔹 last_n > 0 이면 시즌 내에서 최근 N경기만 사용
    if last_n is not None and last_n > 0:
        base_sql += "\n        LIMIT %s"
        params.append(last_n)

    disc_rows = fetch_all(base_sql, tuple(params))

    if not disc_rows:
        # 이 팀/시즌에 해당하는 경기 자체가 없으면 아무 것도 기록하지 않음
        return


    # 경기 수 및 합계 (T/H/A)
    tot_matches = 0
    home_matches = 0
    away_matches = 0

    sum_corners_t = sum_corners_h = sum_corners_a = 0
    sum_yellows_t = sum_yellows_h = sum_yellows_a = 0
    sum_reds_t = sum_reds_h = sum_reds_a = 0

    # fixture → venue('H' / 'A') 매핑.
    # Opp Red / Own Red 계산 때도 같이 사용.
    fixture_venue: Dict[int, str] = {}

    for dr in disc_rows:
        fid = dr["fixture_id"]
        home_id = dr["home_id"]
        away_id = dr["away_id"]

        is_home = (home_id == team_id)
        is_away = (away_id == team_id)
        if not (is_home or is_away):
            # 이 팀이 아닌 경기면 방어적으로 스킵
            continue

        venue = "H" if is_home else "A"
        fixture_venue[fid] = venue

        corners = dr["corners"] or 0
        yellows = dr["yellows"] or 0
        reds = dr["reds"] or 0

        # 전체 경기
        tot_matches += 1
        sum_corners_t += corners
        sum_yellows_t += yellows
        sum_reds_t += reds

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

    # 분모(실제 샘플이 있는 경기 수)
    eff_tot = tot_matches or 0
    eff_home = home_matches or 0
    eff_away = away_matches or 0

    def avg_for(v_t: int, v_h: int, v_a: int, d_t: int, d_h: int, d_a: int):
        return (
            fmt_avg(v_t, d_t) if d_t > 0 else 0.0,
            fmt_avg(v_h, d_h) if d_h > 0 else 0.0,
            fmt_avg(v_a, d_a) if d_a > 0 else 0.0,
        )

    # 코너, 옐로, 레드 평균
    c_tot, c_h, c_a = avg_for(
        sum_corners_t, sum_corners_h, sum_corners_a, eff_tot, eff_home, eff_away
    )
    y_tot, y_h, y_a = avg_for(
        sum_yellows_t, sum_yellows_h, sum_yellows_a, eff_tot, eff_home, eff_away
    )
    r_tot, r_h, r_a = avg_for(
        sum_reds_t, sum_reds_h, sum_reds_a, eff_tot, eff_home, eff_away
    )

    # JSON 기록
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

    # ─────────────────────────────────────────
    # 2) Opp Red / Own Red 계산
    #    (레드카드 이후 득점/실점 비율 + 평균 골)
    # ─────────────────────────────────────────

    # 카드 이벤트 (레드 카드만 필터)
    placeholders_ev = ",".join(["%s"] * len(league_ids_for_query))

    card_rows = fetch_all(
        f"""
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            m.home_id,
            m.away_id
        FROM match_events e
        JOIN matches m ON m.fixture_id = e.fixture_id
        WHERE m.league_id IN ({placeholders_ev})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
          AND lower(e.type) IN ('card','red card')
          AND (
                lower(e.detail) LIKE '%%red%%'
             OR lower(e.type) = 'red card'
          )
          AND e.minute IS NOT NULL
        """,
        (*league_ids_for_query, season_int, team_id, team_id),
    )


    # 골 이벤트
    goal_rows = fetch_all(
        f"""
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id
        FROM match_events e
        JOIN matches m ON m.fixture_id = e.fixture_id
        WHERE m.league_id IN ({placeholders_ev})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
          AND lower(e.type) = 'goal'
          AND e.minute IS NOT NULL
        """,
        (*league_ids_for_query, season_int, team_id, team_id),
    )


    # fixture 별 첫 레드카드 시각 (상대 / 자팀)
    opp_red_min: Dict[int, int] = {}
    own_red_min: Dict[int, int] = {}

    for row in card_rows:
        fid = row["fixture_id"]
        minute = row["minute"]
        card_team_id = row["team_id"]

        # 이 경기에서 우리 팀이 실제로 뛴 경우만 (안전 방어)
        if fid not in fixture_venue:
            continue

        if card_team_id == team_id:
            # Own red
            prev = own_red_min.get(fid)
            if prev is None or minute < prev:
                own_red_min[fid] = minute
        else:
            # Opp red
            prev = opp_red_min.get(fid)
            if prev is None or minute < prev:
                opp_red_min[fid] = minute

    # 골 이후 플래그 + 골 개수 집계
    opp_scored_after: Dict[int, bool] = {}
    own_conceded_after: Dict[int, bool] = {}

    opp_goals_after_t = opp_goals_after_h = opp_goals_after_a = 0
    own_goals_after_t = own_goals_after_h = own_goals_after_a = 0

    for row in goal_rows:
        fid = row["fixture_id"]
        minute = row["minute"]
        scorer_id = row["team_id"]

        # 이 경기에서 우리 팀이 실제로 뛴 경우만
        if fid not in fixture_venue:
            continue

        venue = fixture_venue[fid]

        # 상대 레드 이후 우리가 득점?
        if fid in opp_red_min and minute > opp_red_min[fid] and scorer_id == team_id:
            opp_scored_after[fid] = True
            opp_goals_after_t += 1
            if venue == "H":
                opp_goals_after_h += 1
            else:
                opp_goals_after_a += 1

        # 우리 레드 이후 우리가 실점?
        if fid in own_red_min and minute > own_red_min[fid] and scorer_id != team_id:
            own_conceded_after[fid] = True
            own_goals_after_t += 1
            if venue == "H":
                own_goals_after_h += 1
            else:
                own_goals_after_a += 1

    # 샘플 수 및 히트 수 (T/H/A) 집계
    opp_sample_t = opp_sample_h = opp_sample_a = 0
    opp_scored_t = opp_scored_h = opp_scored_a = 0

    for fid, minute in opp_red_min.items():
        venue = fixture_venue.get(fid)
        if venue is None:
            continue

        opp_sample_t += 1
        if venue == "H":
            opp_sample_h += 1
        else:
            opp_sample_a += 1

        if opp_scored_after.get(fid):
            opp_scored_t += 1
            if venue == "H":
                opp_scored_h += 1
            else:
                opp_scored_a += 1

    own_sample_t = own_sample_h = own_sample_a = 0
    own_conceded_t = own_conceded_h = own_conceded_a = 0

    for fid, minute in own_red_min.items():
        venue = fixture_venue.get(fid)
        if venue is None:
            continue

        own_sample_t += 1
        if venue == "H":
            own_sample_h += 1
        else:
            own_sample_a += 1

        if own_conceded_after.get(fid):
            own_conceded_t += 1
            if venue == "H":
                own_conceded_h += 1
            else:
                own_conceded_a += 1

    # 퍼센트 계산 (정수)
    opp_pct_total = _pct_int(opp_sample_t, opp_scored_t)
    opp_pct_home = _pct_int(opp_sample_h, opp_scored_h)
    opp_pct_away = _pct_int(opp_sample_a, opp_scored_a)

    own_pct_total = _pct_int(own_sample_t, own_conceded_t)
    own_pct_home = _pct_int(own_sample_h, own_conceded_h)
    own_pct_away = _pct_int(own_sample_a, own_conceded_a)

    # 골 개수 → 경기당 평균 골 (T/H/A)
    opp_gavg_total = fmt_avg(opp_goals_after_t, opp_sample_t)
    opp_gavg_home = fmt_avg(opp_goals_after_h, opp_sample_h)
    opp_gavg_away = fmt_avg(opp_goals_after_a, opp_sample_a)

    own_gavg_total = fmt_avg(own_goals_after_t, own_sample_t)
    own_gavg_home = fmt_avg(own_goals_after_h, own_sample_h)
    own_gavg_away = fmt_avg(own_goals_after_a, own_sample_a)

    # JSON 기록
    # (샘플은 전체 기준 하나, 퍼센트/평균은 T/H/A 3개)
    insights["opp_red_sample"] = opp_sample_t
    insights["opp_red_scored_pct"] = {
        "total": opp_pct_total,
        "home": opp_pct_home,
        "away": opp_pct_away,
    }
    insights["opp_red_goals_after_avg"] = {
        "total": opp_gavg_total,
        "home": opp_gavg_home,
        "away": opp_gavg_away,
    }

    insights["own_red_sample"] = own_sample_t
    insights["own_red_conceded_pct"] = {
        "total": own_pct_total,
        "home": own_pct_home,
        "away": own_pct_away,
    }
    insights["own_red_goals_after_avg"] = {
        "total": own_gavg_total,
        "home": own_gavg_home,
        "away": own_gavg_away,
    }
