# services/insights/insights_overall_outcome_totals.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_pct, fmt_avg


def enrich_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
    last_n: int = 0,
) -> None:
    """
    Insights Overall - Outcome & Totals / Goal Diff / Clean Sheet / No Goals / Result Combos.

    생성/보정하는 키들:
      - win_pct
      - btts_pct
      - team_over05_pct
      - team_over15_pct
      - over15_pct
      - over25_pct
      - goal_diff_avg
      - clean_sheet_pct
      - no_goals_pct
      - win_and_over25_pct
      - lose_and_btts_pct

    matches_total_api:
        API-Football 의 fixtures.played.total 같은 값.
        0 이면 실제 경기 수(mt_tot)를 그대로 사용.

    last_n:
        >0 이면 최근 last_n 경기만 집계 (date_utc 기준 DESC).
        0 이면 시즌 전체 경기 사용.
    """
    if season_int is None:
        return

        # Competition 필터 + Last N 에서 사용할 league_id 집합 결정
    league_ids_for_query: list[Any]
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if filters and isinstance(filters, dict):
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


    # ─────────────────────────────────────
    # 1) 샘플 매치 로딩 (시즌 전체 or 최근 N경기)
    # ─────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        ORDER BY m.date_utc DESC
    """

    params: list[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n and last_n > 0:
        base_sql += " LIMIT %s"
        params.append(last_n)

    match_rows = fetch_all(base_sql, tuple(params))

    if not match_rows:
        return

    # ─────────────────────────────────────
    # 2) 카운터 초기화
    # ─────────────────────────────────────
    mt_tot = mh_tot = ma_tot = 0

    win_t = win_h = win_a = 0
    draw_t = draw_h = draw_a = 0
    lose_t = lose_h = lose_a = 0

    btts_t = btts_h = btts_a = 0
    team_o05_t = team_o05_h = team_o05_a = 0
    team_o15_t = team_o15_h = team_o15_a = 0
    o15_t = o15_h = o15_a = 0
    o25_t = o25_h = o25_a = 0

    win_o25_t = win_o25_h = win_o25_a = 0
    lose_btts_t = lose_btts_h = lose_btts_a = 0

    cs_t = cs_h = cs_a = 0
    ng_t = ng_h = ng_a = 0

    gf_sum_t = gf_sum_h = gf_sum_a = 0
    ga_sum_t = ga_sum_h = ga_sum_a = 0

    # ─────────────────────────────────────
    # 3) 매치 루프
    # ─────────────────────────────────────
    for mr in match_rows:
        home_id = mr["home_id"]
        away_id = mr["away_id"]
        home_ft = mr["home_ft"]
        away_ft = mr["away_ft"]

        # 스코어가 비어 있으면 스킵
        if home_ft is None or away_ft is None:
            continue

        try:
            hf = int(home_ft)
            af = int(away_ft)
        except (TypeError, ValueError):
            continue

        is_home = home_id == team_id
        gf = hf if is_home else af
        ga = af if is_home else hf
        total_goals = hf + af

        mt_tot += 1
        gf_sum_t += gf
        ga_sum_t += ga

        if is_home:
            mh_tot += 1
            gf_sum_h += gf
            ga_sum_h += ga
        else:
            ma_tot += 1
            gf_sum_a += gf
            ga_sum_a += ga

        # W/D/L
        if gf > ga:
            win_t += 1
            if is_home:
                win_h += 1
            else:
                win_a += 1
        elif gf == ga:
            draw_t += 1
            if is_home:
                draw_h += 1
            else:
                draw_a += 1
        else:
            lose_t += 1
            if is_home:
                lose_h += 1
            else:
                lose_a += 1

        # BTTS / Team Over / Totals
        is_btts = (gf > 0 and ga > 0)
        if is_btts:
            btts_t += 1
            if is_home:
                btts_h += 1
            else:
                btts_a += 1

        if gf >= 1:
            team_o05_t += 1
            if is_home:
                team_o05_h += 1
            else:
                team_o05_a += 1

        if gf >= 2:
            team_o15_t += 1
            if is_home:
                team_o15_h += 1
            else:
                team_o15_a += 1

        if total_goals >= 2:
            o15_t += 1
            if is_home:
                o15_h += 1
            else:
                o15_a += 1

        if total_goals >= 3:
            o25_t += 1
            if is_home:
                o25_h += 1
            else:
                o25_a += 1

        # Clean sheet / No goals
        if ga == 0:
            cs_t += 1
            if is_home:
                cs_h += 1
            else:
                cs_a += 1

        if gf == 0:
            ng_t += 1
            if is_home:
                ng_h += 1
            else:
                ng_a += 1

        # 콤보: Win & Over2.5, Lose & BTTS
        if gf > ga and total_goals >= 3:
            win_o25_t += 1
            if is_home:
                win_o25_h += 1
            else:
                win_o25_a += 1

        if gf < ga and is_btts:
            lose_btts_t += 1
            if is_home:
                lose_btts_h += 1
            else:
                lose_btts_a += 1

    if mt_tot == 0:
        return

    # ─────────────────────────────────────
    # 4) 분모(경기 수) 결정
    #    - 가능한 한 실제로 집계에 사용된 경기 수(mt_tot)를 우선 사용
    #    - mt_tot 이 0(데이터가 없거나 쿼리 실패)일 때만
    #      fixtures.played.total(matches_total_api)를 폴백으로 사용
    # ─────────────────────────────────────
    if mt_tot > 0:
        eff_tot = mt_tot
    else:
        eff_tot = matches_total_api or 0

    eff_home = mh_tot or eff_tot
    eff_away = ma_tot or eff_tot


    # 4-1) 샘플 수(events_sample) 기록
    #      - 이미 다른 곳에서 유효한 값이 들어있으면 그대로 두고
    #      - 없으면 Outcome & Totals 분모(eff_tot)를 기준으로 세팅
    try:
        current_events_sample = insights.get("events_sample")
    except Exception:
        current_events_sample = None

    if not isinstance(current_events_sample, int) or current_events_sample <= 0:
        try:
            insights["events_sample"] = int(eff_tot)
        except (TypeError, ValueError):
            # 변환 실패 시에는 조용히 무시 (기존 동작 유지)
            pass

    # ─────────────────────────────────────
    # 5) 승률/오버/클린시트/노골 등 퍼센트
    # ─────────────────────────────────────
    insights["win_pct"] = {
        "total": fmt_pct(win_t, eff_tot),
        "home": fmt_pct(win_h, eff_home),
        "away": fmt_pct(win_a, eff_away),
    }
    insights["draw_pct"] = {
        "total": fmt_pct(draw_t, eff_tot),
        "home": fmt_pct(draw_h, eff_home),
        "away": fmt_pct(draw_a, eff_away),
    }
    insights["btts_pct"] = {
        "total": fmt_pct(btts_t, eff_tot),
        "home": fmt_pct(btts_h, eff_home),
        "away": fmt_pct(btts_a, eff_away),
    }
    insights["team_over05_pct"] = {
        "total": fmt_pct(team_o05_t, eff_tot),
        "home": fmt_pct(team_o05_h, eff_home),
        "away": fmt_pct(team_o05_a, eff_away),
    }
    insights["team_over15_pct"] = {
        "total": fmt_pct(team_o15_t, eff_tot),
        "home": fmt_pct(team_o15_h, eff_home),
        "away": fmt_pct(team_o15_a, eff_away),
    }
    insights["over15_pct"] = {
        "total": fmt_pct(o15_t, eff_tot),
        "home": fmt_pct(o15_h, eff_home),
        "away": fmt_pct(o15_a, eff_away),
    }
    insights["over25_pct"] = {
        "total": fmt_pct(o25_t, eff_tot),
        "home": fmt_pct(o25_h, eff_home),
        "away": fmt_pct(o25_a, eff_away),
    }

    # 클린 시트 / 노골
    insights["clean_sheet_pct"] = {
        "total": fmt_pct(cs_t, eff_tot),
        "home": fmt_pct(cs_h, eff_home),
        "away": fmt_pct(cs_a, eff_away),
    }
    insights["no_goals_pct"] = {
        "total": fmt_pct(ng_t, eff_tot),
        "home": fmt_pct(ng_h, eff_home),
        "away": fmt_pct(ng_a, eff_away),
    }

    # ─────────────────────────────────────
    # 6) 골 득실 차 평균 (GF - GA)
    # ─────────────────────────────────────
    gf_avg_t = fmt_avg(gf_sum_t, mt_tot)
    ga_avg_t = fmt_avg(ga_sum_t, mt_tot)
    gf_avg_h = fmt_avg(gf_sum_h, mh_tot)
    ga_avg_h = fmt_avg(ga_sum_h, mh_tot)
    gf_avg_a = fmt_avg(gf_sum_a, ma_tot)
    ga_avg_a = fmt_avg(ga_sum_a, ma_tot)

    # 🔥 새로 추가: AVG GF / AVG GA (total / home / away)
    insights["avg_gf"] = {
        "total": gf_avg_t,
        "home": gf_avg_h,
        "away": gf_avg_a,
    }
    insights["avg_ga"] = {
        "total": ga_avg_t,
        "home": ga_avg_h,
        "away": ga_avg_a,
    }

    diff_t = round(gf_avg_t - ga_avg_t, 2)
    diff_h = round(gf_avg_h - ga_avg_h, 2)
    diff_a = round(gf_avg_a - ga_avg_a, 2)

    insights["goal_diff_avg"] = {
        "total": diff_t,
        "home": diff_h,
        "away": diff_a,
    }


    # ─────────────────────────────────────
    # 7) 콤보 지표
    # ─────────────────────────────────────
    insights["win_and_over25_pct"] = {
        "total": fmt_pct(win_o25_t, eff_tot),
        "home": fmt_pct(win_o25_h, eff_home),
        "away": fmt_pct(win_o25_a, eff_away),
    }
    insights["lose_and_btts_pct"] = {
        "total": fmt_pct(lose_btts_t, eff_tot),
        "home": fmt_pct(lose_btts_h, eff_home),
        "away": fmt_pct(lose_btts_a, eff_away),
    }
