from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def _safe_div(num, den) -> float:
    try:
        num_f = float(num)
    except (TypeError, ValueError):
        return 0.0
    try:
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0
    if den_f == 0:
        return 0.0
    return num_f / den_f


def _fmt_pct(n, d) -> int:
    v = _safe_div(n, d)
    return int(round(v * 100)) if v > 0 else 0


def insights_overall_timing(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    기존 home_service.get_team_season_stats 안의
    'Timing / First Goal / Momentum (match_events 기반)' 블록 전체를 옮긴 함수.

    여기서 score_1h_pct, score_2h_pct,
    concede_1h_pct, concede_2h_pct,
    score_0_15_pct, concede_0_15_pct,
    score_80_90_pct, concede_80_90_pct,
    first_to_score_pct, first_conceded_pct,
    when_leading_*, when_trailing_* 등을 계산한다.
    """

    if season_int is None:
        return

    match_rows: List[Dict[str, Any]] = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group
        FROM matches m
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        """,
        (league_id, season_int, team_id, team_id),
    )

    if not match_rows:
        return

    fixture_ids = [mr["fixture_id"] for mr in match_rows]
    events_by_fixture: Dict[int, List[Dict[str, Any]]] = {}

    if fixture_ids:
        placeholders = ",".join(["%s"] * len(fixture_ids))
        sql = f"""
            SELECT
                e.fixture_id,
                e.minute,
                e.team_id
            FROM match_events e
            WHERE e.fixture_id IN ({placeholders})
              AND e.minute IS NOT NULL
              AND lower(e.type) IN ('goal','own goal','penalty','penalty goal')
            ORDER BY e.fixture_id, e.minute ASC
        """
        event_rows = fetch_all(sql, tuple(fixture_ids))
        for ev in event_rows:
            fid = ev["fixture_id"]
            events_by_fixture.setdefault(fid, []).append(ev)

    half_mt_tot = half_mt_home = half_mt_away = 0

    score_1h_t = score_1h_h = score_1h_a = 0
    score_2h_t = score_2h_h = score_2h_a = 0
    concede_1h_t = concede_1h_h = concede_1h_a = 0
    concede_2h_t = concede_2h_h = concede_2h_a = 0

    score_015_t = score_015_h = score_015_a = 0
    concede_015_t = concede_015_h = concede_015_a = 0
    score_8090_t = score_8090_h = score_8090_a = 0
    concede_8090_t = concede_8090_h = concede_8090_a = 0

    fg_sample_t = fg_sample_h = fg_sample_a = 0
    fg_for_t = fg_for_h = fg_for_a = 0
    fg_against_t = fg_against_h = fg_against_a = 0

    leading_sample_t = leading_sample_h = leading_sample_a = 0
    leading_win_t = leading_win_h = leading_win_a = 0
    leading_draw_t = leading_draw_h = leading_draw_a = 0
    leading_loss_t = leading_loss_h = leading_loss_a = 0

    trailing_sample_t = trailing_sample_h = trailing_sample_a = 0
    trailing_win_t = trailing_win_h = trailing_win_a = 0
    trailing_draw_t = trailing_draw_h = trailing_draw_a = 0
    trailing_loss_t = trailing_loss_h = trailing_loss_a = 0

    for mr in match_rows:
        fid = mr["fixture_id"]
        home_id = mr["home_id"]
        away_id = mr["away_id"]
        home_ft = mr["home_ft"]
        away_ft = mr["away_ft"]

        if home_ft is None or away_ft is None:
            continue

        is_home = (team_id == home_id)
        gf = home_ft if is_home else away_ft
        ga = away_ft if is_home else home_ft

        evs = events_by_fixture.get(fid)
        if not evs:
            continue

        half_mt_tot += 1
        if is_home:
            half_mt_home += 1
        else:
            half_mt_away += 1

        scored_1h = conceded_1h = False
        scored_2h = conceded_2h = False
        scored_015 = conceded_015 = False
        scored_8090 = conceded_8090 = False

        first_minute: Optional[int] = None
        first_for: Optional[bool] = None

        for ev in evs:
            minute = ev["minute"]
            if minute is None:
                continue
            try:
                m_int = int(minute)
            except (TypeError, ValueError):
                continue

            is_for_goal = (ev["team_id"] == team_id)

            if first_minute is None or m_int < first_minute:
                first_minute = m_int
                first_for = is_for_goal

            if m_int <= 45:
                if is_for_goal:
                    scored_1h = True
                else:
                    conceded_1h = True
            else:
                if is_for_goal:
                    scored_2h = True
                else:
                    conceded_2h = True

            if m_int <= 15:
                if is_for_goal:
                    scored_015 = True
                else:
                    conceded_015 = True

            if m_int >= 80:
                if is_for_goal:
                    scored_8090 = True
                else:
                    conceded_8090 = True

        def _inc(flag: bool, total_ref, home_ref, away_ref):
            if not flag:
                return
            if is_home:
                home_ref[0] += 1
            else:
                away_ref[0] += 1
            total_ref[0] += 1

        t_ref = [score_1h_t]
        h_ref = [score_1h_h]
        a_ref = [score_1h_a]
        _inc(scored_1h, t_ref, h_ref, a_ref)
        score_1h_t, score_1h_h, score_1h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_2h_t]
        h_ref = [score_2h_h]
        a_ref = [score_2h_a]
        _inc(scored_2h, t_ref, h_ref, a_ref)
        score_2h_t, score_2h_h, score_2h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_1h_t]
        h_ref = [concede_1h_h]
        a_ref = [concede_1h_a]
        _inc(conceded_1h, t_ref, h_ref, a_ref)
        concede_1h_t, concede_1h_h, concede_1h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_2h_t]
        h_ref = [concede_2h_h]
        a_ref = [concede_2h_a]
        _inc(conceded_2h, t_ref, h_ref, a_ref)
        concede_2h_t, concede_2h_h, concede_2h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_015_t]
        h_ref = [score_015_h]
        a_ref = [score_015_a]
        _inc(scored_015, t_ref, h_ref, a_ref)
        score_015_t, score_015_h, score_015_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_015_t]
        h_ref = [concede_015_h]
        a_ref = [concede_015_a]
        _inc(conceded_015, t_ref, h_ref, a_ref)
        concede_015_t, concede_015_h, concede_015_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_8090_t]
        h_ref = [score_8090_h]
        a_ref = [score_8090_a]
        _inc(scored_8090, t_ref, h_ref, a_ref)
        score_8090_t, score_8090_h, score_8090_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_8090_t]
        h_ref = [concede_8090_h]
        a_ref = [concede_8090_a]
        _inc(conceded_8090, t_ref, h_ref, a_ref)
        concede_8090_t, concede_8090_h, concede_8090_a = t_ref[0], h_ref[0], a_ref[0]

        if first_minute is not None and first_for is not None:
            fg_sample_t += 1
            if is_home:
                fg_sample_h += 1
            else:
                fg_sample_a += 1

            if first_for:
                fg_for_t += 1
                if is_home:
                    fg_for_h += 1
                else:
                    fg_for_a += 1

                leading_sample_t += 1
                if is_home:
                    leading_sample_h += 1
                else:
                    leading_sample_a += 1

                if gf > ga:
                    leading_win_t += 1
                    if is_home:
                        leading_win_h += 1
                    else:
                        leading_win_a += 1
                elif gf == ga:
                    leading_draw_t += 1
                    if is_home:
                        leading_draw_h += 1
                    else:
                        leading_draw_a += 1
                else:
                    leading_loss_t += 1
                    if is_home:
                        leading_loss_h += 1
                    else:
                        leading_loss_a += 1
            else:
                fg_against_t += 1
                if is_home:
                    fg_against_h += 1
                else:
                    fg_against_a += 1

                trailing_sample_t += 1
                if is_home:
                    trailing_sample_h += 1
                else:
                    trailing_sample_a += 1

                if gf > ga:
                    trailing_win_t += 1
                    if is_home:
                        trailing_win_h += 1
                    else:
                        trailing_win_a += 1
                elif gf == ga:
                    trailing_draw_t += 1
                    if is_home:
                        trailing_draw_h += 1
                    else:
                        trailing_draw_a += 1
                else:
                    trailing_loss_t += 1
                    if is_home:
                        trailing_loss_h += 1
                    else:
                        trailing_loss_a += 1

    if half_mt_tot > 0:
        insights["score_1h_pct"] = {
            "total": _fmt_pct(score_1h_t, half_mt_tot),
            "home": _fmt_pct(score_1h_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(score_1h_a, half_mt_away or half_mt_tot),
        }
        insights["score_2h_pct"] = {
            "total": _fmt_pct(score_2h_t, half_mt_tot),
            "home": _fmt_pct(score_2h_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(score_2h_a, half_mt_away or half_mt_tot),
        }
        insights["concede_1h_pct"] = {
            "total": _fmt_pct(concede_1h_t, half_mt_tot),
            "home": _fmt_pct(concede_1h_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(concede_1h_a, half_mt_away or half_mt_tot),
        }
        insights["concede_2h_pct"] = {
            "total": _fmt_pct(concede_2h_t, half_mt_tot),
            "home": _fmt_pct(concede_2h_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(concede_2h_a, half_mt_away or half_mt_tot),
        }
        insights["score_0_15_pct"] = {
            "total": _fmt_pct(score_015_t, half_mt_tot),
            "home": _fmt_pct(score_015_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(score_015_a, half_mt_away or half_mt_tot),
        }
        insights["concede_0_15_pct"] = {
            "total": _fmt_pct(concede_015_t, half_mt_tot),
            "home": _fmt_pct(concede_015_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(concede_015_a, half_mt_away or half_mt_tot),
        }
        insights["score_80_90_pct"] = {
            "total": _fmt_pct(score_8090_t, half_mt_tot),
            "home": _fmt_pct(score_8090_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(score_8090_a, half_mt_away or half_mt_tot),
        }
        insights["concede_80_90_pct"] = {
            "total": _fmt_pct(concede_8090_t, half_mt_tot),
            "home": _fmt_pct(concede_8090_h, half_mt_home or half_mt_tot),
            "away": _fmt_pct(concede_8090_a, half_mt_away or half_mt_tot),
        }

    if fg_sample_t > 0:
        insights["first_to_score_pct"] = {
            "total": _fmt_pct(fg_for_t, fg_sample_t),
            "home": _fmt_pct(fg_for_h, fg_sample_h or fg_sample_t),
            "away": _fmt_pct(fg_for_a, fg_sample_a or fg_sample_t),
        }
        insights["first_conceded_pct"] = {
            "total": _fmt_pct(fg_against_t, fg_sample_t),
            "home": _fmt_pct(fg_against_h, fg_sample_h or fg_sample_t),
            "away": _fmt_pct(fg_against_a, fg_sample_a or fg_sample_t),
        }

    if leading_sample_t > 0:
        insights["when_leading_win_pct"] = {
            "total": _fmt_pct(leading_win_t, leading_sample_t),
            "home": _fmt_pct(leading_win_h, leading_sample_h or leading_sample_t),
            "away": _fmt_pct(leading_win_a, leading_sample_a or leading_sample_t),
        }
        insights["when_leading_draw_pct"] = {
            "total": _fmt_pct(leading_draw_t, leading_sample_t),
            "home": _fmt_pct(leading_draw_h, leading_sample_h or leading_sample_t),
            "away": _fmt_pct(leading_draw_a, leading_sample_a or leading_sample_t),
        }
        insights["when_leading_loss_pct"] = {
            "total": _fmt_pct(leading_loss_t, leading_sample_t),
            "home": _fmt_pct(leading_loss_h, leading_sample_h or leading_sample_t),
            "away": _fmt_pct(leading_loss_a, leading_sample_a or leading_sample_t),
        }

    if trailing_sample_t > 0:
        insights["when_trailing_win_pct"] = {
            "total": _fmt_pct(trailing_win_t, trailing_sample_t),
            "home": _fmt_pct(trailing_win_h, trailing_sample_h or trailing_sample_t),
            "away": _fmt_pct(trailing_win_a, trailing_sample_a or trailing_sample_t),
        }
        insights["when_trailing_draw_pct"] = {
            "total": _fmt_pct(trailing_draw_t, trailing_sample_t),
            "home": _fmt_pct(trailing_draw_h, trailing_sample_h or trailing_sample_t),
            "away": _fmt_pct(trailing_draw_a, trailing_sample_a or trailing_sample_t),
        }
        insights["when_trailing_loss_pct"] = {
            "total": _fmt_pct(trailing_loss_t, trailing_sample_t),
            "home": _fmt_pct(trailing_loss_h, trailing_sample_h or trailing_sample_t),
            "away": _fmt_pct(trailing_loss_a, trailing_sample_a or trailing_sample_t),
        }

    insights["events_sample"] = half_mt_tot
    insights["first_goal_sample"] = fg_sample_t
