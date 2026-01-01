from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional
import math


def _get_triple(stat: Optional[dict], key: str = "total") -> Optional[float]:
    """
    TripleIntStat / TripleDoubleStat 형태:
      {"total": 12, "home": 10, "away": 14}
    에서 숫자를 꺼내는 헬퍼.
    """
    if not isinstance(stat, dict):
        return None
    v = stat.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_pct(x: float) -> int:
    """0~100 퍼센트 정수로 클램프."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        v = 0.0
    return max(0, min(100, int(round(v))))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    try:
        return math.exp(-lam) * lam**k / math.factorial(k)
    except OverflowError:
        return 0.0


def _joint_score_probs(lam_home: float, lam_away: float, max_goals: int = 7) -> Dict[Tuple[int, int], float]:
    """포아송 독립 가정으로 스코어라인 결합 확률 테이블을 만든다."""
    probs: Dict[Tuple[int, int], float] = {}
    for h in range(0, max_goals + 1):
        ph = _poisson_pmf(h, lam_home)
        for a in range(0, max_goals + 1):
            pa = _poisson_pmf(a, lam_away)
            probs[(h, a)] = ph * pa

    # truncation 보정: 합이 1이 되도록 정규화
    s = sum(probs.values())
    if s > 0:
        for k in probs:
            probs[k] /= s
    return probs


def _blend(a: Optional[float], b: Optional[float], wa: float, wb: float) -> Optional[float]:
    """None-safe weighted blend."""
    vals: List[float] = []
    weights: List[float] = []
    if a is not None:
        vals.append(a)
        weights.append(wa)
    if b is not None:
        vals.append(b)
        weights.append(wb)
    if not vals or sum(weights) == 0:
        return None
    wsum = sum(weights)
    return sum(v * w for v, w in zip(vals, weights)) / wsum


def _market_probs_from_score_probs(score_probs: Dict[Tuple[int, int], float]) -> Dict[str, float]:
    """스코어 테이블에서 시장(1X2/오버/BTTS/클린시트/팀오버)을 계산."""
    p_home = sum(p for (h, a), p in score_probs.items() if h > a)
    p_draw = sum(p for (h, a), p in score_probs.items() if h == a)
    p_away = max(0.0, 1.0 - p_home - p_draw)

    p_1x = p_home + p_draw
    p_12 = p_home + p_away
    p_x2 = p_draw + p_away

    p_over05 = sum(p for (h, a), p in score_probs.items() if (h + a) >= 1)
    p_over15 = sum(p for (h, a), p in score_probs.items() if (h + a) >= 2)
    p_over25 = sum(p for (h, a), p in score_probs.items() if (h + a) >= 3)
    p_over35 = sum(p for (h, a), p in score_probs.items() if (h + a) >= 4)

    p_home_over05 = sum(p for (h, a), p in score_probs.items() if h >= 1)
    p_home_over15 = sum(p for (h, a), p in score_probs.items() if h >= 2)
    p_away_over05 = sum(p for (h, a), p in score_probs.items() if a >= 1)
    p_away_over15 = sum(p for (h, a), p in score_probs.items() if a >= 2)

    p_btts_yes = sum(p for (h, a), p in score_probs.items() if h >= 1 and a >= 1)
    p_btts_no = 1.0 - p_btts_yes

    p_home_cs = sum(p for (h, a), p in score_probs.items() if a == 0)  # away 0
    p_away_cs = sum(p for (h, a), p in score_probs.items() if h == 0)  # home 0

    return {
        "p_home": p_home,
        "p_draw": p_draw,
        "p_away": p_away,
        "p_1x": p_1x,
        "p_12": p_12,
        "p_x2": p_x2,
        "p_over05": p_over05,
        "p_over15": p_over15,
        "p_over25": p_over25,
        "p_over35": p_over35,
        "p_home_over05": p_home_over05,
        "p_home_over15": p_home_over15,
        "p_away_over05": p_away_over05,
        "p_away_over15": p_away_over15,
        "p_btts_yes": p_btts_yes,
        "p_btts_no": p_btts_no,
        "p_home_cs": p_home_cs,
        "p_away_cs": p_away_cs,
    }


def compute_ai_predictions_from_lambdas(
    *,
    lam_home_ft: float,
    lam_away_ft: float,
    lam_home_1h: Optional[float] = None,
    lam_away_1h: Optional[float] = None,
    lam_home_2h: Optional[float] = None,
    lam_away_2h: Optional[float] = None,
    insights_overall: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    λ(기대득점)을 입력 받아 AI Predictions 결과 딕셔너리를 생성한다.

    - FT/1H/2H 각각 스코어 확률표를 만들고, 동일한 방식으로 시장 확률을 계산한다.
    - insights_overall 이 주어지면 (퍼스트골/리드/카드/코너/시간대골) 보조 지표를 기존처럼 추가한다.
    """
    lam_home_ft = max(0.05, float(lam_home_ft or 0.05))
    lam_away_ft = max(0.05, float(lam_away_ft or 0.05))

    # ───── FT ─────
    score_probs_ft = _joint_score_probs(lam_home_ft, lam_away_ft, max_goals=7)
    m_ft = _market_probs_from_score_probs(score_probs_ft)

    # 1X2는 기존처럼 히스토리 보정(있으면)
    p_home_final = m_ft["p_home"]
    p_draw_final = m_ft["p_draw"]
    p_away_final = m_ft["p_away"]

    if isinstance(insights_overall, dict):
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}

        h_win_hist = _get_triple(home.get("win_pct"))
        h_draw_hist = _get_triple(home.get("draw_pct"))
        a_win_hist = _get_triple(away.get("win_pct"))

        if h_win_hist is not None and a_win_hist is not None:
            p_home_hist = (h_win_hist / 100.0 + (100.0 - a_win_hist) / 100.0) / 2.0
        else:
            p_home_hist = None
        p_draw_hist = h_draw_hist / 100.0 if h_draw_hist is not None else None

        def _blend_prob(poiss: float, hist: Optional[float]) -> float:
            if hist is None:
                return poiss
            # 포아송 70% + 과거 승률 30%
            return 0.7 * poiss + 0.3 * hist

        p_home_final = _blend_prob(m_ft["p_home"], p_home_hist)
        p_draw_final = _blend_prob(m_ft["p_draw"], p_draw_hist)
        p_away_final = max(0.0, 1.0 - p_home_final - p_draw_final)

    # FT 더블찬스는 "보정 1X2" 기반으로 계산
    ft_1x = p_home_final + p_draw_final
    ft_12 = p_home_final + p_away_final
    ft_x2 = p_draw_final + p_away_final

    out: Dict[str, Any] = {
        # FT 1X2 + Double chance (기존 키 유지)
        "home_win_pct": _safe_pct(p_home_final * 100),
        "draw_pct": _safe_pct(p_draw_final * 100),
        "away_win_pct": _safe_pct(p_away_final * 100),
        "home_or_draw_pct": _safe_pct(ft_1x * 100),
        "home_or_away_pct": _safe_pct(ft_12 * 100),
        "draw_or_away_pct": _safe_pct(ft_x2 * 100),

        # FT Totals (기존 over15/over25/over35 유지 + over05 추가)
        "over05_pct": _safe_pct(m_ft["p_over05"] * 100),
        "over15_pct": _safe_pct(m_ft["p_over15"] * 100),
        "over25_pct": _safe_pct(m_ft["p_over25"] * 100),
        "over35_pct": _safe_pct(m_ft["p_over35"] * 100),

        # FT Team totals (기존 키 유지)
        "home_team_over05_pct": _safe_pct(m_ft["p_home_over05"] * 100),
        "home_team_over15_pct": _safe_pct(m_ft["p_home_over15"] * 100),
        "away_team_over05_pct": _safe_pct(m_ft["p_away_over05"] * 100),
        "away_team_over15_pct": _safe_pct(m_ft["p_away_over15"] * 100),

        # FT BTTS / Clean Sheet (기존 키 유지)
        "btts_yes_pct": _safe_pct(m_ft["p_btts_yes"] * 100),
        "btts_no_pct": _safe_pct(m_ft["p_btts_no"] * 100),
        "home_clean_sheet_pct": _safe_pct(m_ft["p_home_cs"] * 100),
        "away_clean_sheet_pct": _safe_pct(m_ft["p_away_cs"] * 100),

        # 골 못 넣음(기존 키 유지)
        "home_no_goals_pct": _safe_pct((1.0 - m_ft["p_home_over05"]) * 100),
        "away_no_goals_pct": _safe_pct((1.0 - m_ft["p_away_over05"]) * 100),
    }

    # ───── 1H/2H (요구 항목 전체 추가) ─────
    # 입력이 없으면 FT 비율로 동일하게 쪼개는 폴백
    if lam_home_1h is None or lam_away_1h is None or lam_home_2h is None or lam_away_2h is None:
        ratio_1h = 0.45
        if isinstance(insights_overall, dict):
            home = insights_overall.get("home") or {}
            away = insights_overall.get("away") or {}

            gbt_home_for = home.get("goals_by_time_for") or []
            gbt_home_against = home.get("goals_by_time_against") or []
            gbt_away_for = away.get("goals_by_time_for") or []
            gbt_away_against = away.get("goals_by_time_against") or []

            def _sum_idxs(arr, idxs):
                s = 0.0
                for i in idxs:
                    try:
                        v = float(arr[i])
                    except Exception:
                        v = 0.0
                    s += v
                return s

            total_1h = (
                _sum_idxs(gbt_home_for, range(0, 5))
                + _sum_idxs(gbt_home_against, range(0, 5))
                + _sum_idxs(gbt_away_for, range(0, 5))
                + _sum_idxs(gbt_away_against, range(0, 5))
            )
            total_2h = (
                _sum_idxs(gbt_home_for, range(5, 10))
                + _sum_idxs(gbt_home_against, range(5, 10))
                + _sum_idxs(gbt_away_for, range(5, 10))
                + _sum_idxs(gbt_away_against, range(5, 10))
            )
            total = total_1h + total_2h
            if total > 0:
                ratio_1h = total_1h / total

        lam_home_1h = lam_home_ft * ratio_1h
        lam_away_1h = lam_away_ft * ratio_1h
        lam_home_2h = max(0.0, lam_home_ft - lam_home_1h)
        lam_away_2h = max(0.0, lam_away_ft - lam_away_1h)

    # 1H
    score_probs_1h = _joint_score_probs(max(0.01, lam_home_1h), max(0.01, lam_away_1h), max_goals=5)
    m_1h = _market_probs_from_score_probs(score_probs_1h)
    out.update({
        "first_half_home_win_pct": _safe_pct(m_1h["p_home"] * 100),
        "first_half_draw_pct": _safe_pct(m_1h["p_draw"] * 100),
        "first_half_away_win_pct": _safe_pct(m_1h["p_away"] * 100),

        "first_half_home_or_draw_pct": _safe_pct(m_1h["p_1x"] * 100),
        "first_half_home_or_away_pct": _safe_pct(m_1h["p_12"] * 100),
        "first_half_draw_or_away_pct": _safe_pct(m_1h["p_x2"] * 100),

        "first_half_over05_pct": _safe_pct(m_1h["p_over05"] * 100),
        "first_half_over15_pct": _safe_pct(m_1h["p_over15"] * 100),

        "first_half_home_team_over05_pct": _safe_pct(m_1h["p_home_over05"] * 100),
        "first_half_home_team_over15_pct": _safe_pct(m_1h["p_home_over15"] * 100),
        "first_half_away_team_over05_pct": _safe_pct(m_1h["p_away_over05"] * 100),
        "first_half_away_team_over15_pct": _safe_pct(m_1h["p_away_over15"] * 100),

        "first_half_btts_yes_pct": _safe_pct(m_1h["p_btts_yes"] * 100),
        "first_half_btts_no_pct": _safe_pct(m_1h["p_btts_no"] * 100),
        "first_half_home_clean_sheet_pct": _safe_pct(m_1h["p_home_cs"] * 100),
        "first_half_away_clean_sheet_pct": _safe_pct(m_1h["p_away_cs"] * 100),
    })

    # 2H
    score_probs_2h = _joint_score_probs(max(0.01, lam_home_2h), max(0.01, lam_away_2h), max_goals=5)
    m_2h = _market_probs_from_score_probs(score_probs_2h)
    out.update({
        "second_half_home_win_pct": _safe_pct(m_2h["p_home"] * 100),
        "second_half_draw_pct": _safe_pct(m_2h["p_draw"] * 100),
        "second_half_away_win_pct": _safe_pct(m_2h["p_away"] * 100),

        "second_half_home_or_draw_pct": _safe_pct(m_2h["p_1x"] * 100),
        "second_half_home_or_away_pct": _safe_pct(m_2h["p_12"] * 100),
        "second_half_draw_or_away_pct": _safe_pct(m_2h["p_x2"] * 100),

        "second_half_over05_pct": _safe_pct(m_2h["p_over05"] * 100),
        "second_half_over15_pct": _safe_pct(m_2h["p_over15"] * 100),

        "second_half_home_team_over05_pct": _safe_pct(m_2h["p_home_over05"] * 100),
        "second_half_home_team_over15_pct": _safe_pct(m_2h["p_home_over15"] * 100),
        "second_half_away_team_over05_pct": _safe_pct(m_2h["p_away_over05"] * 100),
        "second_half_away_team_over15_pct": _safe_pct(m_2h["p_away_over15"] * 100),

        "second_half_btts_yes_pct": _safe_pct(m_2h["p_btts_yes"] * 100),
        "second_half_btts_no_pct": _safe_pct(m_2h["p_btts_no"] * 100),
        "second_half_home_clean_sheet_pct": _safe_pct(m_2h["p_home_cs"] * 100),
        "second_half_away_clean_sheet_pct": _safe_pct(m_2h["p_away_cs"] * 100),
    })

    # ───── 부가 지표 (기존 키 유지) ─────
    # most likely score / top3
    most_likely = max(score_probs_ft.items(), key=lambda kv: kv[1])[0]
    out["most_likely_score"] = f"{most_likely[0]}-{most_likely[1]}"
    top3 = sorted(score_probs_ft.items(), key=lambda kv: kv[1], reverse=True)[:3]
    out["top3_scorelines"] = [f"{h}-{a}" for (h, a), _ in top3]

    # 하프에 골이 날 확률(기존 키 유지)
    lam_total_ft = lam_home_ft + lam_away_ft
    lam_total_1h = max(0.01, (lam_home_1h or 0.0) + (lam_away_1h or 0.0))
    lam_total_2h = max(0.01, (lam_home_2h or 0.0) + (lam_away_2h or 0.0))
    p_goal_1h = 1.0 - math.exp(-lam_total_1h)
    p_goal_2h = 1.0 - math.exp(-lam_total_2h)
    out["goal_1st_half_pct"] = _safe_pct(p_goal_1h * 100)
    out["goal_2nd_half_pct"] = _safe_pct(p_goal_2h * 100)
    out["both_halves_goal_pct"] = _safe_pct((p_goal_1h * p_goal_2h) * 100)  # 독립 근사
    out["second_half_more_goals_pct"] = _safe_pct((lam_total_2h / (lam_total_1h + lam_total_2h)) * 100)

    # 0-15 / 80-90+ (goals_by_time bucket 기반, 있으면)
    p_goal_0_15 = 0.0
    p_goal_80_90 = 0.0
    if isinstance(insights_overall, dict):
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}
        gbt_home_for = home.get("goals_by_time_for") or []
        gbt_home_against = home.get("goals_by_time_against") or []
        gbt_away_for = away.get("goals_by_time_for") or []
        gbt_away_against = away.get("goals_by_time_against") or []

        def _sum_idxs(arr, idxs):
            s = 0.0
            for i in idxs:
                try:
                    v = float(arr[i])
                except Exception:
                    v = 0.0
                s += v
            return s

        total_all = (
            _sum_idxs(gbt_home_for, range(0, 10))
            + _sum_idxs(gbt_home_against, range(0, 10))
            + _sum_idxs(gbt_away_for, range(0, 10))
            + _sum_idxs(gbt_away_against, range(0, 10))
        )

        b0 = _sum_idxs(gbt_home_for, [0]) + _sum_idxs(gbt_home_against, [0]) + _sum_idxs(gbt_away_for, [0]) + _sum_idxs(gbt_away_against, [0])
        b9 = _sum_idxs(gbt_home_for, [9]) + _sum_idxs(gbt_home_against, [9]) + _sum_idxs(gbt_away_for, [9]) + _sum_idxs(gbt_away_against, [9])

        if total_all > 0:
            share_0_15 = b0 / total_all
            share_80_90 = b9 / total_all
            lam_0_15 = lam_total_ft * share_0_15
            lam_80_90 = lam_total_ft * share_80_90
            p_goal_0_15 = 1.0 - math.exp(-lam_0_15)
            p_goal_80_90 = 1.0 - math.exp(-lam_80_90)

    out["goal_0_15_pct"] = _safe_pct(p_goal_0_15 * 100)
    out["goal_80_90_pct"] = _safe_pct(p_goal_80_90 * 100)

    # 퍼스트골(히스토리 있으면, 없으면 λ 비율) + 리드/트레일 (기존 키 유지)
    p_first_home = 0.5
    if isinstance(insights_overall, dict):
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}
        first_home_hist = _get_triple(home.get("first_to_score_pct"))
        first_away_hist = _get_triple(away.get("first_to_score_pct"))
        if first_home_hist is not None and first_away_hist is not None:
            p_first_home = (first_home_hist / 100.0 + (100.0 - first_away_hist) / 100.0) / 2.0
        else:
            p_first_home = lam_home_ft / (lam_home_ft + lam_away_ft) if (lam_home_ft + lam_away_ft) > 0 else 0.5
        when_leading_win_home = _get_triple(home.get("when_leading_win_pct"))
        when_trailing_win_home = _get_triple(home.get("when_trailing_win_pct"))
    else:
        when_leading_win_home = None
        when_trailing_win_home = None

    out["first_goal_home_pct"] = _safe_pct(p_first_home * 100)
    out["first_goal_away_pct"] = _safe_pct((1.0 - p_first_home) * 100)
    out["if_home_scores_first_win_pct"] = _safe_pct(when_leading_win_home or 0.0)
    out["if_away_scores_first_home_win_pct"] = _safe_pct(when_trailing_win_home or 0.0)

    # 카드/코너 (기존 키 유지)
    p_high_cards = 0.0
    p_high_corners = 0.0
    if isinstance(insights_overall, dict):
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}

        def _to_f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        y_h = _to_f(home.get("yellow_per_match"))
        y_a = _to_f(away.get("yellow_per_match"))
        r_h = _to_f(home.get("red_per_match"))
        r_a = _to_f(away.get("red_per_match"))
        c_h = _to_f(home.get("corners_per_match"))
        c_a = _to_f(away.get("corners_per_match"))

        avg_cards = 0.0
        cnt = 0
        for v in [y_h, y_a]:
            if v is not None:
                avg_cards += v
                cnt += 1
        # red는 가중치 2배 정도
        for v in [r_h, r_a]:
            if v is not None:
                avg_cards += 2.0 * v
                cnt += 1
        avg_cards = avg_cards / cnt if cnt > 0 else 4.5

        high_cards_index = (avg_cards - 3.0) / (7.5 - 3.0)
        p_high_cards = _clamp(0.3 + 0.7 * high_cards_index, 0.0, 1.0)

        avg_corners = 0.0
        cntc = 0
        for v in [c_h, c_a]:
            if v is not None:
                avg_corners += v
                cntc += 1
        avg_corners = avg_corners / cntc if cntc > 0 else 8.5

        high_corners_index = (avg_corners - 7.0) / (12.0 - 7.0)
        p_high_corners = _clamp(0.4 + 0.6 * high_corners_index, 0.0, 1.0)

    out["high_cards_game_pct"] = _safe_pct(p_high_cards * 100)
    out["high_corners_game_pct"] = _safe_pct(p_high_corners * 100)

    return out


def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    (기존 호환용) match_detail_bundle 내 insights_overall 블록을 기반으로
    AI Predictions 결과 딕셔너리를 생성한다.

    ✅ 참고:
    - 지금은 build_ai_predictions_block 쪽에서 DB 기반 λ를 계산해서
      compute_ai_predictions_from_lambdas(...) 를 호출하도록 확장됨.
    - 이 함수는 fallback / 호환 목적.
    """
    home = insights_overall.get("home")
    away = insights_overall.get("away")
    if not home or not away:
        return None

    # ───── 1) 공격/수비 strength (최근 + 시즌 홈/원정 블렌딩) ─────
    h_att_recent = _get_triple(home.get("avg_gf"), "total")
    a_att_recent = _get_triple(away.get("avg_gf"), "total")
    h_def_recent = _get_triple(home.get("avg_ga"), "total")
    a_def_recent = _get_triple(away.get("avg_ga"), "total")

    h_att_season_home = _get_triple(home.get("avg_gf"), "home")
    a_att_season_away = _get_triple(away.get("avg_gf"), "away")
    h_def_season_home = _get_triple(home.get("avg_ga"), "home")
    a_def_season_away = _get_triple(away.get("avg_ga"), "away")

    # 최근 65% + 시즌 홈/원정 35%
    h_attack = _blend(h_att_recent, h_att_season_home, 0.65, 0.35)
    a_attack = _blend(a_att_recent, a_att_season_away, 0.65, 0.35)
    h_def = _blend(h_def_recent, h_def_season_home, 0.65, 0.35)
    a_def = _blend(a_def_recent, a_def_season_away, 0.65, 0.35)

    if h_attack is None or a_attack is None:
        return None

    # “리그 평균 실점” 대략 추정 (양 팀 recent 평균의 평균)
    league_ga_samples = [v for v in [h_def_recent, a_def_recent] if v is not None]
    league_avg_ga = sum(league_ga_samples) / len(league_ga_samples) if league_ga_samples else 1.2

    if h_def is None:
        h_def = league_avg_ga
    if a_def is None:
        a_def = league_avg_ga

    # ───── 2) FT λ 계산 (fallback) ─────
    lam_home = max(0.1, h_attack * (a_def / league_avg_ga))
    lam_away = max(0.1, a_attack * (h_def / league_avg_ga))

    # 하프 λ는 goals_by_time 기반 비율로 폴백 분해
    ratio_1h = 0.45
    gbt_home_for = home.get("goals_by_time_for") or []
    gbt_home_against = home.get("goals_by_time_against") or []
    gbt_away_for = away.get("goals_by_time_for") or []
    gbt_away_against = away.get("goals_by_time_against") or []

    def _sum_idxs(arr, idxs):
        s = 0.0
        for i in idxs:
            try:
                v = float(arr[i])
            except Exception:
                v = 0.0
            s += v
        return s

    total_1h = (
        _sum_idxs(gbt_home_for, range(0, 5))
        + _sum_idxs(gbt_home_against, range(0, 5))
        + _sum_idxs(gbt_away_for, range(0, 5))
        + _sum_idxs(gbt_away_against, range(0, 5))
    )
    total_2h = (
        _sum_idxs(gbt_home_for, range(5, 10))
        + _sum_idxs(gbt_home_against, range(5, 10))
        + _sum_idxs(gbt_away_for, range(5, 10))
        + _sum_idxs(gbt_away_against, range(5, 10))
    )
    total = total_1h + total_2h
    if total > 0:
        ratio_1h = total_1h / total

    lam_home_1h = lam_home * ratio_1h
    lam_away_1h = lam_away * ratio_1h
    lam_home_2h = max(0.0, lam_home - lam_home_1h)
    lam_away_2h = max(0.0, lam_away - lam_away_1h)

    return compute_ai_predictions_from_lambdas(
        lam_home_ft=lam_home,
        lam_away_ft=lam_away,
        lam_home_1h=lam_home_1h,
        lam_away_1h=lam_away_1h,
        lam_home_2h=lam_home_2h,
        lam_away_2h=lam_away_2h,
        insights_overall=insights_overall,
    )
