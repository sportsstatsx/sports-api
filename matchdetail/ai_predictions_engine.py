from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional
import math


# ============================================================
#  AI Predictions Engine
#  - Outputs ONLY requested markets:
#    FT / 1H / 2H 1X2, Double chance, Totals Over,
#    Team Totals, BTTS, Clean Sheets,
#    First goal (H/A), Goal 0-15, Goal 80-90+
# ============================================================


# -----------------------------
# Small helpers
# -----------------------------
def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _pct(p: Optional[float]) -> int:
    if p is None:
        return 0
    return int(round(_clamp01(float(p)) * 100.0))


def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0.0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _poisson_cdf(k: int, lam: float) -> float:
    return sum(_poisson_pmf(i, lam) for i in range(0, k + 1))


def _result_probs_1x2(lam_home: float, lam_away: float, max_goals: int = 10) -> Tuple[float, float, float]:
    """
    Returns (P(Home Win), P(Draw), P(Away Win)) using truncated Poisson score grid.
    """
    ph = [_poisson_pmf(i, lam_home) for i in range(0, max_goals + 1)]
    pa = [_poisson_pmf(i, lam_away) for i in range(0, max_goals + 1)]

    # truncate correction
    sh = sum(ph)
    sa = sum(pa)
    if sh > 0:
        ph = [x / sh for x in ph]
    if sa > 0:
        pa = [x / sa for x in pa]

    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    for i in range(0, max_goals + 1):
        for j in range(0, max_goals + 1):
            p = ph[i] * pa[j]
            if i > j:
                p_home += p
            elif i == j:
                p_draw += p
            else:
                p_away += p

    s = p_home + p_draw + p_away
    if s > 0:
        p_home /= s
        p_draw /= s
        p_away /= s

    return p_home, p_draw, p_away


def _core_markets(lam_home: float, lam_away: float) -> Dict[str, float]:
    """
    Core Poisson markets for a given (lam_home, lam_away).
    All values are probabilities in [0, 1].
    """
    lam_home = max(0.0, float(lam_home))
    lam_away = max(0.0, float(lam_away))
    lam_tot = lam_home + lam_away

    # 1X2
    p_home, p_draw, p_away = _result_probs_1x2(lam_home, lam_away, max_goals=10)

    # Double chance
    p_1x = p_home + p_draw
    p_12 = p_home + p_away
    p_x2 = p_draw + p_away

    # Totals (Poisson sum)
    p_tot_over_0_5 = 1.0 - _poisson_cdf(0, lam_tot)  # >=1
    p_tot_over_1_5 = 1.0 - _poisson_cdf(1, lam_tot)  # >=2
    p_tot_over_2_5 = 1.0 - _poisson_cdf(2, lam_tot)  # >=3

    # Team totals
    p_h_over_0_5 = 1.0 - _poisson_cdf(0, lam_home)
    p_h_over_1_5 = 1.0 - _poisson_cdf(1, lam_home)
    p_a_over_0_5 = 1.0 - _poisson_cdf(0, lam_away)
    p_a_over_1_5 = 1.0 - _poisson_cdf(1, lam_away)

    # BTTS
    p_h0 = _poisson_pmf(0, lam_home)
    p_a0 = _poisson_pmf(0, lam_away)
    p_btts_yes = 1.0 - p_h0 - p_a0 + (p_h0 * p_a0)
    p_btts_no = 1.0 - p_btts_yes

    # Clean sheets (concede 0)
    p_home_cs = p_a0  # away scores 0
    p_away_cs = p_h0  # home scores 0

    return {
        "home_win": p_home,
        "draw": p_draw,
        "away_win": p_away,
        "home_or_draw": p_1x,
        "home_or_away": p_12,
        "draw_or_away": p_x2,
        "total_over_0_5": p_tot_over_0_5,
        "total_over_1_5": p_tot_over_1_5,
        "total_over_2_5": p_tot_over_2_5,
        "home_team_over_0_5": p_h_over_0_5,
        "home_team_over_1_5": p_h_over_1_5,
        "away_team_over_0_5": p_a_over_0_5,
        "away_team_over_1_5": p_a_over_1_5,
        "btts_yes": p_btts_yes,
        "btts_no": p_btts_no,
        "home_clean_sheet": p_home_cs,
        "away_clean_sheet": p_away_cs,
    }


def _goals_by_time_ratio(total_goals_by_time: Optional[List[float]]) -> Tuple[float, float, float, float]:
    """
    Returns:
      ratio_1h, ratio_2h, ratio_0_15, ratio_80_90
    total_goals_by_time is expected to be length 10 (index 0 => 0-15, index 9 => 80-90+).
    If missing/invalid, falls back to reasonable defaults.
    """
    ratio_1h = 0.45
    ratio_2h = 0.55
    ratio_0_15 = 15.0 / 90.0
    ratio_80_90 = 10.0 / 90.0

    if isinstance(total_goals_by_time, list) and len(total_goals_by_time) >= 10:
        vals = []
        for x in total_goals_by_time[:10]:
            xf = _to_float(x)
            vals.append(max(0.0, xf) if xf is not None else 0.0)
        s = sum(vals)
        if s > 0.0:
            s1 = sum(vals[0:5])
            s2 = sum(vals[5:10])
            ratio_1h = s1 / s
            ratio_2h = s2 / s
            ratio_0_15 = vals[0] / s
            ratio_80_90 = vals[9] / s

    ratio_1h = _clamp01(ratio_1h)
    ratio_2h = _clamp01(ratio_2h)
    if ratio_1h + ratio_2h > 0:
        s = ratio_1h + ratio_2h
        ratio_1h /= s
        ratio_2h /= s

    ratio_0_15 = _clamp01(ratio_0_15)
    ratio_80_90 = _clamp01(ratio_80_90)
    return ratio_1h, ratio_2h, ratio_0_15, ratio_80_90


def compute_ai_predictions_from_lambdas(
    lam_home: float,
    lam_away: float,
    total_goals_by_time: Optional[List[float]] = None,
) -> Dict[str, int]:
    """
    Lambda 기반(순수 Poisson) AI Predictions.
    - history blend 없음
    - goals_by_time 가 있으면 1H/2H ratio 및 0-15 / 80-90+ ratio 계산에 반영
    """
    lam_home = max(0.0, float(lam_home))
    lam_away = max(0.0, float(lam_away))
    lam_tot = lam_home + lam_away

    ratio_1h, ratio_2h, ratio_0_15, ratio_80_90 = _goals_by_time_ratio(total_goals_by_time)

    # FT markets
    ft = _core_markets(lam_home, lam_away)

    # 1H / 2H markets (goals only inside each half)
    lam_home_1h = lam_home * ratio_1h
    lam_away_1h = lam_away * ratio_1h
    lam_home_2h = lam_home * ratio_2h
    lam_away_2h = lam_away * ratio_2h

    h1 = _core_markets(lam_home_1h, lam_away_1h)
    h2 = _core_markets(lam_home_2h, lam_away_2h)

    # First goal (unconditional; 0-0 case excluded automatically)
    if lam_tot <= 0.0:
        p_first_home = 0.0
        p_first_away = 0.0
    else:
        p_any_goal = 1.0 - math.exp(-lam_tot)
        share_home = lam_home / lam_tot
        share_away = lam_away / lam_tot
        p_first_home = p_any_goal * share_home
        p_first_away = p_any_goal * share_away

    # Goal in minute buckets (at least one goal occurs in the bucket)
    p_goal_0_15 = 1.0 - math.exp(-lam_tot * ratio_0_15)
    p_goal_80_90 = 1.0 - math.exp(-lam_tot * ratio_80_90)

    out: Dict[str, int] = {}

    # FT
    out["home_win_pct"] = _pct(ft["home_win"])
    out["draw_pct"] = _pct(ft["draw"])
    out["away_win_pct"] = _pct(ft["away_win"])

    out["home_or_draw_pct"] = _pct(ft["home_or_draw"])
    out["home_or_away_pct"] = _pct(ft["home_or_away"])
    out["draw_or_away_pct"] = _pct(ft["draw_or_away"])

    out["total_over_0_5_pct"] = _pct(ft["total_over_0_5"])
    out["total_over_1_5_pct"] = _pct(ft["total_over_1_5"])
    out["total_over_2_5_pct"] = _pct(ft["total_over_2_5"])

    out["home_team_over_0_5_pct"] = _pct(ft["home_team_over_0_5"])
    out["home_team_over_1_5_pct"] = _pct(ft["home_team_over_1_5"])
    out["away_team_over_0_5_pct"] = _pct(ft["away_team_over_0_5"])
    out["away_team_over_1_5_pct"] = _pct(ft["away_team_over_1_5"])

    out["btts_yes_pct"] = _pct(ft["btts_yes"])
    out["btts_no_pct"] = _pct(ft["btts_no"])
    out["home_clean_sheet_pct"] = _pct(ft["home_clean_sheet"])
    out["away_clean_sheet_pct"] = _pct(ft["away_clean_sheet"])

    # 1H
    out["h1_home_win_pct"] = _pct(h1["home_win"])
    out["h1_draw_pct"] = _pct(h1["draw"])
    out["h1_away_win_pct"] = _pct(h1["away_win"])

    out["h1_home_or_draw_pct"] = _pct(h1["home_or_draw"])
    out["h1_home_or_away_pct"] = _pct(h1["home_or_away"])
    out["h1_draw_or_away_pct"] = _pct(h1["draw_or_away"])

    out["h1_total_over_0_5_pct"] = _pct(h1["total_over_0_5"])
    out["h1_total_over_1_5_pct"] = _pct(h1["total_over_1_5"])

    out["h1_home_team_over_0_5_pct"] = _pct(h1["home_team_over_0_5"])
    out["h1_home_team_over_1_5_pct"] = _pct(h1["home_team_over_1_5"])
    out["h1_away_team_over_0_5_pct"] = _pct(h1["away_team_over_0_5"])
    out["h1_away_team_over_1_5_pct"] = _pct(h1["away_team_over_1_5"])

    out["h1_btts_yes_pct"] = _pct(h1["btts_yes"])
    out["h1_btts_no_pct"] = _pct(h1["btts_no"])
    out["h1_home_clean_sheet_pct"] = _pct(h1["home_clean_sheet"])
    out["h1_away_clean_sheet_pct"] = _pct(h1["away_clean_sheet"])

    # 2H
    out["h2_home_win_pct"] = _pct(h2["home_win"])
    out["h2_draw_pct"] = _pct(h2["draw"])
    out["h2_away_win_pct"] = _pct(h2["away_win"])

    out["h2_home_or_draw_pct"] = _pct(h2["home_or_draw"])
    out["h2_home_or_away_pct"] = _pct(h2["home_or_away"])
    out["h2_draw_or_away_pct"] = _pct(h2["draw_or_away"])

    out["h2_total_over_0_5_pct"] = _pct(h2["total_over_0_5"])
    out["h2_total_over_1_5_pct"] = _pct(h2["total_over_1_5"])

    out["h2_home_team_over_0_5_pct"] = _pct(h2["home_team_over_0_5"])
    out["h2_home_team_over_1_5_pct"] = _pct(h2["home_team_over_1_5"])
    out["h2_away_team_over_0_5_pct"] = _pct(h2["away_team_over_0_5"])
    out["h2_away_team_over_1_5_pct"] = _pct(h2["away_team_over_1_5"])

    out["h2_btts_yes_pct"] = _pct(h2["btts_yes"])
    out["h2_btts_no_pct"] = _pct(h2["btts_no"])
    out["h2_home_clean_sheet_pct"] = _pct(h2["home_clean_sheet"])
    out["h2_away_clean_sheet_pct"] = _pct(h2["away_clean_sheet"])

    # Specials
    out["first_goal_home_pct"] = _pct(p_first_home)
    out["first_goal_away_pct"] = _pct(p_first_away)
    out["goal_0_15_pct"] = _pct(p_goal_0_15)
    out["goal_80_90_pct"] = _pct(p_goal_80_90)

    return out


def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Dict[str, int]:
    """
    insights_overall(홈/원정 팀 최근 성적 요약)로부터 lam_home / lam_away 구성 후 확률 계산.
    """
    if not isinstance(insights_overall, dict):
        return compute_ai_predictions_from_lambdas(0.0, 0.0, None)

    home = insights_overall.get("home") or {}
    away = insights_overall.get("away") or {}
    league = insights_overall.get("league") or {}

    # 공격/수비 강도 (없으면 1.0)
    h_attack = _to_float(home.get("attack_strength")) or 1.0
    h_def = _to_float(home.get("defense_strength")) or 1.0
    a_attack = _to_float(away.get("attack_strength")) or 1.0
    a_def = _to_float(away.get("defense_strength")) or 1.0

    # 리그 평균 득점 (없으면 기본값)
    league_home_gf = _to_float(league.get("avg_home_goals_for")) or 1.35
    league_away_gf = _to_float(league.get("avg_away_goals_for")) or 1.10

    # expected goals (lambdas)
    lam_home = max(0.0, league_home_gf * h_attack * a_def)
    lam_away = max(0.0, league_away_gf * a_attack * h_def)

    # goals_by_time 기반 ratio
    h_gbt_for = home.get("goals_by_time_for") or []
    h_gbt_against = home.get("goals_by_time_against") or []
    a_gbt_for = away.get("goals_by_time_for") or []
    a_gbt_against = away.get("goals_by_time_against") or []

    total_gbt: Optional[List[float]] = None
    try:
        total_gbt = []
        for i in range(10):
            total_gbt.append(
                float(h_gbt_for[i] or 0)
                + float(h_gbt_against[i] or 0)
                + float(a_gbt_for[i] or 0)
                + float(a_gbt_against[i] or 0)
            )
    except Exception:
        total_gbt = None

    return compute_ai_predictions_from_lambdas(lam_home, lam_away, total_gbt)
