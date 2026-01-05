# ai_predictions_engine.py
from __future__ import annotations

from dataclasses import dataclass
from math import exp, factorial
from typing import Any, Dict, List, Tuple


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

def _clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _round_pct(p01: float) -> int:
    return int(round(_clamp01(p01) * 100.0))


def _poisson_pmf_list(lam: float, gmax: int) -> List[float]:
    lam = max(0.0, float(lam))
    out = []
    for k in range(gmax + 1):
        out.append(exp(-lam) * (lam ** k) / factorial(k))
    # tail correction: keep total == 1.0 by pushing remainder into last bin
    s = sum(out)
    if s > 0.0:
        out[-1] += max(0.0, 1.0 - s)
    return out


def _poisson_tail_prob(lam: float, k: int) -> float:
    """
    P(X >= k) for X~Pois(lam), k>=0.
    """
    lam = max(0.0, float(lam))
    if k <= 0:
        return 1.0
    # 1 - CDF(k-1)
    cdf = 0.0
    for i in range(k):
        cdf += exp(-lam) * (lam ** i) / factorial(i)
    return _clamp01(1.0 - cdf)


def _btts_yes(lh: float, la: float) -> float:
    lh = max(0.0, float(lh))
    la = max(0.0, float(la))
    # 1 - P(H=0) - P(A=0) + P(H=0,A=0)
    return _clamp01(1.0 - exp(-lh) - exp(-la) + exp(-(lh + la)))


def _normalize_1x2_pcts(hw: float, d: float, aw: float) -> Tuple[int, int, int]:
    """
    Convert float probs (sum≈1) to int % that sum exactly to 100.
    """
    hw = _clamp01(hw)
    d = _clamp01(d)
    aw = _clamp01(aw)
    s = hw + d + aw
    if s <= 0.0:
        return (33, 34, 33)
    hw *= 100.0 / s
    d *= 100.0 / s
    aw *= 100.0 / s

    hw_i = int(round(hw))
    d_i = int(round(d))
    aw_i = int(round(aw))

    # fix rounding drift
    drift = 100 - (hw_i + d_i + aw_i)
    if drift != 0:
        # add/subtract to the largest bucket (most stable)
        arr = [("hw", hw_i, hw), ("d", d_i, d), ("aw", aw_i, aw)]
        arr.sort(key=lambda x: x[2], reverse=True)
        name, val_i, _ = arr[0]
        val_i += drift
        if name == "hw":
            hw_i = val_i
        elif name == "d":
            d_i = val_i
        else:
            aw_i = val_i

    # final clamp
    hw_i = max(0, min(100, hw_i))
    d_i = max(0, min(100, d_i))
    aw_i = max(0, min(100, aw_i))
    # re-fix if clamp broke sum (rare)
    s2 = hw_i + d_i + aw_i
    if s2 != 100:
        hw_i = max(0, min(100, hw_i + (100 - s2)))
    return (hw_i, d_i, aw_i)


def _scorelines_top3(lh: float, la: float, gmax: int = 10) -> Tuple[str, List[str]]:
    ph = _poisson_pmf_list(lh, gmax)
    pa = _poisson_pmf_list(la, gmax)

    pairs: List[Tuple[float, int, int]] = []
    for i in range(gmax + 1):
        for j in range(gmax + 1):
            pairs.append((ph[i] * pa[j], i, j))
    pairs.sort(key=lambda x: x[0], reverse=True)

    top = pairs[:3]
    fmt = [f"{i}-{j}" for _, i, j in top]
    most = fmt[0] if fmt else "0-0"
    return most, fmt


@dataclass
class SectionInputs:
    lam_home: float
    lam_away: float


def _section_core_1x2(lam_home: float, lam_away: float, gmax: int = 10) -> Tuple[float, float, float]:
    """
    Return (P(HW), P(D), P(AW)) from independent Pois(lh),Pois(la).
    """
    ph = _poisson_pmf_list(lam_home, gmax)
    pa = _poisson_pmf_list(lam_away, gmax)

    hw = 0.0
    d = 0.0
    aw = 0.0
    for i in range(gmax + 1):
        for j in range(gmax + 1):
            p = ph[i] * pa[j]
            if i > j:
                hw += p
            elif i == j:
                d += p
            else:
                aw += p

    s = hw + d + aw
    if s > 0.0:
        hw /= s
        d /= s
        aw /= s
    return (_clamp01(hw), _clamp01(d), _clamp01(aw))



def _derive_half_lambdas(
    lam_h_ft: float,
    lam_a_ft: float,
    total_goals_by_time: List[float] | None,
    *,
    fallback_share_1h: float = 0.45,
) -> Tuple[SectionInputs, SectionInputs]:
    """
    Return (1H lambdas, 2H lambdas).
    total_goals_by_time expected length 10 with first 5 = 1H, last 5 = 2H.
    """
    share_1h = fallback_share_1h
    share_2h = 1.0 - share_1h

    if total_goals_by_time and len(total_goals_by_time) >= 10:
        s1 = sum(float(x or 0.0) for x in total_goals_by_time[:5])
        s2 = sum(float(x or 0.0) for x in total_goals_by_time[5:10])
        st = s1 + s2
        if st > 0.0:
            share_1h = _clamp01(s1 / st)
            share_2h = 1.0 - share_1h

    h1 = SectionInputs(lam_home=lam_h_ft * share_1h, lam_away=lam_a_ft * share_1h)
    h2 = SectionInputs(lam_home=lam_h_ft * share_2h, lam_away=lam_a_ft * share_2h)
    return h1, h2


def _derive_window_lambda(
    section_total_lambda: float,
    total_goals_by_time: List[float] | None,
    *,
    section: str,  # "1H" or "2H"
    fallback_ratio_in_section: float = 0.20,
) -> float:
    """
    We assume total_goals_by_time length 10:
      - 1H window 35-45+ is bucket index 4 (last of first 5)
      - 2H window 80-90+ is bucket index 9 (last overall)
    """
    if not total_goals_by_time or len(total_goals_by_time) < 10:
        return max(0.0, float(section_total_lambda)) * fallback_ratio_in_section

    vals = [float(x or 0.0) for x in total_goals_by_time[:10]]

    if section == "1H":
        denom = sum(vals[:5])
        numer = vals[4]
    else:  # "2H"
        denom = sum(vals[5:10])
        numer = vals[9]

    if denom <= 0.0:
        ratio = fallback_ratio_in_section
    else:
        ratio = _clamp01(numer / denom)

    return max(0.0, float(section_total_lambda)) * ratio

def _derive_section_probs(
    lam_home: float,
    lam_away: float,
    p_hw: float,
    p_d: float,
    p_aw: float,
) -> Dict[str, int]:
    # 1X2 -> 정수 퍼센트(합 100 보장)
    hw_i, d_i, aw_i = _normalize_1x2_pcts(p_hw, p_d, p_aw)

    # Double chance
    home_or_draw = max(0, min(100, hw_i + d_i))  # 1X
    home_or_away = max(0, min(100, hw_i + aw_i))  # 12
    draw_or_away = max(0, min(100, d_i + aw_i))  # X2

    # Totals (Poisson total)
    lam_t = max(0.0, float(lam_home)) + max(0.0, float(lam_away))
    over_0_5 = _round_pct(_poisson_tail_prob(lam_t, 1))  # >=1
    over_1_5 = _round_pct(_poisson_tail_prob(lam_t, 2))  # >=2
    over_2_5 = _round_pct(_poisson_tail_prob(lam_t, 3))  # >=3

    # Team totals
    home_over_0_5 = _round_pct(_poisson_tail_prob(lam_home, 1))
    home_over_1_5 = _round_pct(_poisson_tail_prob(lam_home, 2))
    away_over_0_5 = _round_pct(_poisson_tail_prob(lam_away, 1))
    away_over_1_5 = _round_pct(_poisson_tail_prob(lam_away, 2))

    # BTTS / Clean sheet
    btts_yes = _round_pct(_btts_yes(lam_home, lam_away))
    btts_no = max(0, min(100, 100 - btts_yes))

    home_clean_sheet = _round_pct(_clamp01(exp(-max(0.0, float(lam_away)))))  # P(A=0)
    away_clean_sheet = _round_pct(_clamp01(exp(-max(0.0, float(lam_home)))))  # P(H=0)

    return {
        "home_win": hw_i,
        "draw": d_i,
        "away_win": aw_i,

        # ✅ 기존 키(유지)
        "home_or_draw": home_or_draw,
        "home_or_away": home_or_away,
        "draw_or_away": draw_or_away,

        # ✅ 더블찬스 호환(alias) 키들 (앱이 뭘 읽든 잡히게)
        "home_or_draw_1x": home_or_draw,
        "home_or_away_12": home_or_away,
        "draw_or_away_x2": draw_or_away,
        "dc_1x": home_or_draw,
        "dc_12": home_or_away,
        "dc_x2": draw_or_away,

        "over_0_5": over_0_5,
        "over_1_5": over_1_5,
        "over_2_5": over_2_5,
        "home_over_0_5": home_over_0_5,
        "home_over_1_5": home_over_1_5,
        "away_over_0_5": away_over_0_5,
        "away_over_1_5": away_over_1_5,
        "btts_yes": btts_yes,
        "btts_no": btts_no,
        "home_clean_sheet": home_clean_sheet,
        "away_clean_sheet": away_clean_sheet,
    }



def _top_scorelines(lh: float, la: float, gmax: int = 10) -> Tuple[str, List[str]]:
    # 너 파일에 이미 있는 _scorelines_top3를 그대로 사용
    return _scorelines_top3(lh, la, gmax)



def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ 설계서 기반(현재 DB/insights 구조에서 가능한 범위)
      - μ_home/μ_away(리그 평균 홈/원정 득점) + Att/Def 비율로 FT λ 산출
      - 1H/2H 는 팀 득점 분포(goals_by_time10_for)로 FT λ를 분배
      - 35-45+, 80-90+ 는 전/후반 내 구간 share로 λ_W 계산 → P=1-e^-λ_W
      - 파생확률(1X2/DC/Over/TeamOver/BTTS/CleanSheet)은 기존 포아송 로직 그대로 사용
    필요 키:
      - insights_overall['league_avgs'] = {'mu_home': float, 'mu_away': float}
      - insights_overall['home'|'away']['avg_gf'|'avg_ga'] = {'home'|'away': float ...}
      - insights_overall['home'|'away']['goals_by_time10_for'] = [int]*10  (없으면 6버킷으로 폴백)
    """
    overall = insights_overall or {}
    home = overall.get("home") or {}
    away = overall.get("away") or {}

    def clamp(x: float, lo: float, hi: float) -> float:
        try:
            xf = float(x)
        except Exception:
            xf = 0.0
        if xf < lo:
            return lo
        if xf > hi:
            return hi
        return xf

    def safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    # ─────────────────────────────────────
    # 1) μ_home/μ_away (리그 평균 홈/원정 득점)
    # ─────────────────────────────────────
    league_avgs = overall.get("league_avgs") or {}
    mu_home = max(0.2, safe_float(league_avgs.get("mu_home"), 1.0))
    mu_away = max(0.2, safe_float(league_avgs.get("mu_away"), 1.0))

    # ─────────────────────────────────────
    # 2) Team GF/GA (홈팀=home컨텍스트, 원정팀=away컨텍스트)
    # ─────────────────────────────────────
    home_gf_home = safe_float((home.get("avg_gf") or {}).get("home"), 0.0)
    home_ga_home = safe_float((home.get("avg_ga") or {}).get("home"), 0.0)

    away_gf_away = safe_float((away.get("avg_gf") or {}).get("away"), 0.0)
    away_ga_away = safe_float((away.get("avg_ga") or {}).get("away"), 0.0)

    # ─────────────────────────────────────
    # 3) Att/Def 및 FT λ (설계서 핵심)
    # ─────────────────────────────────────
    att_home = (home_gf_home / mu_home) if mu_home > 0 else 0.0
    def_home = (home_ga_home / mu_away) if mu_away > 0 else 0.0

    att_away = (away_gf_away / mu_away) if mu_away > 0 else 0.0
    def_away = (away_ga_away / mu_home) if mu_home > 0 else 0.0

    lam_h_ft = mu_home * att_home * def_away
    lam_a_ft = mu_away * att_away * def_home

    # 안정화(clamp)
    lam_h_ft = clamp(lam_h_ft, 0.05, 4.50)
    lam_a_ft = clamp(lam_a_ft, 0.05, 4.50)

    # ─────────────────────────────────────
    # 4) 1H/2H 분배 (팀 득점 분포 기반)
    # ─────────────────────────────────────
    def _get_goals10_for(team_block: Dict[str, Any]) -> List[int]:
        arr = team_block.get("goals_by_time10_for")
        if isinstance(arr, list) and len(arr) == 10:
            out: List[int] = []
            for x in arr:
                try:
                    out.append(int(x))
                except Exception:
                    out.append(0)
            return out

        # 폴백: 기존 6버킷만 있는 경우(정밀도는 떨어짐)
        arr6 = team_block.get("goals_by_time_for")
        if isinstance(arr6, list) and len(arr6) == 6:
            # [0-15,16-30,31-45,46-60,61-75,76-90] → 대략 10버킷으로 분배
            # (30-34 / 35-45+, 76-79 / 80-90+ 를 50:50로 단순 분해)
            a0, a1, a2, a3, a4, a5 = [int(x or 0) for x in arr6]
            return [
                int(round(a0 * 0.5)), int(round(a0 * 0.5)),  # 0-9 / 10-19
                int(round(a1 * 0.5)), int(round(a1 * 0.5)),  # 20-29 / 30-34(대략)
                a2,                                          # 35-45+ (대략)
                int(round(a3 * 0.5)), int(round(a3 * 0.5)),  # 46-55 / 56-65
                a4,                                          # 66-75 (대략)
                int(round(a5 * 0.5)), int(round(a5 * 0.5)),  # 76-79 / 80-90+
            ]
        return [0] * 10

    def _sum(xs: List[int]) -> int:
        s = 0
        for v in xs:
            try:
                s += int(v)
            except Exception:
                pass
        return s

    def _share(num: float, den: float, fallback: float) -> float:
        try:
            if den <= 0:
                return fallback
            return clamp(float(num) / float(den), 0.0, 1.0)
        except Exception:
            return fallback

    home_g10 = _get_goals10_for(home)
    away_g10 = _get_goals10_for(away)

    home_total = _sum(home_g10)
    away_total = _sum(away_g10)

    # 전반 득점 비중(팀 기준). 표본 부족 시 폴백 0.45
    share_home_1h = _share(_sum(home_g10[0:5]), home_total, 0.45)
    share_away_1h = _share(_sum(away_g10[0:5]), away_total, 0.45)

    lam_h_1h = clamp(lam_h_ft * share_home_1h, 0.05, 4.50)
    lam_a_1h = clamp(lam_a_ft * share_away_1h, 0.05, 4.50)

    lam_h_2h = clamp(lam_h_ft * (1.0 - share_home_1h), 0.05, 4.50)
    lam_a_2h = clamp(lam_a_ft * (1.0 - share_away_1h), 0.05, 4.50)

    lam_t_1h = lam_h_1h + lam_a_1h
    lam_t_2h = lam_h_2h + lam_a_2h

    # ─────────────────────────────────────
    # 5) 구간골 λ_W → 확률
    # ─────────────────────────────────────
    # 35-45+ share: bin4 / (1H bins0..4)
    # 80-90+ share: bin9 / (2H bins5..9)
    home_1h_total = _sum(home_g10[0:5])
    away_1h_total = _sum(away_g10[0:5])
    home_2h_total = _sum(home_g10[5:10])
    away_2h_total = _sum(away_g10[5:10])

    share_home_35_45 = _share(home_g10[4], home_1h_total, 0.20)
    share_away_35_45 = _share(away_g10[4], away_1h_total, 0.20)

    share_home_80_90 = _share(home_g10[9], home_2h_total, 0.20)
    share_away_80_90 = _share(away_g10[9], away_2h_total, 0.20)

    lam_w_35_45 = max(0.0, lam_h_1h * share_home_35_45 + lam_a_1h * share_away_35_45)
    lam_w_80_90 = max(0.0, lam_h_2h * share_home_80_90 + lam_a_2h * share_away_80_90)

    p_35_45 = _clamp01(1.0 - exp(-lam_w_35_45))
    p_80_90 = _clamp01(1.0 - exp(-lam_w_80_90))

    # ─────────────────────────────────────
    # 6) 섹션별 확률 파생
    # ─────────────────────────────────────
    ft_hw, ft_d, ft_aw = _section_core_1x2(lam_h_ft, lam_a_ft)
    h1_hw, h1_d, h1_aw = _section_core_1x2(lam_h_1h, lam_a_1h)
    h2_hw, h2_d, h2_aw = _section_core_1x2(lam_h_2h, lam_a_2h)

    ft = _derive_section_probs(lam_h_ft, lam_a_ft, ft_hw, ft_d, ft_aw)
    h1 = _derive_section_probs(lam_h_1h, lam_a_1h, h1_hw, h1_d, h1_aw)
    h2 = _derive_section_probs(lam_h_2h, lam_a_2h, h2_hw, h2_d, h2_aw)

    # 표시용 점수라인(FT)
    most_likely, top3 = _top_scorelines(lam_h_ft, lam_a_ft)

    out: Dict[str, Any] = {
        "expected_goals_home": round(lam_h_ft, 3),
        "expected_goals_away": round(lam_a_ft, 3),
        "most_likely_score": most_likely,
        "top3_scorelines": top3,

        # FT
        "ft_home_win": ft["home_win"],
        "ft_draw": ft["draw"],
        "ft_away_win": ft["away_win"],
        "ft_home_or_draw": ft["home_or_draw"],
        "ft_home_or_away": ft["home_or_away"],
        "ft_draw_or_away": ft["draw_or_away"],

        # ✅ 더블찬스 호환(짧은 키)
        "ft_1x": ft["home_or_draw"],
        "ft_12": ft["home_or_away"],
        "ft_x2": ft["draw_or_away"],

        "ft_total_over_0_5": ft["over_0_5"],
        "ft_total_over_1_5": ft["over_1_5"],
        "ft_total_over_2_5": ft["over_2_5"],
        "ft_home_over_0_5": ft["home_over_0_5"],
        "ft_home_over_1_5": ft["home_over_1_5"],
        "ft_away_over_0_5": ft["away_over_0_5"],
        "ft_away_over_1_5": ft["away_over_1_5"],
        "ft_btts_yes": ft["btts_yes"],
        "ft_btts_no": ft["btts_no"],
        "ft_home_clean_sheet": ft["home_clean_sheet"],
        "ft_away_clean_sheet": ft["away_clean_sheet"],

        # 1H
        "1h_home_win": h1["home_win"],
        "1h_draw": h1["draw"],
        "1h_away_win": h1["away_win"],
        "1h_home_or_draw": h1["home_or_draw"],
        "1h_home_or_away": h1["home_or_away"],
        "1h_draw_or_away": h1["draw_or_away"],

        # ✅ 더블찬스 호환(짧은 키)
        "1h_1x": h1["home_or_draw"],
        "1h_12": h1["home_or_away"],
        "1h_x2": h1["draw_or_away"],

        "1h_total_over_0_5": h1["over_0_5"],
        "1h_total_over_1_5": h1["over_1_5"],
        "1h_home_over_0_5": h1["home_over_0_5"],
        "1h_home_over_1_5": h1["home_over_1_5"],
        "1h_away_over_0_5": h1["away_over_0_5"],
        "1h_away_over_1_5": h1["away_over_1_5"],
        "1h_btts_yes": h1["btts_yes"],
        "1h_btts_no": h1["btts_no"],
        "1h_home_clean_sheet": h1["home_clean_sheet"],
        "1h_away_clean_sheet": h1["away_clean_sheet"],
        "1h_goal_35_45_plus": _round_pct(p_35_45),

        # 2H
        "2h_home_win": h2["home_win"],
        "2h_draw": h2["draw"],
        "2h_away_win": h2["away_win"],
        "2h_home_or_draw": h2["home_or_draw"],
        "2h_home_or_away": h2["home_or_away"],
        "2h_draw_or_away": h2["draw_or_away"],

        # ✅ 더블찬스 호환(짧은 키)
        "2h_1x": h2["home_or_draw"],
        "2h_12": h2["home_or_away"],
        "2h_x2": h2["draw_or_away"],

        "2h_total_over_0_5": h2["over_0_5"],
        "2h_total_over_1_5": h2["over_1_5"],
        "2h_home_over_0_5": h2["home_over_0_5"],
        "2h_home_over_1_5": h2["home_over_1_5"],
        "2h_away_over_0_5": h2["away_over_0_5"],
        "2h_away_over_1_5": h2["away_over_1_5"],
        "2h_btts_yes": h2["btts_yes"],
        "2h_btts_no": h2["btts_no"],
        "2h_home_clean_sheet": h2["home_clean_sheet"],
        "2h_away_clean_sheet": h2["away_clean_sheet"],
        "2h_goal_80_90_plus": _round_pct(p_80_90),
    }
    return out

