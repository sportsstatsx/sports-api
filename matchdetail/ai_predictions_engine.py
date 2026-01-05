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


def _section_core_1x2(inp: SectionInputs, gmax: int = 10) -> Tuple[float, float, float]:
    """
    Return (P(HW), P(D), P(AW)) from independent Pois(lh),Pois(la).
    """
    ph = _poisson_pmf_list(inp.lam_home, gmax)
    pa = _poisson_pmf_list(inp.lam_away, gmax)

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


def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ 기존(구형) insights_overall(평면 구조)과
    ✅ 신규 insights_overall(home/away 중첩 구조) 모두 대응

    신규 구조에서는:
      - home.avg_gf.home / home.avg_ga.home
      - away.avg_gf.away / away.avg_ga.away
      - home.goals_by_time_for / away.goals_by_time_for
    등을 사용해서 람다(기대득점)를 만든다.
    """

    # ─────────────────────────────
    # 1) 람다(FT expected goals) 산출
    # ─────────────────────────────

    def _to_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    # (A) 구형 평면 구조 지원
    if "expected_goals_for" in insights_overall or "expected_goals_against" in insights_overall:
        lam_h_ft = _to_float(insights_overall.get("expected_goals_for") or 0.0)
        lam_a_ft = _to_float(insights_overall.get("expected_goals_against") or 0.0)

        gbt_for = insights_overall.get("goals_by_time_for") or []
        gbt_against = insights_overall.get("goals_by_time_against") or []

    # (B) 신규 home/away 중첩 구조
    else:
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}

        # 공격/수비(홈경기/원정경기 기준) 값 추출
        home_avg_gf = (home.get("avg_gf") or {})
        home_avg_ga = (home.get("avg_ga") or {})
        away_avg_gf = (away.get("avg_gf") or {})
        away_avg_ga = (away.get("avg_ga") or {})

        home_attack_home = _to_float(home_avg_gf.get("home") or 0.0)
        home_defense_home = _to_float(home_avg_ga.get("home") or 0.0)

        away_attack_away = _to_float(away_avg_gf.get("away") or 0.0)
        away_defense_away = _to_float(away_avg_ga.get("away") or 0.0)

        # 매치업 람다(간단하고 안정적인 방식)
        # 홈 득점 기대 = (홈 공격력 + 원정 수비력) / 2
        # 원정 득점 기대 = (원정 공격력 + 홈 수비력) / 2
        # 값이 하나만 있으면 있는 값만 사용 (0으로 떨어지는 걸 최대한 방지)
        if home_attack_home > 0.0 and away_defense_away > 0.0:
            lam_h_ft = (home_attack_home + away_defense_away) / 2.0
        else:
            lam_h_ft = max(home_attack_home, away_defense_away, 0.0)

        if away_attack_away > 0.0 and home_defense_home > 0.0:
            lam_a_ft = (away_attack_away + home_defense_home) / 2.0
        else:
            lam_a_ft = max(away_attack_away, home_defense_home, 0.0)

        # goals_by_time은 현재 구조상 팀 하위에 있음
        gbt_for = home.get("goals_by_time_for") or []
        # 상대팀 득점 분포를 합치고 싶으면 away.goals_by_time_for도 참고 가능
        # 여기서는 엔진 내부에서 len<10이면 폴백을 쓰므로 안전하게 최소만 연결
        gbt_against = away.get("goals_by_time_for") or []

    # ─────────────────────────────
    # 2) total_goals_by_time 구성 (있으면 사용, 없으면 폴백)
    # ─────────────────────────────
    total_goals_by_time: List[float] | None = None
    if isinstance(gbt_for, list) and isinstance(gbt_against, list) and len(gbt_for) >= 10 and len(gbt_against) >= 10:
        total_goals_by_time = []
        for i in range(10):
            try:
                total_goals_by_time.append(_to_float(gbt_for[i] or 0.0) + _to_float(gbt_against[i] or 0.0))
            except Exception:
                total_goals_by_time.append(0.0)

    ft = SectionInputs(lam_home=lam_h_ft, lam_away=lam_a_ft)
    h1, h2 = _derive_half_lambdas(lam_h_ft, lam_a_ft, total_goals_by_time)

    lam_t1 = h1.lam_home + h1.lam_away
    lam_t2 = h2.lam_home + h2.lam_away

    lam_w35_45 = _derive_window_lambda(lam_t1, total_goals_by_time, section="1H", fallback_ratio_in_section=0.20)
    lam_w80_90 = _derive_window_lambda(lam_t2, total_goals_by_time, section="2H", fallback_ratio_in_section=0.20)

    out: Dict[str, Any] = {}

    out["expected_goals_home"] = round(lam_h_ft, 2)
    out["expected_goals_away"] = round(lam_a_ft, 2)
    most, top3 = _scorelines_top3(lam_h_ft, lam_a_ft, gmax=10)
    out["most_likely_score"] = most
    out["top3_scorelines"] = top3

    # ── FT
    hw, d, aw = _section_core_1x2(ft, gmax=10)
    hw_i, d_i, aw_i = _normalize_1x2_pcts(hw, d, aw)
    out["ft_home_win"] = hw_i
    out["ft_draw"] = d_i
    out["ft_away_win"] = aw_i
    out["ft_1x"] = max(0, min(100, hw_i + d_i))
    out["ft_12"] = max(0, min(100, hw_i + aw_i))
    out["ft_x2"] = max(0, min(100, d_i + aw_i))

    lam_t_ft = lam_h_ft + lam_a_ft
    out["ft_total_over_0_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 1))
    out["ft_total_over_1_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 2))
    out["ft_total_over_2_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 3))

    out["ft_btts_yes"] = _round_pct(_btts_yes(lam_h_ft, lam_a_ft))

    out["ft_home_over_0_5"] = _round_pct(_poisson_tail_prob(lam_h_ft, 1))
    out["ft_home_over_1_5"] = _round_pct(_poisson_tail_prob(lam_h_ft, 2))
    out["ft_away_over_0_5"] = _round_pct(_poisson_tail_prob(lam_a_ft, 1))
    out["ft_away_over_1_5"] = _round_pct(_poisson_tail_prob(lam_a_ft, 2))

    # (이 아래 기존 코드: 1H/2H 및 window 확률 계산 부분은 그대로 유지)
    # out["1h_*"], out["2h_*"] 채우는 부분이 이어져야 함

    # ⚠️ 주의: 이 함수 아래쪽(1H/2H 계산 로직)이 원본 파일에 이미 존재하므로
    # 여기서 return out 하지 말고 원본대로 이어서 out에 계속 채워야 함.


    out: Dict[str, Any] = {}

    out["expected_goals_home"] = round(lam_h_ft, 2)
    out["expected_goals_away"] = round(lam_a_ft, 2)
    most, top3 = _scorelines_top3(lam_h_ft, lam_a_ft, gmax=10)
    out["most_likely_score"] = most
    out["top3_scorelines"] = top3

    # ── FT
    hw, d, aw = _section_core_1x2(ft, gmax=10)
    hw_i, d_i, aw_i = _normalize_1x2_pcts(hw, d, aw)
    out["ft_home_win"] = hw_i
    out["ft_draw"] = d_i
    out["ft_away_win"] = aw_i
    out["ft_1x"] = max(0, min(100, hw_i + d_i))
    out["ft_12"] = max(0, min(100, hw_i + aw_i))
    out["ft_x2"] = max(0, min(100, d_i + aw_i))

    lam_t_ft = lam_h_ft + lam_a_ft
    out["ft_total_over_0_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 1))
    out["ft_total_over_1_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 2))
    out["ft_total_over_2_5"] = _round_pct(_poisson_tail_prob(lam_t_ft, 3))

    out["ft_btts_yes"] = _round_pct(_btts_yes(lam_h_ft, lam_a_ft))

    out["ft_home_over_0_5"] = _round_pct(_poisson_tail_prob(lam_h_ft, 1))
    out["ft_home_over_1_5"] = _round_pct(_poisson_tail_prob(lam_h_ft, 2))
    out["ft_away_over_0_5"] = _round_pct(_poisson_tail_prob(lam_a_ft, 1))
    out["ft_away_over_1_5"] = _round_pct(_poisson_tail_prob(lam_a_ft, 2))

    # ── 1H
    hw, d, aw = _section_core_1x2(h1, gmax=10)
    hw_i, d_i, aw_i = _normalize_1x2_pcts(hw, d, aw)
    out["1h_home_win"] = hw_i
    out["1h_draw"] = d_i
    out["1h_away_win"] = aw_i
    out["1h_1x"] = max(0, min(100, hw_i + d_i))
    out["1h_12"] = max(0, min(100, hw_i + aw_i))
    out["1h_x2"] = max(0, min(100, d_i + aw_i))

    out["1h_total_over_0_5"] = _round_pct(_poisson_tail_prob(lam_t1, 1))
    out["1h_total_over_1_5"] = _round_pct(_poisson_tail_prob(lam_t1, 2))
    out["1h_btts_yes"] = _round_pct(_btts_yes(h1.lam_home, h1.lam_away))

    out["1h_home_over_0_5"] = _round_pct(_poisson_tail_prob(h1.lam_home, 1))
    out["1h_home_over_1_5"] = _round_pct(_poisson_tail_prob(h1.lam_home, 2))
    out["1h_away_over_0_5"] = _round_pct(_poisson_tail_prob(h1.lam_away, 1))
    out["1h_away_over_1_5"] = _round_pct(_poisson_tail_prob(h1.lam_away, 2))

    out["1h_goal_35_45_plus"] = _round_pct(1.0 - exp(-max(0.0, lam_w35_45)))

    # ── 2H
    hw, d, aw = _section_core_1x2(h2, gmax=10)
    hw_i, d_i, aw_i = _normalize_1x2_pcts(hw, d, aw)
    out["2h_home_win"] = hw_i
    out["2h_draw"] = d_i
    out["2h_away_win"] = aw_i
    out["2h_1x"] = max(0, min(100, hw_i + d_i))
    out["2h_12"] = max(0, min(100, hw_i + aw_i))
    out["2h_x2"] = max(0, min(100, d_i + aw_i))

    out["2h_total_over_0_5"] = _round_pct(_poisson_tail_prob(lam_t2, 1))
    out["2h_total_over_1_5"] = _round_pct(_poisson_tail_prob(lam_t2, 2))
    out["2h_btts_yes"] = _round_pct(_btts_yes(h2.lam_home, h2.lam_away))

    out["2h_home_over_0_5"] = _round_pct(_poisson_tail_prob(h2.lam_home, 1))
    out["2h_home_over_1_5"] = _round_pct(_poisson_tail_prob(h2.lam_home, 2))
    out["2h_away_over_0_5"] = _round_pct(_poisson_tail_prob(h2.lam_away, 1))
    out["2h_away_over_1_5"] = _round_pct(_poisson_tail_prob(h2.lam_away, 2))

    out["2h_goal_80_90_plus"] = _round_pct(1.0 - exp(-max(0.0, lam_w80_90)))

    return out
