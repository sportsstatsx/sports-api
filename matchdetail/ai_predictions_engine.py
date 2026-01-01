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
    return max(0, min(100, int(round(x))))


def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    try:
        return math.exp(-lam) * lam**k / math.factorial(k)
    except OverflowError:
        return 0.0


def _joint_score_probs(lam_home: float, lam_away: float, max_goals: int = 7) -> Dict[Tuple[int, int], float]:
    """
    (home_goals, away_goals) → 확률 테이블 생성.
    0~max_goals 까지만 계산하고 마지막에 정규화.
    """
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
    """a, b 두 개 값을 가중치로 블렌딩. 둘 중 하나만 있어도 알아서 처리."""
    vals = []
    weights = []
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


def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    match_detail_bundle 내의 insights_overall 블록을 기반으로
    AI Predictions 결과 딕셔너리를 생성한다.
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
        # 득점 평균 자체가 없으면 예측 불가
        return None

    # “리그 평균 실점” 대략 추정 (양 팀 recent 평균의 평균)
    league_ga_samples = [v for v in [h_def_recent, a_def_recent] if v is not None]
    league_avg_ga = sum(league_ga_samples) / len(league_ga_samples) if league_ga_samples else 1.2

    if h_def is None:
        h_def = league_avg_ga
    if a_def is None:
        a_def = league_avg_ga

    # ───── 2) 포아송 λ 계산 ─────
    # 공격력 * (상대 수비 / 리그 평균) 구조
    lam_home = max(0.1, h_attack * (a_def / league_avg_ga))
    lam_away = max(0.1, a_attack * (h_def / league_avg_ga))

    score_probs = _joint_score_probs(lam_home, lam_away, max_goals=7)

    # ───── 3) 1X2 확률 + 히스토리 보정 ─────
    p_home = sum(p for (h, a), p in score_probs.items() if h > a)
    p_draw = sum(p for (h, a), p in score_probs.items() if h == a)
    p_away = 1.0 - p_home - p_draw

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

    p_home_final = _blend_prob(p_home, p_home_hist)
    p_draw_final = _blend_prob(p_draw, p_draw_hist)
    p_away_final = max(0.0, 1.0 - p_home_final - p_draw_final)

    # Double chance
    p_1x = p_home_final + p_draw_final
    p_12 = p_home_final + p_away_final
    p_x2 = p_draw_final + p_away_final

    # ───── 4) Totals (1.5, 2.5, 3.5) & 팀별 득점 ─────
    def _prob_over(k: int) -> float:
        # 총 득점 > k
        return sum(p for (h, a), p in score_probs.items() if h + a > k)

    p_over15 = _prob_over(1)
    p_over25 = _prob_over(2)
    p_over35 = _prob_over(3)

    def _prob_team_over(is_home: bool, k: int) -> float:
        if is_home:
            return sum(p for (h, a), p in score_probs.items() if h > k)
        else:
            return sum(p for (h, a), p in score_probs.items() if a > k)

    p_home_over05 = _prob_team_over(True, 0)
    p_home_over15 = _prob_team_over(True, 1)
    p_away_over05 = _prob_team_over(False, 0)
    p_away_over15 = _prob_team_over(False, 1)

    # ───── 5) BTTS / 클린시트 / 무득점 ─────
    p_btts = sum(p for (h, a), p in score_probs.items() if h >= 1 and a >= 1)
    p_no_btts = 1.0 - p_btts

    p_home_cs = sum(p for (h, a), p in score_probs.items() if a == 0)
    p_away_cs = sum(p for (h, a), p in score_probs.items() if h == 0)
    p_home_no_goal = sum(p for (h, a), p in score_probs.items() if h == 0)
    p_away_no_goal = sum(p for (h, a), p in score_probs.items() if a == 0)

    # ───── 6) Expected Goals & Scoreline ─────
    xg_home = sum(h * p for (h, a), p in score_probs.items())
    xg_away = sum(a * p for (h, a), p in score_probs.items())

    sorted_scores = sorted(score_probs.items(), key=lambda kv: kv[1], reverse=True)
    most_likely_score = None
    top3_scores: List[Dict[str, Any]] = []
    for idx, ((h, a), p) in enumerate(sorted_scores[:3]):
        if idx == 0:
            most_likely_score = {"home_goals": h, "away_goals": a, "pct": _safe_pct(p * 100)}
        top3_scores.append({"home_goals": h, "away_goals": a, "pct": _safe_pct(p * 100)})

    # ───── 7) 타이밍 관련 확률 (GoalsByTime 기반 러프 추정) ─────
    gbt_home_for = home.get("goals_by_time_for") or []
    gbt_home_against = home.get("goals_by_time_against") or []
    gbt_away_for = away.get("goals_by_time_for") or []
    gbt_away_against = away.get("goals_by_time_against") or []

    def _sum_idxs(lst, idxs):
        return float(sum(lst[i] for i in idxs if i < len(lst))) if lst else 0.0

    # 10칸: 0–10,10–20,20–30,30–40,40–45,45–50,50–60,60–70,70–80,80–90
    # 전반 = 0~4, 후반 = 5~9
    h_for_1h = _sum_idxs(gbt_home_for, range(0, 5))
    h_against_1h = _sum_idxs(gbt_home_against, range(0, 5))
    a_for_1h = _sum_idxs(gbt_away_for, range(0, 5))
    a_against_1h = _sum_idxs(gbt_away_against, range(0, 5))
    total_1h = h_for_1h + h_against_1h + a_for_1h + a_against_1h

    h_for_2h = _sum_idxs(gbt_home_for, range(5, 10))
    h_against_2h = _sum_idxs(gbt_home_against, range(5, 10))
    a_for_2h = _sum_idxs(gbt_away_for, range(5, 10))
    a_against_2h = _sum_idxs(gbt_away_against, range(5, 10))
    total_2h = h_for_2h + h_against_2h + a_for_2h + a_against_2h

    total_goals_by_time = total_1h + total_2h
    if total_goals_by_time > 0:
        ratio_1h = total_1h / total_goals_by_time
    else:
        ratio_1h = 0.45  # 데이터 없으면 전반 45%, 후반 55% 정도로 가정
    ratio_2h = 1.0 - ratio_1h

    lam_total = lam_home + lam_away
    lam_1h = lam_total * ratio_1h
    lam_2h = lam_total * ratio_2h

    # 각 하프에서 한 골 이상 날 확률: 1 - P(0골)
    p_goal_1h = 1.0 - math.exp(-lam_1h)
    p_goal_2h = 1.0 - math.exp(-lam_2h)
    # 둘 다 골: 독립 근사
    p_both_halves = p_goal_1h * p_goal_2h
    # 후반 더 많은 골: 기대값 비율로 근사
    p_second_half_more = lam_2h / (lam_1h + lam_2h) if (lam_1h + lam_2h) > 0 else 0.5

    # 0–15, 80–90+ 는 bucket 0, 9 비중으로 λ 쪼개기
    total_goals_all = total_goals_by_time
    b0 = (
        _sum_idxs(gbt_home_for, [0])
        + _sum_idxs(gbt_home_against, [0])
        + _sum_idxs(gbt_away_for, [0])
        + _sum_idxs(gbt_away_against, [0])
    )
    b9 = (
        _sum_idxs(gbt_home_for, [9])
        + _sum_idxs(gbt_home_against, [9])
        + _sum_idxs(gbt_away_for, [9])
        + _sum_idxs(gbt_away_against, [9])
    )

    if total_goals_all > 0:
        ratio_0_15 = b0 / total_goals_all
        ratio_80_90 = b9 / total_goals_all
    else:
        ratio_0_15 = 0.12
        ratio_80_90 = 0.10

    lam_0_15 = lam_total * ratio_0_15
    lam_80_90 = lam_total * ratio_80_90
    p_goal_0_15 = 1.0 - math.exp(-lam_0_15)
    p_goal_80_90 = 1.0 - math.exp(-lam_80_90)

    # ───── 8) 퍼스트골 & 리드/트레일 모멘텀 ─────
    first_home_hist = _get_triple(home.get("first_to_score_pct"))
    first_away_hist = _get_triple(away.get("first_to_score_pct"))
    if first_home_hist is not None and first_away_hist is not None:
        p_first_home = (first_home_hist / 100.0 + (100.0 - first_away_hist) / 100.0) / 2.0
    else:
        # 데이터 없으면 λ 비율로 근사
        p_first_home = lam_home / (lam_home + lam_away) if (lam_home + lam_away) > 0 else 0.5
    p_first_away = 1.0 - p_first_home

    when_leading_win_home = _get_triple(home.get("when_leading_win_pct"))
    when_trailing_win_home = _get_triple(home.get("when_trailing_win_pct"))

    # ───── 9) 카드/코너 (간단 지수) ─────
    yellow_home = home.get("yellow_per_match")
    yellow_away = away.get("yellow_per_match")
    red_home = home.get("red_per_match")
    red_away = away.get("red_per_match")
    corners_home = home.get("corners_per_match")
    corners_away = away.get("corners_per_match")

    def _to_f(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    y_h = _to_f(yellow_home)
    y_a = _to_f(yellow_away)
    r_h = _to_f(red_home)
    r_a = _to_f(red_away)
    c_h = _to_f(corners_home)
    c_a = _to_f(corners_away)

    avg_cards = 0.0
    cnt = 0
    for v in [y_h, y_a]:
        if v is not None:
            avg_cards += v
            cnt += 1
    # 레드는 2장으로 환산
    for v in [r_h, r_a]:
        if v is not None:
            avg_cards += 2.0 * v
            cnt += 2
    avg_cards = avg_cards / cnt if cnt > 0 else 3.5

    # 아주 러프하게: 3~7.5 범위로 스케일해서 30~100%로 맵핑
    high_cards_index = (avg_cards - 3.0) / (7.5 - 3.0)
    p_high_cards = max(0.0, min(1.0, 0.3 + 0.7 * high_cards_index))

    avg_corners = 0.0
    cntc = 0
    for v in [c_h, c_a]:
        if v is not None:
            avg_corners += v
            cntc += 1
    avg_corners = avg_corners / cntc if cntc > 0 else 8.5
    # 코너도 대충 7~12 범위로 40~100% 맵핑
    high_corners_index = (avg_corners - 7.0) / (12.0 - 7.0)
    p_high_corners = max(0.0, min(1.0, 0.4 + 0.6 * high_corners_index))

    # ───── 10) 최종 딕셔너리 반환 ─────
    return {
        # 1X2 + 더블찬스
        "home_win_pct": _safe_pct(p_home_final * 100),
        "draw_pct": _safe_pct(p_draw_final * 100),
        "away_win_pct": _safe_pct(p_away_final * 100),
        "home_or_draw_pct": _safe_pct(p_1x * 100),
        "home_or_away_pct": _safe_pct(p_12 * 100),
        "draw_or_away_pct": _safe_pct(p_x2 * 100),

        # Totals & 팀별 득점
        "over15_pct": _safe_pct(p_over15 * 100),
        "over25_pct": _safe_pct(p_over25 * 100),
        "over35_pct": _safe_pct(p_over35 * 100),

        "home_team_over05_pct": _safe_pct(p_home_over05 * 100),
        "home_team_over15_pct": _safe_pct(p_home_over15 * 100),
        "away_team_over05_pct": _safe_pct(p_away_over05 * 100),
        "away_team_over15_pct": _safe_pct(p_away_over15 * 100),

        # BTTS / 클린시트 / 무득점
        "btts_yes_pct": _safe_pct(p_btts * 100),
        "btts_no_pct": _safe_pct(p_no_btts * 100),

        "home_clean_sheet_pct": _safe_pct(p_home_cs * 100),
        "away_clean_sheet_pct": _safe_pct(p_away_cs * 100),
        "home_no_goals_pct": _safe_pct(p_home_no_goal * 100),
        "away_no_goals_pct": _safe_pct(p_away_no_goal * 100),

        # Expected Goals & Scorelines
        "expected_goals_home": round(xg_home, 2),
        "expected_goals_away": round(xg_away, 2),

        "most_likely_score": most_likely_score,
        "top3_scorelines": top3_scores,

        # 하프/타임 기반
        "goal_1st_half_pct": _safe_pct(p_goal_1h * 100),
        "goal_2nd_half_pct": _safe_pct(p_goal_2h * 100),
        "both_halves_goal_pct": _safe_pct(p_both_halves * 100),
        "second_half_more_goals_pct": _safe_pct(p_second_half_more * 100),

        "goal_0_15_pct": _safe_pct(p_goal_0_15 * 100),
        "goal_80_90_pct": _safe_pct(p_goal_80_90 * 100),

        # 퍼스트골 & 리드/트레일
        "first_goal_home_pct": _safe_pct(p_first_home * 100),
        "first_goal_away_pct": _safe_pct(p_first_away * 100),

        "if_home_scores_first_win_pct": _safe_pct(when_leading_win_home or 0.0),
        "if_away_scores_first_home_win_pct": _safe_pct(when_trailing_win_home or 0.0),

        # 카드/코너
        "high_cards_game_pct": _safe_pct(p_high_cards * 100),
        "high_corners_game_pct": _safe_pct(p_high_corners * 100),
    }
