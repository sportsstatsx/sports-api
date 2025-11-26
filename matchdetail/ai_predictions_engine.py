# matchdetail/ai_predictions_engine.py

from typing import Dict, Any, Tuple, List
import math


# ----------------------------
# 1) Poisson PMF
# ----------------------------
def poisson_pmf(k: int, lam: float) -> float:
    """Poisson Probability Mass Function."""
    try:
        return (lam ** k) * math.exp(-lam) / math.factorial(k)
    except OverflowError:
        return 0.0


# ----------------------------
# 2) Expected Goals λ 추정
# ----------------------------
def estimate_lambda(team: Dict[str, Any]) -> float:
    """
    insights_overall의 goals_by_time 값을 기반으로 λ(총 득점 평균)를 추정한다.
    """
    gb = team.get("goals_by_time_for") or []
    sample = team.get("events_sample") or 0
    if sample <= 0:
        return 0.8  # 기본값 (샘플 부족)
    total_goals = sum(gb)
    return max(total_goals / sample, 0.1)


# ----------------------------
# 3) 전체 경기 스코어 확률 테이블 만들기
# ----------------------------
def build_score_matrix(lam_home: float, lam_away: float, max_goals: int = 6):
    """
    0~max_goals 범위의 Poisson 스코어 확률 테이블을 반환한다.
    """
    matrix = []
    for h in range(max_goals + 1):
        row = []
        for a in range(max_goals + 1):
            row.append(poisson_pmf(h, lam_home) * poisson_pmf(a, lam_away))
        matrix.append(row)
    return matrix


# ----------------------------
# 4) 스코어 기반 확률 계산
# ----------------------------
def compute_match_probabilities(matrix: List[List[float]]) -> Dict[str, float]:
    home_win = 0
    draw = 0
    away_win = 0
    btts_yes = 0
    over15 = 0
    over25 = 0
    over35 = 0
    cs_home = 0
    cs_away = 0
    no_goal_home = 0
    no_goal_away = 0

    max_len = len(matrix)

    for h in range(max_len):
        for a in range(max_len):
            p = matrix[h][a]

            # Match outcome
            if h > a:
                home_win += p
            elif h == a:
                draw += p
            else:
                away_win += p

            # BTTS
            if h > 0 and a > 0:
                btts_yes += p

            # Overs
            if h + a >= 2:
                over15 += p
            if h + a >= 3:
                over25 += p
            if h + a >= 4:
                over35 += p

            # Clean sheets
            if a == 0:
                cs_home += p
            if h == 0:
                cs_away += p

            # No goals
            if h == 0:
                no_goal_home += p
            if a == 0:
                no_goal_away += p

    return {
        "home_win": home_win,
        "draw": draw,
        "away_win": away_win,
        "btts_yes": btts_yes,
        "btts_no": 1 - btts_yes,
        "over15": over15,
        "over25": over25,
        "over35": over35,
        "home_clean_sheet": cs_home,
        "away_clean_sheet": cs_away,
        "home_no_goals": no_goal_home,
        "away_no_goals": no_goal_away,
    }


# ----------------------------
# 5) Most likely scoreline
# ----------------------------
def compute_top_scorelines(matrix: List[List[float]], top_n: int = 3):
    scores = []
    for h in range(len(matrix)):
        for a in range(len(matrix)):
            scores.append(((h, a), matrix[h][a]))

    scores.sort(key=lambda x: x[1], reverse=True)
    result = []
    for i in range(min(top_n, len(scores))):
        (h, a), p = scores[i]
        result.append({
            "score": f"{h}-{a}",
            "prob": round(p * 100, 2)
        })
    return result


# ----------------------------
# 6) MAIN ENTRY POINT
# ----------------------------
def compute_ai_predictions_from_overall(insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    """
    insights_overall(home + away 데이터)에서 λ 추정 → 예측 계산.
    """

    home = insights_overall.get("home")
    away = insights_overall.get("away")

    if home is None or away is None:
        return {"error": "invalid insights_overall"}

    lam_home = estimate_lambda(home)
    lam_away = estimate_lambda(away)

    matrix = build_score_matrix(lam_home, lam_away)

    prob = compute_match_probabilities(matrix)
    top_scores = compute_top_scorelines(matrix)

    return {
        "lambda_home": round(lam_home, 3),
        "lambda_away": round(lam_away, 3),
        "probabilities": prob,
        "top_scorelines": top_scores
    }
