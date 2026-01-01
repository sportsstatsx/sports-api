# matchdetail/ai_predictions_block.py

from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import math

from db import fetch_all
from .ai_predictions_engine import (
    compute_ai_predictions_from_overall,
    compute_ai_predictions_from_lambdas,
)


def _is_continental_league(league_id: Any) -> bool:
    """
    현재 fixture 의 league_id 가 UEFA / ACL 같은 대륙컵 계열인지 간단히 판별.
    - leagues.name 을 한 번 조회해서 문자열로 체크한다.
    """
    try:
        lid = int(league_id)
    except (TypeError, ValueError):
        return False

    try:
        rows = fetch_all(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            LIMIT 1
            """,
            (lid,),
        )
    except Exception:
        return False

    if not rows:
        return False

    name = (rows[0].get("name") or "").strip().lower()
    if not name:
        return False

    # UEFA / 유럽 대륙컵 계열
    if (
        "uefa" in name
        or "champions league" in name
        or "europa league" in name
        or "conference league" in name
    ):
        return True

    # 아시아 ACL 계열
    if "afc" in name or "acl" in name or "afc champions league" in name:
        return True

    return False


def _build_ai_comp_block(
    *,
    header: Dict[str, Any],
    insights_overall: Dict[str, Any],
) -> Dict[str, Any]:
    """
    insights_overall.filters.comp 를 기반으로
    AI Predictions 전용 comp 블록을 만든다.

    - 기본: insights_overall 과 동일한 options/selected
    - 대륙컵 경기(UEFA / ACL 등)일 때:
        → All + (컵/대륙컵 계열 이름)만 남기고, 각 리그 이름은 제거
    """
    filters_overall = insights_overall.get("filters") or {}
    comp_block = filters_overall.get("comp") or {}

    raw_options = list(comp_block.get("options") or [])
    raw_selected = comp_block.get("selected") or "All"

    league_id = header.get("league_id")
    is_continental = _is_continental_league(league_id)

    # 기본값: 그대로 복사
    ai_options: List[str] = raw_options[:]
    ai_selected: str = str(raw_selected) if raw_selected is not None else "All"

    if is_continental and raw_options:
        kept: List[str] = []

        for opt in raw_options:
            s = str(opt).strip()
            if not s:
                continue

            # All 은 항상 유지
            if s == "All":
                kept.append(s)
                continue

            lower = s.lower()

            # 컵 / 대륙컵 계열만 남긴다
            is_cup = (
                "cup" in lower
                or "copa" in lower
                or "컵" in lower
                or "taça" in lower
                or "杯" in lower
            )
            is_uefa = (
                "uefa" in lower
                or "champions league" in lower
                or "europa league" in lower
                or "conference league" in lower
            )
            is_acl = (
                "afc" in lower
                or "acl" in lower
                or "afc champions league" in lower
            )

            if is_cup or is_uefa or is_acl:
                if s not in kept:
                    kept.append(s)

        # 최소 한 개는 보장
        ai_options = kept or ["All"]

        # 선택 값이 빠졌으면 All 로 폴백
        if ai_selected not in ai_options:
            ai_selected = "All"

    return {
        "options": ai_options,
        "selected": ai_selected,
    }


def _build_ai_last_n_block(insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    """
    last_n 은 그냥 insights_overall 쪽 값을 그대로 복사해서 내려준다.
    (나중에 필요하면 여기서만 별도로 커스터마이징 가능)
    """
    filters_overall = insights_overall.get("filters") or {}
    last_n_block = filters_overall.get("last_n") or {}

    options = list(last_n_block.get("options") or [])
    selected = last_n_block.get("selected") or "Last 10"

    return {
        "options": options,
        "selected": selected,
    }


# ─────────────────────────────────────────────────────────────
# ✅ DB 기반 λ(기대득점) 계산: 현재시즌 + 최근10/5 + 리그평균 μ
# ─────────────────────────────────────────────────────────────

def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _get_nested(d: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _get_fixture_meta(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    header에 home/away/league/season/timestamp 가 빠진 경우가 있어서
    matches 테이블에서 보완한다.
    """
    fixture_id = _safe_int(header.get("fixture_id") or header.get("id") or header.get("fixture"))
    if fixture_id is None:
        return None

    # header 우선
    league_id = _safe_int(header.get("league_id"))
    season = _safe_int(header.get("season"))
    home_id = _safe_int(header.get("home_id") or header.get("home_team_id") or header.get("home"))
    away_id = _safe_int(header.get("away_id") or header.get("away_team_id") or header.get("away"))
    ts = _safe_float(header.get("fixture_timestamp") or header.get("timestamp"))

    if league_id and season and home_id and away_id and ts:
        return {
            "fixture_id": fixture_id,
            "league_id": league_id,
            "season": season,
            "home_id": home_id,
            "away_id": away_id,
            "fixture_timestamp": int(ts),
        }

    # DB에서 보완
    rows = fetch_all(
        """
        SELECT fixture_id, league_id, season, home_id, away_id, fixture_timestamp
        FROM matches
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    if not rows:
        return None

    r = rows[0]
    league_id = league_id or _safe_int(r.get("league_id"))
    season = season or _safe_int(r.get("season"))
    home_id = home_id or _safe_int(r.get("home_id"))
    away_id = away_id or _safe_int(r.get("away_id"))
    ts = ts or _safe_float(r.get("fixture_timestamp"))

    if not (league_id and season and home_id and away_id and ts):
        return None

    return {
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season": season,
        "home_id": home_id,
        "away_id": away_id,
        "fixture_timestamp": int(ts),
    }


def _fetch_league_mu(league_id: int, season: int) -> Tuple[float, float, float, float]:
    """
    해당 competition(league_id, season) 내 FINISHED 경기만으로:
    - mu_for_per_team: 팀당 평균 득점(= 전체골 / (경기수*2))
    - mu_home, mu_away: 홈/원정 평균 득점
    - share_1h_league: 전반 득점 비중(HT/FT)
    """
    rows = fetch_all(
        """
        SELECT
          AVG(COALESCE(home_ft,0))::float AS mu_home,
          AVG(COALESCE(away_ft,0))::float AS mu_away,
          SUM(COALESCE(home_ft,0) + COALESCE(away_ft,0))::float AS goals_total,
          COUNT(*)::int AS n_games,
          SUM(COALESCE(home_ht,0) + COALESCE(away_ht,0))::float AS goals_1h_total
        FROM matches
        WHERE league_id=%s AND season=%s
          AND status_group IN ('FINISHED','AET','PEN')
          AND home_ft IS NOT NULL AND away_ft IS NOT NULL
        """,
        (league_id, season),
    )
    if not rows:
        return 1.25, 1.15, 1.20, 0.45

    r = rows[0]
    mu_home = float(r.get("mu_home") or 1.25)
    mu_away = float(r.get("mu_away") or 1.15)
    goals_total = float(r.get("goals_total") or 0.0)
    n_games = int(r.get("n_games") or 0)
    goals_1h_total = float(r.get("goals_1h_total") or 0.0)

    mu_for_per_team = goals_total / (n_games * 2.0) if n_games > 0 else (mu_home + mu_away) / 2.0
    share_1h = (goals_1h_total / goals_total) if goals_total > 0 else 0.45
    share_1h = _clamp(share_1h, 0.35, 0.65)

    return mu_home, mu_away, mu_for_per_team, share_1h


def _fetch_team_last_matches(
    team_id: int,
    *,
    league_id: int,
    season: int,
    before_ts: int,
    limit: int,
) -> List[Dict[str, Any]]:
    return fetch_all(
        """
        SELECT
          fixture_id, fixture_timestamp,
          home_id, away_id,
          home_ft, away_ft,
          home_ht, away_ht
        FROM matches
        WHERE league_id=%s AND season=%s
          AND status_group IN ('FINISHED','AET','PEN')
          AND fixture_timestamp < %s
          AND (home_id=%s OR away_id=%s)
          AND home_ft IS NOT NULL AND away_ft IS NOT NULL
        ORDER BY fixture_timestamp DESC
        LIMIT %s
        """,
        (league_id, season, before_ts, team_id, team_id, limit),
    )


def _weighted_avg(values: List[float]) -> Optional[float]:
    """최근일수록 가중치를 조금 더 주는 단순 가중 평균."""
    if not values:
        return None
    # values 는 최신->과거 순서로 들어오므로, 최신에 더 큰 가중치
    n = len(values)
    weights = [1.0 + 0.08 * (n - 1 - i) for i in range(n)]  # (최신)~(과거)
    wsum = sum(weights)
    if wsum <= 0:
        return None
    return sum(v * w for v, w in zip(values, weights)) / wsum


def _compute_team_attack_defense(
    team_id: int,
    rows: List[Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    rows(최신->과거)에서:
    - atk: 평균 득점
    - dfn: 평균 실점
    - share_1h: (팀 득점 기준) 전반 득점 비중
    """
    gf: List[float] = []
    ga: List[float] = []
    gf_ht: List[float] = []

    for r in rows:
        home_id = _safe_int(r.get("home_id"))
        away_id = _safe_int(r.get("away_id"))
        if home_id is None or away_id is None:
            continue

        is_home = (home_id == team_id)

        hft = _safe_float(r.get("home_ft"))
        aft = _safe_float(r.get("away_ft"))
        hht = _safe_float(r.get("home_ht"))
        aht = _safe_float(r.get("away_ht"))

        if hft is None or aft is None:
            continue

        if is_home:
            gf.append(hft)
            ga.append(aft)
            if hht is not None:
                gf_ht.append(hht)
        else:
            gf.append(aft)
            ga.append(hft)
            if aht is not None:
                gf_ht.append(aht)

    atk = _weighted_avg(gf)
    dfn = _weighted_avg(ga)

    share = None
    if gf and gf_ht:
        denom = sum(gf)
        if denom > 0:
            share = sum(gf_ht) / denom
            share = _clamp(float(share), 0.30, 0.70)

    return atk, dfn, share


def _apply_form_adjustment(v10: Optional[float], v5: Optional[float], k: float = 0.55) -> Optional[float]:
    """
    최근10을 기본으로 하되, 최근5의 편차를 k로 반영:
      v = v10 * (1 + k * ((v5 - v10) / max(v10, eps)))
    """
    if v10 is None:
        return v5
    if v5 is None:
        return v10
    base = max(0.05, float(v10))
    delta = (float(v5) - float(v10)) / base
    return max(0.05, base * (1.0 + k * delta))


def _shrink_share(team_share: Optional[float], league_share: float, n: int, k: float = 8.0) -> float:
    """
    전반 득점 비중 share를 리그 평균으로 shrink:
      share = (n*team_share + k*league_share) / (n + k)
    """
    if team_share is None:
        return league_share
    share = (n * float(team_share) + k * float(league_share)) / (n + k)
    return _clamp(share, 0.35, 0.65)


def _compute_lambdas_from_db(
    meta: Dict[str, Any],
    insights_overall: Optional[Dict[str, Any]],
) -> Optional[Dict[str, float]]:
    league_id = int(meta["league_id"])
    season = int(meta["season"])
    home_id = int(meta["home_id"])
    away_id = int(meta["away_id"])
    before_ts = int(meta["fixture_timestamp"])

    mu_home, mu_away, mu_for_per_team, league_share_1h = _fetch_league_mu(league_id, season)

    # 최근 10 / 5 (해당 competition 기준)
    h10 = _fetch_team_last_matches(home_id, league_id=league_id, season=season, before_ts=before_ts, limit=10)
    a10 = _fetch_team_last_matches(away_id, league_id=league_id, season=season, before_ts=before_ts, limit=10)
    h5 = h10[:5]
    a5 = a10[:5]

    h_atk10, h_def10, h_share10 = _compute_team_attack_defense(home_id, h10)
    a_atk10, a_def10, a_share10 = _compute_team_attack_defense(away_id, a10)
    h_atk5, h_def5, _ = _compute_team_attack_defense(home_id, h5)
    a_atk5, a_def5, _ = _compute_team_attack_defense(away_id, a5)

    # 표본이 너무 적으면 insights_overall 평균으로 폴백
    if (h_atk10 is None or a_atk10 is None) and isinstance(insights_overall, dict):
        home = insights_overall.get("home") or {}
        away = insights_overall.get("away") or {}
        if h_atk10 is None:
            h_atk10 = _safe_float(_get_nested(home, ["avg_gf", "home"])) or _safe_float(_get_nested(home, ["avg_gf", "total"]))
        if a_atk10 is None:
            a_atk10 = _safe_float(_get_nested(away, ["avg_gf", "away"])) or _safe_float(_get_nested(away, ["avg_gf", "total"]))
        if h_def10 is None:
            h_def10 = _safe_float(_get_nested(home, ["avg_ga", "home"])) or _safe_float(_get_nested(home, ["avg_ga", "total"]))
        if a_def10 is None:
            a_def10 = _safe_float(_get_nested(away, ["avg_ga", "away"])) or _safe_float(_get_nested(away, ["avg_ga", "total"]))

    if h_atk10 is None or a_atk10 is None:
        return None

    # 최근5 편차 반영(폼)
    h_atk = _apply_form_adjustment(h_atk10, h_atk5, k=0.55)
    a_atk = _apply_form_adjustment(a_atk10, a_atk5, k=0.55)
    h_def = _apply_form_adjustment(h_def10, h_def5, k=0.40) if h_def10 is not None else None
    a_def = _apply_form_adjustment(a_def10, a_def5, k=0.40) if a_def10 is not None else None

    # 수비가 없으면 리그 평균(팀당)
    if h_def is None:
        h_def = mu_for_per_team
    if a_def is None:
        a_def = mu_for_per_team

    # FT λ:
    # - 홈 득점 기대: mu_home * (홈 공격 / 리그 팀당 평균득점) * (원정 수비실점 / 리그 팀당 평균득점)
    # - 원정 득점 기대: mu_away * (원정 공격 / 리그 팀당 평균득점) * (홈 수비실점 / 리그 팀당 평균득점)
    denom = max(0.25, mu_for_per_team)
    lam_home_ft = mu_home * (h_atk / denom) * (a_def / denom)
    lam_away_ft = mu_away * (a_atk / denom) * (h_def / denom)

    lam_home_ft = max(0.05, float(lam_home_ft))
    lam_away_ft = max(0.05, float(lam_away_ft))

    # 1H 비중(팀 득점 기준 share를 리그 평균으로 shrink)
    h_share = _shrink_share(h_share10, league_share_1h, n=len(h10), k=8.0)
    a_share = _shrink_share(a_share10, league_share_1h, n=len(a10), k=8.0)

    lam_home_1h = max(0.01, lam_home_ft * h_share)
    lam_away_1h = max(0.01, lam_away_ft * a_share)
    lam_home_2h = max(0.01, lam_home_ft - lam_home_1h)
    lam_away_2h = max(0.01, lam_away_ft - lam_away_1h)

    return {
        "lam_home_ft": lam_home_ft,
        "lam_away_ft": lam_away_ft,
        "lam_home_1h": lam_home_1h,
        "lam_away_1h": lam_away_1h,
        "lam_home_2h": lam_home_2h,
        "lam_away_2h": lam_away_2h,
    }


def build_ai_predictions_block(
    header: Dict[str, Any],
    insights_overall: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    matchdetail/insights_block 에서 만든 insights_overall 블록을 기반으로
    AI Predictions 블록(dict)을 생성한다.

    ✅ 변경 사항(요청 반영):
    - 현재시즌 + 최근10/최근5 폼 + 리그 평균 μ를 이용해 FT/1H/2H λ를 DB에서 계산
    - 산출된 λ로 FT/1H/2H 1X2/더블찬스/오버/BTTS/클린시트 등을 모두 생성
    - goal_0_15 / goal_80_90+ / 카드/코너 등은 insights_overall 데이터가 있으면 그대로 추가

    ⚠️ DB 계산이 불가능(표본 부족/메타 누락)하면 기존 compute_ai_predictions_from_overall 로 폴백.
    """
    if not insights_overall:
        return None

    try:
        meta = _get_fixture_meta(header)
        lambdas = None
        if meta:
            lambdas = _compute_lambdas_from_db(meta, insights_overall)

        if lambdas:
            predictions = compute_ai_predictions_from_lambdas(
                lam_home_ft=lambdas["lam_home_ft"],
                lam_away_ft=lambdas["lam_away_ft"],
                lam_home_1h=lambdas["lam_home_1h"],
                lam_away_1h=lambdas["lam_away_1h"],
                lam_home_2h=lambdas["lam_home_2h"],
                lam_away_2h=lambdas["lam_away_2h"],
                insights_overall=insights_overall,
            )
        else:
            predictions = compute_ai_predictions_from_overall(insights_overall)

        if not isinstance(predictions, dict):
            return None

        # 🔥 AI 전용 필터 블록
        filters_block = {
            "comp": _build_ai_comp_block(
                header=header,
                insights_overall=insights_overall,
            ),
            "last_n": _build_ai_last_n_block(insights_overall),
        }

        predictions["filters"] = filters_block
        return predictions

    except Exception as e:
        # 문제가 생겨도 번들 전체가 죽지 않도록 방어
        print(f"[AI_PREDICTIONS] error while computing predictions: {e}")
        return None
