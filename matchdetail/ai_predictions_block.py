# matchdetail/ai_predictions_block.py

from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import math
from datetime import datetime, timezone


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

# ─────────────────────────────────────────────────────────────
# ✅ 추가 요인: 요일/시간대 + 피로/휴식 + 대회 동기 + 상대 강도
#   - 모두 "factor(곱셈)"로 λ(기대득점)에 반영
#   - 데이터/컬럼/테이블이 없으면 항상 1.0(중립)로 폴백
# ─────────────────────────────────────────────────────────────

def _try_fetch_all(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    try:
        rows = fetch_all(sql, params)
        return rows or []
    except Exception:
        return []


def _dow_hour_pg(ts: int) -> Tuple[int, int]:
    """
    Postgres EXTRACT(DOW) 기준(일=0..토=6)에 맞춘 (dow, hour).
    """
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    # python weekday: 월=0..일=6 → pg dow: 일=0..토=6
    dow_pg = (dt.weekday() + 1) % 7
    return dow_pg, int(dt.hour)


def _hour_bucket(hour: int) -> Tuple[int, int]:
    """
    시간대 버킷(단순 4분할):
      0-5 / 6-11 / 12-17 / 18-23
    """
    h = max(0, min(23, int(hour)))
    if h <= 5:
        return 0, 5
    if h <= 11:
        return 6, 11
    if h <= 17:
        return 12, 17
    return 18, 23


def _shrink_factor(raw: Optional[float], n: int, k: float = 10.0) -> float:
    """
    표본 수가 적으면 1.0으로 shrink.
      f = (n*raw + k*1.0) / (n+k)
    """
    if raw is None:
        return 1.0
    try:
        r = float(raw)
    except (TypeError, ValueError):
        return 1.0
    f = (n * r + k * 1.0) / (n + k)
    return float(_clamp(f, 0.85, 1.15))


def _rest_factor(rest_days: Optional[float]) -> float:
    """
    휴식일수 → 득점 기대 factor.
    (너무 과격하지 않게, 보수적으로)
    """
    if rest_days is None:
        return 1.0
    d = float(rest_days)
    if d < 2.0:
        return 0.93
    if d < 3.0:
        return 0.96
    if d < 5.0:
        return 1.00
    if d < 7.0:
        return 1.02
    return 1.03


def _fetch_last_game_ts_any_comp(team_id: int, before_ts: int) -> Optional[int]:
    rows = _try_fetch_all(
        """
        SELECT MAX(fixture_timestamp)::int AS last_ts
        FROM matches
        WHERE status_group IN ('FINISHED','AET','PEN')
          AND fixture_timestamp < %s
          AND (home_id=%s OR away_id=%s)
        """,
        (before_ts, team_id, team_id),
    )
    if not rows:
        return None
    v = rows[0].get("last_ts")
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _fetch_team_gf_ga_overall(
    team_ids: List[int], league_id: int, season: int, before_ts: int
) -> Dict[int, Dict[str, Any]]:
    """
    해당 리그/시즌/현재경기 이전 기준으로 팀별 overall avg_gf/avg_ga + n
    """
    if not team_ids:
        return {}

    # IN 파라미터 안전 구성
    placeholders = ",".join(["%s"] * len(team_ids))
    params: List[Any] = [league_id, season, before_ts] + team_ids

    rows = _try_fetch_all(
        f"""
        WITH m AS (
          SELECT fixture_timestamp, home_id AS team_id, home_ft::float AS gf, away_ft::float AS ga
          FROM matches
          WHERE league_id=%s AND season=%s
            AND status_group IN ('FINISHED','AET','PEN')
            AND fixture_timestamp < %s
            AND home_ft IS NOT NULL AND away_ft IS NOT NULL
          UNION ALL
          SELECT fixture_timestamp, away_id AS team_id, away_ft::float AS gf, home_ft::float AS ga
          FROM matches
          WHERE league_id=%s AND season=%s
            AND status_group IN ('FINISHED','AET','PEN')
            AND fixture_timestamp < %s
            AND home_ft IS NOT NULL AND away_ft IS NOT NULL
        )
        SELECT team_id,
               AVG(gf)::float AS avg_gf,
               AVG(ga)::float AS avg_ga,
               COUNT(*)::int AS n
        FROM m
        WHERE team_id IN ({placeholders})
        GROUP BY team_id
        """,
        (league_id, season, before_ts, league_id, season, before_ts, *team_ids),
    )

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = _safe_int(r.get("team_id"))
        if tid is None:
            continue
        out[tid] = {
            "avg_gf": _safe_float(r.get("avg_gf")),
            "avg_ga": _safe_float(r.get("avg_ga")),
            "n": _safe_int(r.get("n")) or 0,
        }
    return out


def _fetch_team_gf_ga_slot(
    team_ids: List[int],
    league_id: int,
    season: int,
    before_ts: int,
    dow_pg: int,
    h0: int,
    h1: int,
) -> Dict[int, Dict[str, Any]]:
    """
    요일/시간대 슬롯 기준 팀별 avg_gf/avg_ga + n
    """
    if not team_ids:
        return {}

    placeholders = ",".join(["%s"] * len(team_ids))

    rows = _try_fetch_all(
        f"""
        WITH m AS (
          SELECT fixture_timestamp, home_id AS team_id, home_ft::float AS gf, away_ft::float AS ga
          FROM matches
          WHERE league_id=%s AND season=%s
            AND status_group IN ('FINISHED','AET','PEN')
            AND fixture_timestamp < %s
            AND home_ft IS NOT NULL AND away_ft IS NOT NULL
          UNION ALL
          SELECT fixture_timestamp, away_id AS team_id, away_ft::float AS gf, home_ft::float AS ga
          FROM matches
          WHERE league_id=%s AND season=%s
            AND status_group IN ('FINISHED','AET','PEN')
            AND fixture_timestamp < %s
            AND home_ft IS NOT NULL AND away_ft IS NOT NULL
        )
        SELECT team_id,
               AVG(gf)::float AS avg_gf,
               AVG(ga)::float AS avg_ga,
               COUNT(*)::int AS n
        FROM m
        WHERE team_id IN ({placeholders})
          AND EXTRACT(DOW FROM to_timestamp(fixture_timestamp))::int = %s
          AND EXTRACT(HOUR FROM to_timestamp(fixture_timestamp))::int BETWEEN %s AND %s
        GROUP BY team_id
        """,
        (league_id, season, before_ts, league_id, season, before_ts, *team_ids, dow_pg, h0, h1),
    )

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = _safe_int(r.get("team_id"))
        if tid is None:
            continue
        out[tid] = {
            "avg_gf": _safe_float(r.get("avg_gf")),
            "avg_ga": _safe_float(r.get("avg_ga")),
            "n": _safe_int(r.get("n")) or 0,
        }
    return out


def _fetch_league_slot_mu_per_team(
    league_id: int,
    season: int,
    dow_pg: int,
    h0: int,
    h1: int,
) -> Tuple[Optional[float], int]:
    """
    리그 평균(팀당) 득점: 요일/시간대 슬롯 기준
    """
    rows = _try_fetch_all(
        """
        SELECT
          SUM(COALESCE(home_ft,0)+COALESCE(away_ft,0))::float AS goals_total,
          COUNT(*)::int AS n_games
        FROM matches
        WHERE league_id=%s AND season=%s
          AND status_group IN ('FINISHED','AET','PEN')
          AND home_ft IS NOT NULL AND away_ft IS NOT NULL
          AND EXTRACT(DOW FROM to_timestamp(fixture_timestamp))::int = %s
          AND EXTRACT(HOUR FROM to_timestamp(fixture_timestamp))::int BETWEEN %s AND %s
        """,
        (league_id, season, dow_pg, h0, h1),
    )
    if not rows:
        return None, 0

    goals_total = _safe_float(rows[0].get("goals_total")) or 0.0
    n_games = _safe_int(rows[0].get("n_games")) or 0
    if n_games <= 0:
        return None, 0

    mu_per_team = goals_total / (n_games * 2.0)
    return float(mu_per_team), n_games


def _fetch_standings_rows(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    standings 테이블 스키마가 환경마다 조금 다를 수 있어서 여러 패턴을 시도.
    없으면 빈 리스트.
    """
    candidates = [
        # (table, goalsdiff_col)
        ("standings", "goals_diff"),
        ("standings", "goalsDiff"),
        ("league_standings", "goals_diff"),
        ("league_standings", "goalsDiff"),
    ]

    for table, gdcol in candidates:
        rows = _try_fetch_all(
            f"""
            SELECT
              team_id,
              rank,
              points,
              {gdcol} AS goals_diff,
              "group" AS group_name,
              stage
            FROM {table}
            WHERE league_id=%s AND season=%s
            """,
            (league_id, season),
        )
        if rows:
            return rows

    # 마지막 폴백: group/stage 컬럼이 없을 수도
    for table, gdcol in candidates:
        rows = _try_fetch_all(
            f"""
            SELECT
              team_id,
              rank,
              points,
              {gdcol} AS goals_diff
            FROM {table}
            WHERE league_id=%s AND season=%s
            """,
            (league_id, season),
        )
        if rows:
            return rows

    return []


def _compute_opponent_strength_factor(
    league_id: int,
    season: int,
    home_id: int,
    away_id: int,
) -> Tuple[float, float]:
    """
    상대 강도(상대 rank + GD) → 득점 기대 factor.
    strong opponent → factor < 1
    weak opponent   → factor > 1
    """
    rows = _fetch_standings_rows(league_id, season)
    if not rows:
        return 1.0, 1.0

    by_team: Dict[int, Dict[str, Any]] = {}
    ranks: List[int] = []
    gds: List[float] = []

    for r in rows:
        tid = _safe_int(r.get("team_id"))
        if tid is None:
            continue
        rk = _safe_int(r.get("rank"))
        pt = _safe_int(r.get("points"))
        gd = _safe_float(r.get("goals_diff"))
        by_team[tid] = {
            "rank": rk,
            "points": pt,
            "gd": gd,
            "group": r.get("group_name"),
            "stage": r.get("stage"),
        }
        if rk is not None:
            ranks.append(rk)
        if gd is not None:
            gds.append(gd)

    opp_h = by_team.get(away_id)  # home 기준 상대는 away
    opp_a = by_team.get(home_id)  # away 기준 상대는 home
    if not opp_h or not opp_a or not ranks:
        return 1.0, 1.0

    avg_rank = (sum(ranks) / len(ranks)) if ranks else 10.0
    avg_rank = max(2.0, float(avg_rank))

    def _z(xs: List[float], x: float) -> float:
        if not xs:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((v - mu) ** 2 for v in xs) / max(1, (len(xs) - 1))
        sd = math.sqrt(max(1e-6, var))
        return (x - mu) / sd

    def _factor_from_opp(opp: Dict[str, Any]) -> float:
        rk = opp.get("rank")
        gd = opp.get("gd")
        if rk is None:
            return 1.0

        # rank가 낮을수록 강팀 → 감점
        rank_strength = (avg_rank - float(rk)) / avg_rank  # 강팀이면 +
        gd_strength = _z(gds, float(gd)) if gd is not None else 0.0

        # 보수적으로 합산
        idx = 0.9 * rank_strength + 0.35 * gd_strength

        # 강팀(+idx)일수록 exp(-k*idx) < 1
        f = math.exp(-0.18 * idx)
        return float(_clamp(f, 0.88, 1.12))

    return _factor_from_opp(opp_h), _factor_from_opp(opp_a)


def _compute_motivation_factor(
    league_id: int,
    season: int,
    fixture_ts: int,
    team_id: int,
) -> float:
    """
    대회 동기 factor (조별/16강 진출 여부 + 마지막 경기 중요도).
    standings가 없으면 1.0.
    """
    rows = _fetch_standings_rows(league_id, season)
    if not rows:
        return 1.0

    # 팀 row 찾기
    trow: Optional[Dict[str, Any]] = None
    for r in rows:
        if _safe_int(r.get("team_id")) == team_id:
            trow = r
            break
    if not trow:
        return 1.0

    rank = _safe_int(trow.get("rank"))
    points = _safe_int(trow.get("points"))
    group_name = trow.get("group_name") or trow.get("group")  # 호환
    stage = trow.get("stage")

    # 남은 경기 수(해당 리그/시즌 내)
    rem_rows = _try_fetch_all(
        """
        SELECT COUNT(*)::int AS n_rem
        FROM matches
        WHERE league_id=%s AND season=%s
          AND fixture_timestamp > %s
          AND (home_id=%s OR away_id=%s)
          AND (status_group IS NULL OR status_group NOT IN ('FINISHED','AET','PEN'))
        """,
        (league_id, season, fixture_ts, team_id, team_id),
    )
    n_rem = 0
    if rem_rows:
        n_rem = _safe_int(rem_rows[0].get("n_rem")) or 0

    is_last_games = (n_rem <= 1)

    # 16강/토너먼트(대략): stage/round 문자열이 있으면 가중
    stage_s = (str(stage).lower() if stage is not None else "")
    is_knockout = any(x in stage_s for x in ["round of 16", "quarter", "semi", "final", "knockout", "playoff", "16강", "8강", "4강", "결승"])

    # 조별: group_name이 Group/조 같은 텍스트면 조별로 간주
    g_s = (str(group_name).lower() if group_name is not None else "")
    is_group_stage = any(x in g_s for x in ["group", "조"])

    # 기본
    f = 1.0

    if is_knockout:
        # 토너먼트는 기본적으로 동기 높음
        f *= 1.03
        if is_last_games:
            f *= 1.01
        return float(_clamp(f, 0.95, 1.08))

    if is_group_stage and rank is not None and points is not None:
        # "조별/16강 진출 여부"를 단순히 "조 1~2위 = 진출권"으로 가정(데이터 없으면 보수적으로)
        # 같은 group의 2위/3위 포인트를 찾아 "경합"이면 가중
        same_group = []
        for r in rows:
            if (r.get("group_name") or r.get("group")) == group_name:
                same_group.append(r)

        def _points_of_rank(rrank: int) -> Optional[int]:
            cand = [x for x in same_group if _safe_int(x.get("rank")) == rrank]
            if not cand:
                return None
            return _safe_int(cand[0].get("points"))

        p2 = _points_of_rank(2)
        p3 = _points_of_rank(3)

        qualified = (rank <= 2)
        chasing = False

        # 3위인데 2위와 3점 이내면 동기 ↑
        if (rank == 3) and (p2 is not None) and (p2 - points <= 3):
            chasing = True

        # 2위인데 3위와 3점 이내면 방어 동기 ↑
        if (rank == 2) and (p3 is not None) and (points - p3 <= 3):
            chasing = True

        if is_last_games and chasing:
            f *= 1.06
        elif is_last_games and (not qualified):
            f *= 1.05
        elif is_last_games and qualified:
            # 이미 진출 확정이면 약간 다운
            f *= 0.99
        else:
            if chasing:
                f *= 1.03

    return float(_clamp(f, 0.94, 1.08))


def _compute_time_weekday_factor(
    league_id: int,
    season: int,
    home_id: int,
    away_id: int,
    fixture_ts: int,
    mu_for_per_team: float,
) -> Tuple[float, float]:
    """
    요일/시간대 효과 = (리그 평균 슬롯/전체) × (팀 슬롯/팀 전체) [shrink]
    → 득점 기대 factor로 반환 (home_factor, away_factor)
    """
    dow_pg, hour = _dow_hour_pg(fixture_ts)
    h0, h1 = _hour_bucket(hour)

    slot_mu, n_games = _fetch_league_slot_mu_per_team(league_id, season, dow_pg, h0, h1)
    if slot_mu is None or mu_for_per_team <= 0:
        league_factor = 1.0
    else:
        league_factor = float(_clamp(slot_mu / max(0.25, mu_for_per_team), 0.92, 1.08))

    overall = _fetch_team_gf_ga_overall([home_id, away_id], league_id, season, fixture_ts)
    slot = _fetch_team_gf_ga_slot([home_id, away_id], league_id, season, fixture_ts, dow_pg, h0, h1)

    def _team_factor(tid: int) -> float:
        o = overall.get(tid) or {}
        s = slot.get(tid) or {}

        ogf = _safe_float(o.get("avg_gf"))
        sgf = _safe_float(s.get("avg_gf"))
        on = int(o.get("n") or 0)
        sn = int(s.get("n") or 0)

        if ogf is None or ogf <= 0 or sgf is None:
            raw = None
        else:
            raw = sgf / max(0.05, ogf)

        # 팀 슬롯 표본 적으면 1.0으로 shrink
        tf = _shrink_factor(raw, n=sn, k=10.0)

        # 리그 효과(공통) + 팀 편차(개별) 섞기(과하지 않게)
        # league 60%, team 40% 정도
        f = (league_factor ** 0.60) * (tf ** 0.40)
        return float(_clamp(f, 0.90, 1.10))

    return _team_factor(home_id), _team_factor(away_id)


def _compute_context_goal_factors(
    league_id: int,
    season: int,
    home_id: int,
    away_id: int,
    fixture_ts: int,
    mu_for_per_team: float,
) -> Tuple[float, float]:
    """
    최종 득점 기대 factor(home, away):
      요일/시간대 × 휴식 × 동기 × 상대강도
    """
    # 1) 요일/시간대
    f_time_h, f_time_a = _compute_time_weekday_factor(
        league_id, season, home_id, away_id, fixture_ts, mu_for_per_team
    )

    # 2) 휴식(대회 구분 없이)
    last_h = _fetch_last_game_ts_any_comp(home_id, fixture_ts)
    last_a = _fetch_last_game_ts_any_comp(away_id, fixture_ts)

    rest_h = (fixture_ts - last_h) / 86400.0 if last_h else None
    rest_a = (fixture_ts - last_a) / 86400.0 if last_a else None

    f_rest_h = _rest_factor(rest_h)
    f_rest_a = _rest_factor(rest_a)

    # 3) 대회 동기(조별/16강/마지막 경기 중요도)
    f_mot_h = _compute_motivation_factor(league_id, season, fixture_ts, home_id)
    f_mot_a = _compute_motivation_factor(league_id, season, fixture_ts, away_id)

    # 4) 상대 강도(상대 스탠딩/골득실 가중)
    f_opp_h, f_opp_a = _compute_opponent_strength_factor(league_id, season, home_id, away_id)

    # 합성(과격 방지)
    home_factor = f_time_h * f_rest_h * f_mot_h * f_opp_h
    away_factor = f_time_a * f_rest_a * f_mot_a * f_opp_a

    return float(_clamp(home_factor, 0.85, 1.15)), float(_clamp(away_factor, 0.85, 1.15))



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

    # ─────────────────────────────────────────
    # ✅ 추가 요인 반영 (요일/시간대, 휴식, 동기, 상대강도)
    # ─────────────────────────────────────────
    try:
        f_goal_home, f_goal_away = _compute_context_goal_factors(
            league_id=league_id,
            season=season,
            home_id=home_id,
            away_id=away_id,
            fixture_ts=before_ts,
            mu_for_per_team=mu_for_per_team,
        )
    except Exception:
        f_goal_home, f_goal_away = 1.0, 1.0

    lam_home_ft *= f_goal_home
    lam_away_ft *= f_goal_away

    # 최종 clamp
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
