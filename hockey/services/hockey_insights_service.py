# hockey/services/hockey_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all


COMPLETED_STATUSES = ("FT", "AOT", "AP")  # 종료 경기만 인사이트 샘플로 사용


# ─────────────────────────────────────────
# Utils
# ─────────────────────────────────────────
def _pct(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return round((n * 100.0) / float(d), 1)


def _is_goal(ev: Dict[str, Any]) -> bool:
    return (ev.get("type") or "").strip().lower() == "goal"


def _period(p: Any) -> str:
    return (str(p).strip().upper() if p is not None else "")


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _is_home(team_id: int, home_team_id: Optional[int]) -> bool:
    return home_team_id is not None and team_id == home_team_id


def _is_away(team_id: int, away_team_id: Optional[int]) -> bool:
    return away_team_id is not None and team_id == away_team_id


def _norm_minute_for_period(
    *,
    minute: Optional[int],
    period: str,
    mode: str,
) -> Optional[int]:
    """
    mode:
      - "period": minute이 피리어드 내부 분(0~20)
      - "cumulative": minute이 경기 누적 분(예: P2=20~40, P3=40~60)
    반환: 피리어드 내부 minute (0~20 범위 기대)
    """
    if minute is None:
        return None

    m = int(minute)
    p = period

    if mode == "period":
        return m

    # cumulative
    if p == "P1":
        return m
    if p == "P2":
        return m - 20
    if p == "P3":
        return m - 40
    if p == "OT":
        # 공급자마다 다를 수 있으니 보수적으로: 60분 이후는 OT로 본다
        # (OT 길이 5/10 등 리그별 다름)
        return m - 60

    return m


def _detect_minute_mode(events: List[Dict[str, Any]]) -> str:
    """
    minute 컬럼이 period 기준인지 cumulative 기준인지 자동 판별.
    - P3 이벤트 minute 값이 30 이상이면 cumulative 가능성이 매우 높음 (40~60 근처)
    - P2 이벤트 minute 값이 20 이상이면 cumulative 가능성 높음
    - 그 외는 period로 본다.
    """
    p2_max = None
    p3_max = None
    for e in events:
        p = _period(e.get("period"))
        m = _safe_int(e.get("minute"))
        if m is None:
            continue
        if p == "P2":
            p2_max = m if p2_max is None else max(p2_max, m)
        if p == "P3":
            p3_max = m if p3_max is None else max(p3_max, m)

    if (p3_max is not None and p3_max >= 30) or (p2_max is not None and p2_max >= 20):
        return "cumulative"
    return "period"


def _bucket_2min(min_in_period: Optional[int]) -> Optional[int]:
    """
    2분 단위 버킷 index (0~9):
      0: [0,2)
      1: [2,4)
      ...
      9: [18,20+]
    """
    if min_in_period is None:
        return None
    m = max(0, int(min_in_period))
    idx = m // 2
    if idx < 0:
        idx = 0
    if idx > 9:
        idx = 9
    return idx


@dataclass
class GameSample:
    game_id: int
    home_team_id: Optional[int]
    away_team_id: Optional[int]
    game_date: Optional[datetime]


def _fetch_last_n_games(
    *,
    team_id: int,
    last_n: int,
    season: Optional[int],
    league_id: Optional[int],
) -> List[GameSample]:
    where = [
        "(g.home_team_id = %s OR g.away_team_id = %s)",
        "g.status = ANY(%s)",
    ]
    params: List[Any] = [team_id, team_id, list(COMPLETED_STATUSES)]

    if season is not None:
        where.append("g.season = %s")
        params.append(season)

    if league_id is not None:
        where.append("g.league_id = %s")
        params.append(league_id)

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.away_team_id,
            g.game_date
        FROM hockey_games g
        WHERE {where_sql}
        ORDER BY g.game_date DESC NULLS LAST, g.id DESC
        LIMIT %s
    """
    params.append(last_n)

    rows = hockey_fetch_all(sql, tuple(params))
    out: List[GameSample] = []
    for r in rows:
        out.append(
            GameSample(
                game_id=r["game_id"],
                home_team_id=r.get("home_team_id"),
                away_team_id=r.get("away_team_id"),
                game_date=r.get("game_date"),
            )
        )
    return out


def _fetch_events_for_games(game_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not game_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(game_ids))
    sql = f"""
        SELECT
            e.game_id,
            e.period,
            e.minute,
            e.team_id,
            e.type,
            e.comment,
            e.event_order
        FROM hockey_game_events e
        WHERE e.game_id IN ({placeholders})
        ORDER BY e.game_id ASC, e.period ASC, e.minute ASC NULLS LAST, e.event_order ASC
    """
    rows = hockey_fetch_all(sql, tuple(game_ids))

    m: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        gid = r["game_id"]
        m.setdefault(gid, []).append(
            {
                "game_id": gid,
                "period": r.get("period"),
                "minute": r.get("minute"),
                "team_id": r.get("team_id"),
                "type": r.get("type"),
                "comment": r.get("comment"),
                "event_order": r.get("event_order"),
            }
        )
    return m


# ─────────────────────────────────────────
# Insight Calculations
# ─────────────────────────────────────────
def _regular_time_result_for_team(
    *,
    team_id: int,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    events: List[Dict[str, Any]],
) -> Optional[str]:
    """
    정규시간 결과(Regular Time) = P1~P3 goals 합으로 판정.
    반환: "W" / "D" / "L" or None (팀이 홈/어웨이 아니면)
    """
    if not (_is_home(team_id, home_team_id) or _is_away(team_id, away_team_id)):
        return None

    home_goals = 0
    away_goals = 0
    for e in events:
        if not _is_goal(e):
            continue
        p = _period(e.get("period"))
        if p not in ("P1", "P2", "P3"):
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        if home_team_id is not None and tid == home_team_id:
            home_goals += 1
        elif away_team_id is not None and tid == away_team_id:
            away_goals += 1

    # team side
    if _is_home(team_id, home_team_id):
        team_goals = home_goals
        opp_goals = away_goals
    else:
        team_goals = away_goals
        opp_goals = home_goals

    if team_goals > opp_goals:
        return "W"
    if team_goals == opp_goals:
        return "D"
    return "L"


def _score_at_3p_start(
    *,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    events: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """
    3P 시작 스코어 = P1+P2 goal 이벤트 누적
    반환: (home, away)
    """
    home = 0
    away = 0
    for e in events:
        if not _is_goal(e):
            continue
        p = _period(e.get("period"))
        if p not in ("P1", "P2"):
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        if home_team_id is not None and tid == home_team_id:
            home += 1
        elif away_team_id is not None and tid == away_team_id:
            away += 1
    return home, away


def _first_goal_team_in_period(
    *,
    period: str,
    events: List[Dict[str, Any]],
) -> Optional[int]:
    """
    특정 period 내 첫 goal team_id
    정렬은 이미 query에서 period/minute/order로 정렬됨.
    """
    for e in events:
        if not _is_goal(e):
            continue
        if _period(e.get("period")) != period:
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is not None:
            return tid
    return None


def _first_goal_after_condition(
    *,
    start_period: str,
    condition_zero_zero_until_period_start: bool,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    events: List[Dict[str, Any]],
) -> Optional[int]:
    """
    "2P Start 0-0" 혹은 "3P Start 0-0" 같은 조건에서
    그 이후 첫 goal의 team_id 반환.
    - condition_zero_zero_until_period_start=True 인 경우:
      시작 period 직전까지(예: 2P면 P1까지, 3P면 P1+P2까지) 득점이 0-0인지 확인.
    """
    # 0-0 체크
    if condition_zero_zero_until_period_start:
        periods_before = ("P1",) if start_period == "P2" else ("P1", "P2")
        h = a = 0
        for e in events:
            if not _is_goal(e):
                continue
            p = _period(e.get("period"))
            if p not in periods_before:
                continue
            tid = _safe_int(e.get("team_id"))
            if tid is None:
                continue
            if home_team_id is not None and tid == home_team_id:
                h += 1
            elif away_team_id is not None and tid == away_team_id:
                a += 1
        if h != 0 or a != 0:
            return None  # 조건 불만족

    # start_period부터의 첫 골
    for e in events:
        if not _is_goal(e):
            continue
        p = _period(e.get("period"))
        if p in ("P1", "P2", "P3", "OT") and p < start_period:
            continue
        if p != start_period:
            # start_period만 보는 게 아니라 "start 이후 전체"로 보려면 비교가 필요하지만,
            # 우리는 'start period부터'만 보면 충분 (P2 start면 P2에서 첫 골, 없으면 P3/OT로 넘어가는 정의도 가능)
            # 지금은 보수적으로: start_period 안에서만 첫 골을 본다.
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is not None:
            return tid

    return None


def _count_goal_timing_distribution(
    *,
    team_id: int,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    events: List[Dict[str, Any]],
) -> Dict[str, Dict[str, List[int]]]:
    """
    Period별 2분 구간(10 buckets) 득점/실점 배열 반환:
      {
        "P1": {"for":[10], "against":[10]},
        "P2": ...
        "P3": ...
      }
    """
    mode = _detect_minute_mode(events)

    out = {
        "P1": {"for": [0]*10, "against": [0]*10},
        "P2": {"for": [0]*10, "against": [0]*10},
        "P3": {"for": [0]*10, "against": [0]*10},
    }

    for e in events:
        if not _is_goal(e):
            continue
        p = _period(e.get("period"))
        if p not in ("P1", "P2", "P3"):
            continue

        m_raw = _safe_int(e.get("minute"))
        m_in = _norm_minute_for_period(minute=m_raw, period=p, mode=mode)
        b = _bucket_2min(m_in)
        if b is None:
            continue

        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue

        # 선택 팀이 홈/어웨이인지도 모르는데 for/against는 "선택팀 기준"이라서
        # team_id 일치하면 for, 아니면 against로 처리 (단, 상대팀 goal만 카운트)
        if tid == team_id:
            out[p]["for"][b] += 1
        else:
            # 상대 득점(= 선택팀 실점)으로 처리하려면, 이 경기에 팀이 참가한 경기여야 함
            if _is_home(team_id, home_team_id) or _is_away(team_id, away_team_id):
                out[p]["against"][b] += 1

    return out


def hockey_get_team_insights(
    *,
    team_id: int,
    last_n: int,
    season: Optional[int] = None,
    league_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    핵심 반환:
      - sample: totals/home/away 각각의 경기 수
      - blocks: 인사이트 표/게이지바용 데이터 (모두 totals/home/away)
    """
    games = _fetch_last_n_games(team_id=team_id, last_n=last_n, season=season, league_id=league_id)
    game_ids = [g.game_id for g in games]

    events_map = _fetch_events_for_games(game_ids)

    # 샘플 분류
    totals_games = games
    home_games = [g for g in games if _is_home(team_id, g.home_team_id)]
    away_games = [g for g in games if _is_away(team_id, g.away_team_id)]

    def calc_bundle(subset: List[GameSample]) -> Dict[str, Any]:
        # counters
        n_games = len(subset)

        # Regular Time outcome counts
        w = d = l = 0

        # 3P start categories -> outcome distribution
        lead3_w = lead3_d = lead3_l = 0
        tied3_w = tied3_d = tied3_l = 0
        trail3_w = trail3_d = trail3_l = 0
        lead3_n = tied3_n = trail3_n = 0

        # First goal impact (1P)
        fg1p_w = fg1p_d = fg1p_l = 0
        fg1p_n = 0
        cg1p_w = cg1p_d = cg1p_l = 0
        cg1p_n = 0

        # 2P start 0-0 -> first goal team impact
        fg2_00_w = fg2_00_d = fg2_00_l = 0
        fg2_00_n = 0
        cg2_00_w = cg2_00_d = cg2_00_l = 0
        cg2_00_n = 0

        # 3P start 0-0 -> first goal team impact
        fg3_00_w = fg3_00_d = fg3_00_l = 0
        fg3_00_n = 0
        cg3_00_w = cg3_00_d = cg3_00_l = 0
        cg3_00_n = 0

        # Goal timing distribution aggregate
        gtd = {
            "P1": {"for": [0]*10, "against": [0]*10},
            "P2": {"for": [0]*10, "against": [0]*10},
            "P3": {"for": [0]*10, "against": [0]*10},
        }

        for g in subset:
            evs = events_map.get(g.game_id, [])

            # Regular time outcome
            res = _regular_time_result_for_team(
                team_id=team_id,
                home_team_id=g.home_team_id,
                away_team_id=g.away_team_id,
                events=evs,
            )
            if res == "W":
                w += 1
            elif res == "D":
                d += 1
            elif res == "L":
                l += 1

            # 3P start state -> regular outcome
            h2, a2 = _score_at_3p_start(
                home_team_id=g.home_team_id,
                away_team_id=g.away_team_id,
                events=evs,
            )

            # team perspective
            if _is_home(team_id, g.home_team_id):
                team_s = h2
                opp_s = a2
            elif _is_away(team_id, g.away_team_id):
                team_s = a2
                opp_s = h2
            else:
                continue

            if team_s > opp_s:
                lead3_n += 1
                if res == "W":
                    lead3_w += 1
                elif res == "D":
                    lead3_d += 1
                elif res == "L":
                    lead3_l += 1
            elif team_s == opp_s:
                tied3_n += 1
                if res == "W":
                    tied3_w += 1
                elif res == "D":
                    tied3_d += 1
                elif res == "L":
                    tied3_l += 1
            else:
                trail3_n += 1
                if res == "W":
                    trail3_w += 1
                elif res == "D":
                    trail3_d += 1
                elif res == "L":
                    trail3_l += 1

            # 1P first goal impact
            fg_team = _first_goal_team_in_period(period="P1", events=evs)
            if fg_team is not None:
                if fg_team == team_id:
                    fg1p_n += 1
                    if res == "W":
                        fg1p_w += 1
                    elif res == "D":
                        fg1p_d += 1
                    elif res == "L":
                        fg1p_l += 1
                else:
                    cg1p_n += 1
                    if res == "W":
                        cg1p_w += 1
                    elif res == "D":
                        cg1p_d += 1
                    elif res == "L":
                        cg1p_l += 1

            # 2P start 0-0
            # (현재 구현은 "P2 안에서의 첫 골"만 집계. 필요하면 P2 없으면 P3로 넘어가게 확장 가능)
            fg2 = _first_goal_after_condition(
                start_period="P2",
                condition_zero_zero_until_period_start=True,
                home_team_id=g.home_team_id,
                away_team_id=g.away_team_id,
                events=evs,
            )
            if fg2 is not None:
                if fg2 == team_id:
                    fg2_00_n += 1
                    if res == "W":
                        fg2_00_w += 1
                    elif res == "D":
                        fg2_00_d += 1
                    elif res == "L":
                        fg2_00_l += 1
                else:
                    cg2_00_n += 1
                    if res == "W":
                        cg2_00_w += 1
                    elif res == "D":
                        cg2_00_d += 1
                    elif res == "L":
                        cg2_00_l += 1

            # 3P start 0-0
            fg3 = _first_goal_after_condition(
                start_period="P3",
                condition_zero_zero_until_period_start=True,
                home_team_id=g.home_team_id,
                away_team_id=g.away_team_id,
                events=evs,
            )
            if fg3 is not None:
                if fg3 == team_id:
                    fg3_00_n += 1
                    if res == "W":
                        fg3_00_w += 1
                    elif res == "D":
                        fg3_00_d += 1
                    elif res == "L":
                        fg3_00_l += 1
                else:
                    cg3_00_n += 1
                    if res == "W":
                        cg3_00_w += 1
                    elif res == "D":
                        cg3_00_d += 1
                    elif res == "L":
                        cg3_00_l += 1

            # Goal timing distribution aggregate
            dist = _count_goal_timing_distribution(
                team_id=team_id,
                home_team_id=g.home_team_id,
                away_team_id=g.away_team_id,
                events=evs,
            )
            for p in ("P1", "P2", "P3"):
                for i in range(10):
                    gtd[p]["for"][i] += dist[p]["for"][i]
                    gtd[p]["against"][i] += dist[p]["against"][i]

        # build response bundle (퍼센트화)
        bundle = {
            "sample_games": n_games,

            # Regular Time (W/D/L)
            "regular_time": {
                "win_pct": _pct(w, n_games),
                "draw_pct": _pct(d, n_games),
                "loss_pct": _pct(l, n_games),
            },

            # 3P start score impact (정규시간 결과 분포)
            "third_period_start_impact": {
                "leading": {
                    "win_pct": _pct(lead3_w, lead3_n),
                    "draw_pct": _pct(lead3_d, lead3_n),
                    "loss_pct": _pct(lead3_l, lead3_n),
                    "sample": lead3_n,
                },
                "tied": {
                    "win_pct": _pct(tied3_w, tied3_n),
                    "draw_pct": _pct(tied3_d, tied3_n),
                    "loss_pct": _pct(tied3_l, tied3_n),
                    "sample": tied3_n,
                },
                "trailing": {
                    "win_pct": _pct(trail3_w, trail3_n),
                    "draw_pct": _pct(trail3_d, trail3_n),
                    "loss_pct": _pct(trail3_l, trail3_n),
                    "sample": trail3_n,
                },
            },

            # First goal impact
            "first_goal_impact": {
                "p1_first_goal_for": {
                    "win_pct": _pct(fg1p_w, fg1p_n),
                    "draw_pct": _pct(fg1p_d, fg1p_n),
                    "loss_pct": _pct(fg1p_l, fg1p_n),
                    "sample": fg1p_n,
                },
                "p1_first_goal_conceded": {
                    "win_pct": _pct(cg1p_w, cg1p_n),
                    "draw_pct": _pct(cg1p_d, cg1p_n),
                    "loss_pct": _pct(cg1p_l, cg1p_n),
                    "sample": cg1p_n,
                },
                "p2_start_00_first_goal_for": {
                    "win_pct": _pct(fg2_00_w, fg2_00_n),
                    "draw_pct": _pct(fg2_00_d, fg2_00_n),
                    "loss_pct": _pct(fg2_00_l, fg2_00_n),
                    "sample": fg2_00_n,
                },
                "p2_start_00_first_goal_conceded": {
                    "win_pct": _pct(cg2_00_w, cg2_00_n),
                    "draw_pct": _pct(cg2_00_d, cg2_00_n),
                    "loss_pct": _pct(cg2_00_l, cg2_00_n),
                    "sample": cg2_00_n,
                },
                "p3_start_00_first_goal_for": {
                    "win_pct": _pct(fg3_00_w, fg3_00_n),
                    "draw_pct": _pct(fg3_00_d, fg3_00_n),
                    "loss_pct": _pct(fg3_00_l, fg3_00_n),
                    "sample": fg3_00_n,
                },
                "p3_start_00_first_goal_conceded": {
                    "win_pct": _pct(cg3_00_w, cg3_00_n),
                    "draw_pct": _pct(cg3_00_d, cg3_00_n),
                    "loss_pct": _pct(cg3_00_l, cg3_00_n),
                    "sample": cg3_00_n,
                },
            },

            # Goal timing distribution (2-min buckets)
            "goal_timing_distribution": {
                "bucket_minutes": [0, 2, 4, 6, 8, 10, 12, 14, 16, 18],
                "p1": gtd["P1"],
                "p2": gtd["P2"],
                "p3": gtd["P3"],
            },
        }

        return bundle

    totals_bundle = calc_bundle(totals_games)
    home_bundle = calc_bundle(home_games)
    away_bundle = calc_bundle(away_games)

    return {
        "ok": True,
        "team_id": team_id,
        "filters": {
            "last_n": last_n,
            "season": season,
            "league_id": league_id,
        },
        "samples": {
            "totals": totals_bundle["sample_games"],
            "home": home_bundle["sample_games"],
            "away": away_bundle["sample_games"],
        },
        "totals": totals_bundle,
        "home": home_bundle,
        "away": away_bundle,
        "meta": {
            "source": "db",
            "statuses_used": list(COMPLETED_STATUSES),
            "generated_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    }
