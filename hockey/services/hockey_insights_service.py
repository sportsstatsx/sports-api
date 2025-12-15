# hockey/services/hockey_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all


FINISHED_STATUSES = ("FT", "AOT", "AP")  # matchdetail 서비스와 동일 :contentReference[oaicite:1]{index=1}


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _pct(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return round((n * 100.0) / d, 1)


def _period_rank(p: Any) -> int:
    s = (str(p).strip().upper() if p is not None else "")
    if s == "P1":
        return 1
    if s == "P2":
        return 2
    if s == "P3":
        return 3
    if s == "OT":
        return 4
    return 9


@dataclass
class GameSlice:
    game_id: int
    is_home: bool
    opponent_id: Optional[int]


def _load_last_n_games_for_team(
    team_id: int,
    last_n: int,
) -> List[GameSlice]:
    """
    팀 최근 N경기(상대 무관)
    Totals: 최근 N 전체
    Home: 그 중 team이 home인 경기만
    Away: 그 중 team이 away인 경기만
    """
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.away_team_id
        FROM hockey_games g
        WHERE
            (g.home_team_id = %s OR g.away_team_id = %s)
            AND g.status = ANY(%s)
        ORDER BY g.game_date DESC
        LIMIT %s
    """
    rows = hockey_fetch_all(sql, (team_id, team_id, list(FINISHED_STATUSES), last_n))

    out: List[GameSlice] = []
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        hid = _safe_int(r.get("home_team_id"))
        aid = _safe_int(r.get("away_team_id"))
        if gid is None:
            continue

        is_home = (hid == team_id)
        opp = aid if is_home else hid
        out.append(GameSlice(game_id=gid, is_home=is_home, opponent_id=opp))
    return out


def _load_goal_events_for_games(game_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    game_id -> goal events list
    필요한 최소 필드만 로딩 (period, minute, team_id)
    """
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
            e.comment
        FROM hockey_game_events e
        WHERE
            e.game_id IN ({placeholders})
            AND e.type = 'goal'
        ORDER BY e.game_id ASC, e.period ASC, e.minute ASC NULLS LAST, e.event_order ASC
    """
    rows = hockey_fetch_all(sql, tuple(game_ids))

    mp: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        mp.setdefault(gid, []).append(
            {
                "period": (r.get("period") or "").strip().upper(),
                "minute": _safe_int(r.get("minute")),
                "team_id": _safe_int(r.get("team_id")),
                "comment": r.get("comment"),
            }
        )
    return mp


def _regulation_score_from_events(
    *,
    team_id: int,
    events: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """
    정규시간(P1~P3) 기준 득점/실점
    """
    gf = 0
    ga = 0
    for e in events:
        p = (e.get("period") or "").strip().upper()
        if p not in ("P1", "P2", "P3"):
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        if tid == team_id:
            gf += 1
        else:
            ga += 1
    return gf, ga


def _score_until(
    *,
    team_id: int,
    events: List[Dict[str, Any]],
    up_to_period: str,
    up_to_minute_lt: Optional[int],
) -> Tuple[int, int]:
    """
    특정 시점 '이전'까지(P1~P3만) 스코어 계산.
    - up_to_period = "P3"
    - up_to_minute_lt = 17 이면 P3 minute < 17 까지 포함
    """
    gf = 0
    ga = 0
    for e in events:
        p = (e.get("period") or "").strip().upper()
        if p not in ("P1", "P2", "P3"):
            continue

        if _period_rank(p) < _period_rank(up_to_period):
            pass
        elif p == up_to_period:
            m = _safe_int(e.get("minute"))
            if up_to_minute_lt is not None:
                if m is None:
                    continue
                if m >= up_to_minute_lt:
                    continue
        else:
            continue

        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        if tid == team_id:
            gf += 1
        else:
            ga += 1
    return gf, ga


def _count_goals_in_window(
    *,
    team_id: int,
    events: List[Dict[str, Any]],
    period: str,
    minute_ge: int,
) -> Tuple[int, int]:
    """
    특정 구간(예: P3 minute>=17)에서 득점/실점 카운트
    """
    gf = 0
    ga = 0
    p0 = period.strip().upper()
    for e in events:
        p = (e.get("period") or "").strip().upper()
        if p != p0:
            continue
        m = _safe_int(e.get("minute"))
        if m is None:
            continue
        if m < minute_ge:
            continue

        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        if tid == team_id:
            gf += 1
        else:
            ga += 1
    return gf, ga


def _first_goal_in_period(
    *,
    team_id: int,
    events: List[Dict[str, Any]],
    period: str,
) -> Optional[str]:
    """
    해당 period에서 '첫 골'이 누구였는지:
    - return "for" (team이 넣음)
    - return "against" (상대가 넣음)
    - return None (그 period에 골 없음)
    """
    p0 = period.strip().upper()
    candidates: List[Tuple[int, int, str]] = []
    for e in events:
        p = (e.get("period") or "").strip().upper()
        if p != p0:
            continue
        m = _safe_int(e.get("minute"))
        if m is None:
            continue
        tid = _safe_int(e.get("team_id"))
        if tid is None:
            continue
        who = "for" if tid == team_id else "against"
        candidates.append((m, 0, who))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[0][2]


def _goal_timing_distribution_2min(
    *,
    team_id: int,
    events: List[Dict[str, Any]],
    period: str,
) -> Dict[str, Any]:
    """
    2분 단위(0-1, 2-3, ..., 18-19) 득점 분포
    - team이 넣은 골만 집계
    """
    p0 = period.strip().upper()
    bins = [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13), (14, 15), (16, 17), (18, 19)]
    counts = [0] * len(bins)

    for e in events:
        p = (e.get("period") or "").strip().upper()
        if p != p0:
            continue
        tid = _safe_int(e.get("team_id"))
        if tid != team_id:
            continue
        m = _safe_int(e.get("minute"))
        if m is None:
            continue
        if m < 0 or m > 19:
            continue

        idx = m // 2
        if idx < 0:
            continue
        if idx >= len(counts):
            idx = len(counts) - 1
        counts[idx] += 1

    total = sum(counts)
    out_bins: List[Dict[str, Any]] = []
    for i, (a, b) in enumerate(bins):
        c = counts[i]
        out_bins.append(
            {
                "start_min": a,
                "end_min": b,
                "label": f"{a:02d}-{b:02d}",
                "count": c,
                "pct": _pct(c, total),
            }
        )

    return {
        "period": p0,
        "total_goals": total,
        "bins": out_bins,
    }


def _compute_subset_insights(
    *,
    team_id: int,
    game_ids: List[int],
    events_by_game: Dict[int, List[Dict[str, Any]]],
    last_minutes: int,
) -> Dict[str, Any]:
    """
    subset(=totals/home/away)용 계산 결과
    """
    # last 3 minutes 기본: minute>=17
    last_start_minute = 20 - last_minutes  # 20분 period 기준
    if last_start_minute < 0:
        last_start_minute = 0

    # ---------
    # (1) Last 3 Minutes · 1–2 Goal Margin
    # ---------
    # 분모(상태별 게임수)
    den_lead1 = den_lead2 = den_trail1 = den_trail2 = den_tied = 0
    # score at least 1 in last window
    num_lead1_score = num_lead2_score = num_trail1_score = num_trail2_score = num_tied_score = 0
    num_lead1_conc = num_lead2_conc = num_trail1_conc = num_trail2_conc = num_tied_conc = 0

    # ---------
    # (2) 3rd Period Start Score Impact (Regular Time)
    # ---------
    den_p3_lead = den_p3_tied = den_p3_trail = 0
    num_p3_lead_w = num_p3_lead_d = num_p3_lead_l = 0
    num_p3_tied_w = num_p3_tied_d = num_p3_tied_l = 0
    num_p3_trail_w = num_p3_trail_d = num_p3_trail_l = 0

    # ---------
    # (3) First Goal Impact (Regular Time)
    # ---------
    den_1p_first_for = den_1p_first_against = 0
    num_1p_first_for_w = num_1p_first_for_d = num_1p_first_for_l = 0
    num_1p_first_ag_w = num_1p_first_ag_d = num_1p_first_ag_l = 0

    den_2p_00_first_for = den_2p_00_first_against = 0
    num_2p_00_first_for_w = num_2p_00_first_for_d = num_2p_00_first_for_l = 0
    num_2p_00_first_ag_w = num_2p_00_first_ag_d = num_2p_00_first_ag_l = 0

    den_3p_00_first_for = den_3p_00_first_against = 0
    num_3p_00_first_for_w = num_3p_00_first_for_d = num_3p_00_first_for_l = 0
    num_3p_00_first_ag_w = num_3p_00_first_ag_d = num_3p_00_first_ag_l = 0

    # ---------
    # (4) Goal Timing Distribution (2-Minute Intervals) - P1/P2/P3
    # ---------
    # subset 전체 이벤트를 period별로 합산할 때, "분포는 팀 득점만"이므로
    # game별 계산 후 단순 합산(카운트)하고 pct는 다시 계산
    agg_counts = {
        "P1": [0] * 10,
        "P2": [0] * 10,
        "P3": [0] * 10,
    }

    def _apply_outcome_counts(prefix: str, outcome: str):
        # helper: outcome in ("W","D","L")
        pass

    for gid in game_ids:
        evs = events_by_game.get(gid, [])

        # regulation outcome
        reg_gf, reg_ga = _regulation_score_from_events(team_id=team_id, events=evs)
        if reg_gf > reg_ga:
            outcome = "W"
        elif reg_gf == reg_ga:
            outcome = "D"
        else:
            outcome = "L"

        # ---- (1) last window state at P3 minute < last_start_minute (즉, 17 이전)
        gf_before, ga_before = _score_until(
            team_id=team_id,
            events=evs,
            up_to_period="P3",
            up_to_minute_lt=last_start_minute,
        )
        margin = gf_before - ga_before

        gf_last, ga_last = _count_goals_in_window(
            team_id=team_id,
            events=evs,
            period="P3",
            minute_ge=last_start_minute,
        )
        scored_in_last = gf_last >= 1
        conceded_in_last = ga_last >= 1

        if margin == 1:
            den_lead1 += 1
            if scored_in_last:
                num_lead1_score += 1
            if conceded_in_last:
                num_lead1_conc += 1
        elif margin == 2:
            den_lead2 += 1
            if scored_in_last:
                num_lead2_score += 1
            if conceded_in_last:
                num_lead2_conc += 1
        elif margin == -1:
            den_trail1 += 1
            if scored_in_last:
                num_trail1_score += 1
            if conceded_in_last:
                num_trail1_conc += 1
        elif margin == -2:
            den_trail2 += 1
            if scored_in_last:
                num_trail2_score += 1
            if conceded_in_last:
                num_trail2_conc += 1
        elif margin == 0:
            den_tied += 1
            if scored_in_last:
                num_tied_score += 1
            if conceded_in_last:
                num_tied_conc += 1

        # ---- (2) score at start of P3 (after P2)
        gf_p2, ga_p2 = _score_until(team_id=team_id, events=evs, up_to_period="P3", up_to_minute_lt=0)
        # 위 함수는 P3 minute<0 이므로 사실상 P1+P2만 카운트됨
        margin_p3_start = gf_p2 - ga_p2

        if margin_p3_start > 0:
            den_p3_lead += 1
            if outcome == "W":
                num_p3_lead_w += 1
            elif outcome == "D":
                num_p3_lead_d += 1
            else:
                num_p3_lead_l += 1
        elif margin_p3_start == 0:
            den_p3_tied += 1
            if outcome == "W":
                num_p3_tied_w += 1
            elif outcome == "D":
                num_p3_tied_d += 1
            else:
                num_p3_tied_l += 1
        else:
            den_p3_trail += 1
            if outcome == "W":
                num_p3_trail_w += 1
            elif outcome == "D":
                num_p3_trail_d += 1
            else:
                num_p3_trail_l += 1

        # ---- (3) first goal impact
        # 1P first goal for/against
        who_1p = _first_goal_in_period(team_id=team_id, events=evs, period="P1")
        if who_1p == "for":
            den_1p_first_for += 1
            if outcome == "W":
                num_1p_first_for_w += 1
            elif outcome == "D":
                num_1p_first_for_d += 1
            else:
                num_1p_first_for_l += 1
        elif who_1p == "against":
            den_1p_first_against += 1
            if outcome == "W":
                num_1p_first_ag_w += 1
            elif outcome == "D":
                num_1p_first_ag_d += 1
            else:
                num_1p_first_ag_l += 1

        # 2P start 0-0
        gf_after_p1, ga_after_p1 = _score_until(team_id=team_id, events=evs, up_to_period="P2", up_to_minute_lt=0)
        if gf_after_p1 == 0 and ga_after_p1 == 0:
            who_2p = _first_goal_in_period(team_id=team_id, events=evs, period="P2")
            if who_2p == "for":
                den_2p_00_first_for += 1
                if outcome == "W":
                    num_2p_00_first_for_w += 1
                elif outcome == "D":
                    num_2p_00_first_for_d += 1
                else:
                    num_2p_00_first_for_l += 1
            elif who_2p == "against":
                den_2p_00_first_against += 1
                if outcome == "W":
                    num_2p_00_first_ag_w += 1
                elif outcome == "D":
                    num_2p_00_first_ag_d += 1
                else:
                    num_2p_00_first_ag_l += 1

        # 3P start 0-0
        gf_after_p2, ga_after_p2 = _score_until(team_id=team_id, events=evs, up_to_period="P3", up_to_minute_lt=0)
        if gf_after_p2 == 0 and ga_after_p2 == 0:
            who_3p = _first_goal_in_period(team_id=team_id, events=evs, period="P3")
            if who_3p == "for":
                den_3p_00_first_for += 1
                if outcome == "W":
                    num_3p_00_first_for_w += 1
                elif outcome == "D":
                    num_3p_00_first_for_d += 1
                else:
                    num_3p_00_first_for_l += 1
            elif who_3p == "against":
                den_3p_00_first_against += 1
                if outcome == "W":
                    num_3p_00_first_ag_w += 1
                elif outcome == "D":
                    num_3p_00_first_ag_d += 1
                else:
                    num_3p_00_first_ag_l += 1

        # ---- (4) goal timing distribution 2-min bins (team goals only)
        for e in evs:
            p = (e.get("period") or "").strip().upper()
            if p not in ("P1", "P2", "P3"):
                continue
            tid = _safe_int(e.get("team_id"))
            if tid != team_id:
                continue
            m = _safe_int(e.get("minute"))
            if m is None or m < 0 or m > 19:
                continue
            idx = m // 2
            if idx < 0:
                continue
            if idx >= 10:
                idx = 9
            agg_counts[p][idx] += 1

    # build timing response
    def _build_timing(period: str) -> Dict[str, Any]:
        bins = [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13), (14, 15), (16, 17), (18, 19)]
        counts = agg_counts[period]
        total = sum(counts)
        out_bins: List[Dict[str, Any]] = []
        for i, (a, b) in enumerate(bins):
            c = counts[i]
            out_bins.append(
                {
                    "start_min": a,
                    "end_min": b,
                    "label": f"{a:02d}-{b:02d}",
                    "count": c,
                    "pct": _pct(c, total),
                }
            )
        return {"period": period, "total_goals": total, "bins": out_bins}

    return {
        "sample_size": len(game_ids),

        "last_minutes": last_minutes,
        "last_3_minutes_1_2_goal_margin": {
            "leading_by_1_goal": {
                "den": den_lead1,
                "score_prob_pct": _pct(num_lead1_score, den_lead1),
                "concede_prob_pct": _pct(num_lead1_conc, den_lead1),
            },
            "leading_by_2_goals": {
                "den": den_lead2,
                "score_prob_pct": _pct(num_lead2_score, den_lead2),
                "concede_prob_pct": _pct(num_lead2_conc, den_lead2),
            },
            "trailing_by_1_goal": {
                "den": den_trail1,
                "score_prob_pct": _pct(num_trail1_score, den_trail1),
                "concede_prob_pct": _pct(num_trail1_conc, den_trail1),
            },
            "trailing_by_2_goals": {
                "den": den_trail2,
                "score_prob_pct": _pct(num_trail2_score, den_trail2),
                "concede_prob_pct": _pct(num_trail2_conc, den_trail2),
            },
            "tied_game": {
                "den": den_tied,
                "score_prob_pct": _pct(num_tied_score, den_tied),
                "concede_prob_pct": _pct(num_tied_conc, den_tied),
            },
        },

        "third_period_start_score_impact_regular_time": {
            "leading_at_3p_start": {
                "den": den_p3_lead,
                "win_prob_pct": _pct(num_p3_lead_w, den_p3_lead),
                "draw_prob_pct": _pct(num_p3_lead_d, den_p3_lead),
                "loss_prob_pct": _pct(num_p3_lead_l, den_p3_lead),
            },
            "tied_at_3p_start": {
                "den": den_p3_tied,
                "win_prob_pct": _pct(num_p3_tied_w, den_p3_tied),
                "draw_prob_pct": _pct(num_p3_tied_d, den_p3_tied),
                "loss_prob_pct": _pct(num_p3_tied_l, den_p3_tied),
            },
            "trailing_at_3p_start": {
                "den": den_p3_trail,
                "win_prob_pct": _pct(num_p3_trail_w, den_p3_trail),
                "draw_prob_pct": _pct(num_p3_trail_d, den_p3_trail),
                "loss_prob_pct": _pct(num_p3_trail_l, den_p3_trail),
            },
        },

        "first_goal_impact_regular_time": {
            "p1_first_goal_for": {
                "den": den_1p_first_for,
                "win_prob_pct": _pct(num_1p_first_for_w, den_1p_first_for),
                "draw_prob_pct": _pct(num_1p_first_for_d, den_1p_first_for),
                "loss_prob_pct": _pct(num_1p_first_for_l, den_1p_first_for),
            },
            "p1_first_goal_conceded": {
                "den": den_1p_first_against,
                "win_prob_pct": _pct(num_1p_first_ag_w, den_1p_first_against),
                "draw_prob_pct": _pct(num_1p_first_ag_d, den_1p_first_against),
                "loss_prob_pct": _pct(num_1p_first_ag_l, den_1p_first_against),
            },

            "p2_start_0_0_first_goal_for": {
                "den": den_2p_00_first_for,
                "win_prob_pct": _pct(num_2p_00_first_for_w, den_2p_00_first_for),
                "draw_prob_pct": _pct(num_2p_00_first_for_d, den_2p_00_first_for),
                "loss_prob_pct": _pct(num_2p_00_first_for_l, den_2p_00_first_for),
            },
            "p2_start_0_0_first_goal_conceded": {
                "den": den_2p_00_first_against,
                "win_prob_pct": _pct(num_2p_00_first_ag_w, den_2p_00_first_against),
                "draw_prob_pct": _pct(num_2p_00_first_ag_d, den_2p_00_first_against),
                "loss_prob_pct": _pct(num_2p_00_first_ag_l, den_2p_00_first_against),
            },

            "p3_start_0_0_first_goal_for": {
                "den": den_3p_00_first_for,
                "win_prob_pct": _pct(num_3p_00_first_for_w, den_3p_00_first_for),
                "draw_prob_pct": _pct(num_3p_00_first_for_d, den_3p_00_first_for),
                "loss_prob_pct": _pct(num_3p_00_first_for_l, den_3p_00_first_for),
            },
            "p3_start_0_0_first_goal_conceded": {
                "den": den_3p_00_first_against,
                "win_prob_pct": _pct(num_3p_00_first_ag_w, den_3p_00_first_against),
                "draw_prob_pct": _pct(num_3p_00_first_ag_d, den_3p_00_first_against),
                "loss_prob_pct": _pct(num_3p_00_first_ag_l, den_3p_00_first_against),
            },
        },

        "goal_timing_distribution_2min_intervals": {
            "p1": _build_timing("P1"),
            "p2": _build_timing("P2"),
            "p3": _build_timing("P3"),
        },
    }


def hockey_get_team_insights(
    *,
    team_id: int,
    last_n: int,
    last_minutes: int = 3,
) -> Dict[str, Any]:
    """
    팀 기반 insights (Totals/Home/Away)
    - last_n: 팀 최근 N경기(상대 무관)
    - last_minutes: 기본 3 (minute>=17)
    """
    if team_id <= 0:
        raise ValueError("team_id is required")
    if last_n <= 0:
        last_n = 1
    if last_n > 50:
        last_n = 50
    if last_minutes <= 0:
        last_minutes = 3
    if last_minutes > 20:
        last_minutes = 20

    slices = _load_last_n_games_for_team(team_id=team_id, last_n=last_n)
    game_ids_all = [s.game_id for s in slices]
    events_by_game = _load_goal_events_for_games(game_ids_all)

    # split
    game_ids_home = [s.game_id for s in slices if s.is_home]
    game_ids_away = [s.game_id for s in slices if not s.is_home]

    totals = _compute_subset_insights(
        team_id=team_id,
        game_ids=game_ids_all,
        events_by_game=events_by_game,
        last_minutes=last_minutes,
    )
    home = _compute_subset_insights(
        team_id=team_id,
        game_ids=game_ids_home,
        events_by_game=events_by_game,
        last_minutes=last_minutes,
    )
    away = _compute_subset_insights(
        team_id=team_id,
        game_ids=game_ids_away,
        events_by_game=events_by_game,
        last_minutes=last_minutes,
    )

    return {
        "ok": True,
        "team_id": team_id,
        "last_n": last_n,
        "last_minutes": last_minutes,
        "totals": totals,
        "home": home,
        "away": away,
    }
