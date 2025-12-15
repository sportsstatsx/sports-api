# hockey/services/hockey_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


FINISHED_STATUSES = ("FT", "AOT", "AP")  # 종료 경기만


def _iso_utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _safe_div(n: int, d: int) -> Optional[float]:
    if d <= 0:
        return None
    return float(n) / float(d)


def _norm_period(p: Any) -> str:
    s = (p or "").strip().upper()
    if s in ("P1", "1", "1ST", "1ST PERIOD", "1STPERIOD"):
        return "P1"
    if s in ("P2", "2", "2ND", "2ND PERIOD", "2NDPERIOD"):
        return "P2"
    if s in ("P3", "3", "3RD", "3RD PERIOD", "3RDPERIOD"):
        return "P3"
    if s.startswith("OT") or s in ("OT", "P4"):
        return "OT"
    return s or "UNK"


def _period_index(p: str) -> int:
    if p == "P1":
        return 0
    if p == "P2":
        return 1
    if p == "P3":
        return 2
    if p == "OT":
        return 3
    return 9


def _event_sort_key(ev: Dict[str, Any]) -> Tuple[int, int, int]:
    # minute이 NULL일 수도 있으니 안전 처리
    p = _norm_period(ev.get("period"))
    m = ev.get("minute")
    mi = _safe_int(m)
    if mi is None:
        mi = 10**9
    order = _safe_int(ev.get("event_order")) or 0
    return (_period_index(p), mi, order)


def _score_after_regulation(goal_events: List[Dict[str, Any]], team_id: int) -> Tuple[int, int]:
    # team_for, team_against (P1~P3만)
    gf = 0
    ga = 0
    for ev in goal_events:
        p = _norm_period(ev.get("period"))
        if p not in ("P1", "P2", "P3"):
            continue
        tid = _safe_int(ev.get("team_id"))
        if tid is None:
            continue
        if tid == team_id:
            gf += 1
        else:
            ga += 1
    return gf, ga


def _score_at_checkpoint(
    goal_events: List[Dict[str, Any]],
    team_id: int,
    checkpoint: Tuple[str, int],
) -> Tuple[int, int]:
    """
    checkpoint = (period, minute_start)
    예) ("P3", 17) -> P3 17:00 시작 시점 스코어 (P3 minute < 17 까지만 반영)
    """
    cp_period, cp_min = checkpoint
    gf = 0
    ga = 0

    for ev in goal_events:
        p = _norm_period(ev.get("period"))
        tid = _safe_int(ev.get("team_id"))
        mi = _safe_int(ev.get("minute"))
        if tid is None or mi is None:
            continue

        if p in ("P1", "P2"):
            if tid == team_id:
                gf += 1
            else:
                ga += 1
            continue

        if p == cp_period:
            if mi < cp_min:
                if tid == team_id:
                    gf += 1
                else:
                    ga += 1

    return gf, ga


def _first_goal_of_game(goal_events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    reg = [ev for ev in goal_events if _norm_period(ev.get("period")) in ("P1", "P2", "P3")]
    if not reg:
        return None
    reg.sort(key=_event_sort_key)
    return reg[0]


def _first_goal_after_checkpoint(
    goal_events: List[Dict[str, Any]],
    checkpoint_periods: Tuple[str, ...],
) -> Optional[Dict[str, Any]]:
    cand = []
    for ev in goal_events:
        p = _norm_period(ev.get("period"))
        if p in checkpoint_periods:
            cand.append(ev)
    if not cand:
        return None
    cand.sort(key=_event_sort_key)
    return cand[0]


@dataclass
class _Bucket:
    games: List[int]
    # game_id -> (is_home_for_selected, opponent_team_id)
    home_flags: Dict[int, bool]


def _load_recent_games(team_id: int, last_n: int) -> _Bucket:
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.away_team_id
        FROM hockey_games g
        WHERE
            g.status = ANY(%s)
            AND (g.home_team_id = %s OR g.away_team_id = %s)
        ORDER BY g.game_date DESC NULLS LAST, g.id DESC
        LIMIT %s
    """
    rows = hockey_fetch_all(sql, (list(FINISHED_STATUSES), team_id, team_id, last_n))

    games: List[int] = []
    home_flags: Dict[int, bool] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        games.append(gid)
        home_flags[gid] = (_safe_int(r.get("home_team_id")) == team_id)

    return _Bucket(games=games, home_flags=home_flags)


def _load_goal_events(game_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not game_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(game_ids))
    sql = f"""
        SELECT
            e.game_id,
            e.period,
            e.minute,
            e.team_id,
            e.event_order
        FROM hockey_game_events e
        WHERE
            e.type = 'goal'
            AND e.game_id IN ({placeholders})
        ORDER BY e.game_id ASC, e.period ASC, e.minute ASC NULLS LAST, e.event_order ASC
    """
    rows = hockey_fetch_all(sql, tuple(game_ids))

    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        out.setdefault(gid, []).append(r)
    return out


def _triple(values_by_bucket: Dict[str, Optional[float]]) -> Dict[str, Any]:
    return {
        "totals": values_by_bucket.get("totals"),
        "home": values_by_bucket.get("home"),
        "away": values_by_bucket.get("away"),
    }


def _build_section(title: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "title": title,
        "columns": ["Totals", "Home", "Away"],
        "rows": rows,
    }


def hockey_get_game_insights(
    game_id: int,
    team_id: Optional[int] = None,
    last_n: int = 10,
    last_minutes: int = 3,
) -> Dict[str, Any]:
    # 0) game 존재 확인 + 기본 team_id 결정
    g = hockey_fetch_one(
        """
        SELECT id, league_id, season, home_team_id, away_team_id
        FROM hockey_games
        WHERE id = %s
        LIMIT 1
        """,
        (game_id,),
    )
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    default_team_id = _safe_int(g.get("home_team_id"))
    sel_team_id = _safe_int(team_id) or default_team_id
    if sel_team_id is None:
        # 팀이 NULL이면 인사이트 계산 불가
        return {"ok": True, "game_id": game_id, "sections": [], "meta": {"reason": "TEAM_ID_MISSING"}}

    if last_n < 1:
        last_n = 1
    if last_n > 50:
        last_n = 50  # 서버 보호

    if last_minutes < 1:
        last_minutes = 1
    if last_minutes > 10:
        last_minutes = 10

    # 너 확정 룰: 20분 기준 (P3 minute >= 17이면 last 3min)
    threshold_minute = 20 - last_minutes  # 기본 17

    bucket = _load_recent_games(sel_team_id, last_n)
    game_ids = bucket.games

    goal_by_game = _load_goal_events(game_ids)

    # bucket별 game_ids
    totals_ids = list(game_ids)
    home_ids = [gid for gid in game_ids if bucket.home_flags.get(gid) is True]
    away_ids = [gid for gid in game_ids if bucket.home_flags.get(gid) is False]

    def iter_bucket(name: str) -> List[int]:
        if name == "totals":
            return totals_ids
        if name == "home":
            return home_ids
        return away_ids

    # 공통: 결과(정규시간) 구하기
    def reg_result_for_game(gid: int) -> Optional[str]:
        evs = goal_by_game.get(gid, [])
        gf, ga = _score_after_regulation(evs, sel_team_id)
        if gf > ga:
            return "W"
        if gf < ga:
            return "L"
        return "D"

    # ─────────────────────────────────────────
    # A) Last 3 Minutes · 1–2 Goal Margin
    # ─────────────────────────────────────────
    def last_minutes_probs(state: str) -> Dict[str, Optional[float]]:
        """
        state:
          LEAD1/LEAD2/TRAIL1/TRAIL2/TIED
        return: {totals/home/away: prob}
        prob = (#games where condition met AND (scored OR conceded))/ #games where condition met
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num_score = 0
            num_concede = 0

            for gid in ids:
                evs = goal_by_game.get(gid, [])
                # 체크포인트: P3 17:00 (last 3min 시작) 시점 스코어
                gf0, ga0 = _score_at_checkpoint(evs, sel_team_id, ("P3", threshold_minute))
                diff0 = gf0 - ga0

                ok = False
                if state == "LEAD1" and diff0 == 1:
                    ok = True
                elif state == "LEAD2" and diff0 == 2:
                    ok = True
                elif state == "TRAIL1" and diff0 == -1:
                    ok = True
                elif state == "TRAIL2" and diff0 == -2:
                    ok = True
                elif state == "TIED" and diff0 == 0:
                    ok = True

                if not ok:
                    continue

                denom += 1

                # last minutes window: P3 minute >= threshold_minute
                scored = False
                conceded = False
                for ev in evs:
                    if _norm_period(ev.get("period")) != "P3":
                        continue
                    mi = _safe_int(ev.get("minute"))
                    if mi is None or mi < threshold_minute:
                        continue
                    tid = _safe_int(ev.get("team_id"))
                    if tid is None:
                        continue
                    if tid == sel_team_id:
                        scored = True
                    else:
                        conceded = True

                if scored:
                    num_score += 1
                if conceded:
                    num_concede += 1

            # 이 함수는 “Score/Concede”를 나눠서 쓰려고,
            # 여기선 임시로 dict에 담지 않고 호출부에서 분리 계산할 것.
            # (아래에서 별도 함수로 쓴다)
            out[b] = None
        return out

    def last_minutes_score_prob(state: str) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num = 0
            for gid in ids:
                evs = goal_by_game.get(gid, [])
                gf0, ga0 = _score_at_checkpoint(evs, sel_team_id, ("P3", threshold_minute))
                diff0 = gf0 - ga0

                ok = False
                if state == "LEAD1" and diff0 == 1:
                    ok = True
                elif state == "LEAD2" and diff0 == 2:
                    ok = True
                elif state == "TRAIL1" and diff0 == -1:
                    ok = True
                elif state == "TRAIL2" and diff0 == -2:
                    ok = True
                elif state == "TIED" and diff0 == 0:
                    ok = True
                if not ok:
                    continue

                denom += 1
                scored = False
                for ev in evs:
                    if _norm_period(ev.get("period")) != "P3":
                        continue
                    mi = _safe_int(ev.get("minute"))
                    if mi is None or mi < threshold_minute:
                        continue
                    tid = _safe_int(ev.get("team_id"))
                    if tid == sel_team_id:
                        scored = True
                        break
                if scored:
                    num += 1
            out[b] = _safe_div(num, denom)
        return out

    def last_minutes_concede_prob(state: str) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num = 0
            for gid in ids:
                evs = goal_by_game.get(gid, [])
                gf0, ga0 = _score_at_checkpoint(evs, sel_team_id, ("P3", threshold_minute))
                diff0 = gf0 - ga0

                ok = False
                if state == "LEAD1" and diff0 == 1:
                    ok = True
                elif state == "LEAD2" and diff0 == 2:
                    ok = True
                elif state == "TRAIL1" and diff0 == -1:
                    ok = True
                elif state == "TRAIL2" and diff0 == -2:
                    ok = True
                elif state == "TIED" and diff0 == 0:
                    ok = True
                if not ok:
                    continue

                denom += 1
                conceded = False
                for ev in evs:
                    if _norm_period(ev.get("period")) != "P3":
                        continue
                    mi = _safe_int(ev.get("minute"))
                    if mi is None or mi < threshold_minute:
                        continue
                    tid = _safe_int(ev.get("team_id"))
                    if tid is None:
                        continue
                    if tid != sel_team_id:
                        conceded = True
                        break
                if conceded:
                    num += 1
            out[b] = _safe_div(num, denom)
        return out

    sec_last = _build_section(
        "Last 3 Minutes · 1–2 Goal Margin",
        rows=[
            {
                "label": "Leading by 1 Goal – Score Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_score_prob("LEAD1")),
            },
            {
                "label": "Leading by 1 Goal – Concede Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_concede_prob("LEAD1")),
            },
            {
                "label": "Leading by 2 Goals – Score Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_score_prob("LEAD2")),
            },
            {
                "label": "Leading by 2 Goals – Concede Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_concede_prob("LEAD2")),
            },
            {
                "label": "Trailing by 1 Goal – Score Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_score_prob("TRAIL1")),
            },
            {
                "label": "Trailing by 1 Goal – Concede Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_concede_prob("TRAIL1")),
            },
            {
                "label": "Trailing by 2 Goals – Score Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_score_prob("TRAIL2")),
            },
            {
                "label": "Trailing by 2 Goals – Concede Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_concede_prob("TRAIL2")),
            },
            {
                "label": "Tied Game – Score Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_score_prob("TIED")),
            },
            {
                "label": "Tied Game – Concede Probability (Last 3 Minutes)",
                "values": _triple(last_minutes_concede_prob("TIED")),
            },
        ],
    )

    # ─────────────────────────────────────────
    # B) 3rd Period Start Score Impact (Regular Time)
    # ─────────────────────────────────────────
    def p3_start_state_prob(state: str, outcome: str) -> Dict[str, Optional[float]]:
        """
        state: LEAD/TIED/TRAIL at start of P3 (after P2)
        outcome: W/D/L at end of regulation
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num = 0
            for gid in ids:
                evs = goal_by_game.get(gid, [])
                # P3 시작 = P1,P2까지만 반영
                # checkpoint를 ("P3", 0) 로 보면 P1,P2는 다 포함되고 P3 minute<0 없음
                gf0, ga0 = _score_at_checkpoint(evs, sel_team_id, ("P3", 0))
                diff0 = gf0 - ga0

                ok = False
                if state == "LEAD" and diff0 > 0:
                    ok = True
                elif state == "TIED" and diff0 == 0:
                    ok = True
                elif state == "TRAIL" and diff0 < 0:
                    ok = True
                if not ok:
                    continue

                denom += 1
                res = reg_result_for_game(gid)
                if res == outcome:
                    num += 1
            out[b] = _safe_div(num, denom)
        return out

    sec_p3 = _build_section(
        "3rd Period Start Score Impact (Regular Time)",
        rows=[
            {"label": "Leading at 3P Start → Win Probability", "values": _triple(p3_start_state_prob("LEAD", "W"))},
            {"label": "Leading at 3P Start → Draw Probability", "values": _triple(p3_start_state_prob("LEAD", "D"))},
            {"label": "Leading at 3P Start → Loss Probability", "values": _triple(p3_start_state_prob("LEAD", "L"))},
            {"label": "Tied at 3P Start → Win Probability", "values": _triple(p3_start_state_prob("TIED", "W"))},
            {"label": "Tied at 3P Start → Draw Probability", "values": _triple(p3_start_state_prob("TIED", "D"))},
            {"label": "Tied at 3P Start → Loss Probability", "values": _triple(p3_start_state_prob("TIED", "L"))},
            {"label": "Trailing at 3P Start → Win Probability", "values": _triple(p3_start_state_prob("TRAIL", "W"))},
            {"label": "Trailing at 3P Start → Draw Probability", "values": _triple(p3_start_state_prob("TRAIL", "D"))},
            {"label": "Trailing at 3P Start → Loss Probability", "values": _triple(p3_start_state_prob("TRAIL", "L"))},
        ],
    )

    # ─────────────────────────────────────────
    # C) First Goal Impact (Regular Time)
    # ─────────────────────────────────────────
    def first_goal_condition_prob(cond: str, outcome: str) -> Dict[str, Optional[float]]:
        """
        cond:
          - P1_FIRST_FOR / P1_FIRST_AGAINST
          - P2_00_FIRST_FOR / P2_00_FIRST_AGAINST (P1 0-0)
          - P3_00_FIRST_FOR / P3_00_FIRST_AGAINST (P1+P2 0-0)
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num = 0
            for gid in ids:
                evs = goal_by_game.get(gid, [])
                # 결과
                res = reg_result_for_game(gid)
                if res is None:
                    continue

                ok = False

                if cond in ("P1_FIRST_FOR", "P1_FIRST_AGAINST"):
                    fg = _first_goal_of_game(evs)
                    if not fg:
                        continue
                    if _norm_period(fg.get("period")) != "P1":
                        continue
                    tid = _safe_int(fg.get("team_id"))
                    if tid is None:
                        continue
                    if cond == "P1_FIRST_FOR" and tid == sel_team_id:
                        ok = True
                    if cond == "P1_FIRST_AGAINST" and tid != sel_team_id:
                        ok = True

                elif cond in ("P2_00_FIRST_FOR", "P2_00_FIRST_AGAINST"):
                    # P1 0-0
                    gf1 = 0
                    ga1 = 0
                    for ev in evs:
                        if _norm_period(ev.get("period")) != "P1":
                            continue
                        tid = _safe_int(ev.get("team_id"))
                        if tid is None:
                            continue
                        if tid == sel_team_id:
                            gf1 += 1
                        else:
                            ga1 += 1
                    if (gf1 + ga1) != 0:
                        continue

                    fg2 = _first_goal_after_checkpoint(evs, ("P2", "P3"))
                    if not fg2:
                        continue
                    tid = _safe_int(fg2.get("team_id"))
                    if tid is None:
                        continue
                    if cond == "P2_00_FIRST_FOR" and tid == sel_team_id:
                        ok = True
                    if cond == "P2_00_FIRST_AGAINST" and tid != sel_team_id:
                        ok = True

                elif cond in ("P3_00_FIRST_FOR", "P3_00_FIRST_AGAINST"):
                    # P1+P2 0-0
                    gf12, ga12 = _score_at_checkpoint(evs, sel_team_id, ("P3", 0))
                    if (gf12 + ga12) != 0:
                        continue

                    fg3 = _first_goal_after_checkpoint(evs, ("P3",))
                    if not fg3:
                        continue
                    tid = _safe_int(fg3.get("team_id"))
                    if tid is None:
                        continue
                    if cond == "P3_00_FIRST_FOR" and tid == sel_team_id:
                        ok = True
                    if cond == "P3_00_FIRST_AGAINST" and tid != sel_team_id:
                        ok = True

                if not ok:
                    continue

                denom += 1
                if res == outcome:
                    num += 1

            out[b] = _safe_div(num, denom)
        return out

    sec_fg = _build_section(
        "First Goal Impact (Regular Time)",
        rows=[
            {"label": "1P First Goal → Win Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_FOR", "W"))},
            {"label": "1P First Goal → Draw Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_FOR", "D"))},
            {"label": "1P First Goal → Loss Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_FOR", "L"))},

            {"label": "1P Conceded First Goal → Win Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_AGAINST", "W"))},
            {"label": "1P Conceded First Goal → Draw Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_AGAINST", "D"))},
            {"label": "1P Conceded First Goal → Loss Probability", "values": _triple(first_goal_condition_prob("P1_FIRST_AGAINST", "L"))},

            {"label": "2P Start 0–0 → First Goal → Win Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_FOR", "W"))},
            {"label": "2P Start 0–0 → First Goal → Draw Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_FOR", "D"))},
            {"label": "2P Start 0–0 → First Goal → Loss Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_FOR", "L"))},

            {"label": "2P Start 0–0 → First Goal Conceded → Win Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_AGAINST", "W"))},
            {"label": "2P Start 0–0 → First Goal Conceded → Draw Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_AGAINST", "D"))},
            {"label": "2P Start 0–0 → First Goal Conceded → Loss Probability", "values": _triple(first_goal_condition_prob("P2_00_FIRST_AGAINST", "L"))},

            {"label": "3P Start 0–0 → First Goal → Win Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_FOR", "W"))},
            {"label": "3P Start 0–0 → First Goal → Draw Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_FOR", "D"))},
            {"label": "3P Start 0–0 → First Goal → Loss Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_FOR", "L"))},

            {"label": "3P Start 0–0 → First Goal Conceded → Win Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_AGAINST", "W"))},
            {"label": "3P Start 0–0 → First Goal Conceded → Draw Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_AGAINST", "D"))},
            {"label": "3P Start 0–0 → First Goal Conceded → Loss Probability", "values": _triple(first_goal_condition_prob("P3_00_FIRST_AGAINST", "L"))},
        ],
    )

    # ─────────────────────────────────────────
    # D) Goal Timing Distribution (2-Minute Intervals)
    #    - 앱에서 게이지바 그릴 수 있게 “분포(합=1)”로 내려줌
    # ─────────────────────────────────────────
    def goal_timing_distribution(period: str) -> Dict[str, List[Optional[float]]]:
        """
        return {totals:[10], home:[10], away:[10]}
        각 버킷은 해당 period에서 나온 '선택팀 득점'을 2분 구간으로 나눈 비율
        """
        out: Dict[str, List[Optional[float]]] = {}

        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)

            counts = [0] * 10  # 0-2,2-4,...,18-20
            total_goals = 0

            for gid in ids:
                evs = goal_by_game.get(gid, [])
                for ev in evs:
                    if _norm_period(ev.get("period")) != period:
                        continue
                    tid = _safe_int(ev.get("team_id"))
                    mi = _safe_int(ev.get("minute"))
                    if tid != sel_team_id or mi is None:
                        continue
                    # minute 0~19 가정 (너가 확인한 raw_json minute="00")
                    idx = mi // 2
                    if 0 <= idx <= 9:
                        counts[idx] += 1
                        total_goals += 1

            if total_goals <= 0:
                out[b] = [None] * 10
            else:
                out[b] = [c / total_goals for c in counts]

        return out

    def _interval_label(i: int) -> str:
        start = i * 2
        end = start + 2
        return f"{start:02d}–{end:02d}"

    def _dist_rows(period: str, title_prefix: str) -> List[Dict[str, Any]]:
        dist = goal_timing_distribution(period)
        rows: List[Dict[str, Any]] = []
        for i in range(10):
            rows.append(
                {
                    "label": f"{title_prefix} { _interval_label(i) }",
                    "values": {
                        "totals": dist["totals"][i],
                        "home": dist["home"][i],
                        "away": dist["away"][i],
                    },
                }
            )
        return rows

    sec_goal_time = _build_section(
        "Goal Timing Distribution",
        rows=(
            _dist_rows("P1", "1st Period (2-min)") +
            _dist_rows("P2", "2nd Period (2-min)") +
            _dist_rows("P3", "3rd Period (2-min)")
        ),
    )

    sections = [sec_last, sec_p3, sec_fg, sec_goal_time]

    return {
        "ok": True,
        "game_id": game_id,
        "team_id": sel_team_id,
        "last_n": last_n,
        "sections": sections,
        "meta": {
            "source": "db",
            "finished_statuses": list(FINISHED_STATUSES),
            "last_minutes": last_minutes,
            "threshold_minute": threshold_minute,
            "generated_at": _iso_utc_now(),
            "sample_sizes": {
                "totals": len(totals_ids),
                "home": len(home_ids),
                "away": len(away_ids),
            },
        },
    }
