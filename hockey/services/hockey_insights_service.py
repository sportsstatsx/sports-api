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


def _load_recent_games(team_id: int, last_n: int, league_id: Optional[int] = None) -> _Bucket:
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.away_team_id
        FROM hockey_games g
        WHERE
            g.status = ANY(%s)
            AND (g.home_team_id = %s OR g.away_team_id = %s)
    """
    params: List[Any] = [list(FINISHED_STATUSES), team_id, team_id]

    if league_id is not None:
        sql += " AND g.league_id = %s\n"
        params.append(league_id)

    sql += """
        ORDER BY g.game_date DESC NULLS LAST, g.id DESC
        LIMIT %s
    """
    params.append(last_n)

    rows = hockey_fetch_all(sql, tuple(params))

    games: List[int] = []
    home_flags: Dict[int, bool] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        games.append(gid)
        home_flags[gid] = (_safe_int(r.get("home_team_id")) == team_id)

    return _Bucket(games=games, home_flags=home_flags)

def _load_available_seasons_for_league(league_id: int, limit: int = 2) -> List[int]:
    sql = """
        SELECT DISTINCT g.season
        FROM hockey_games g
        WHERE
            g.league_id = %s
            AND g.status = ANY(%s)
            AND g.season IS NOT NULL
        ORDER BY g.season DESC
        LIMIT %s
    """
    rows = hockey_fetch_all(sql, (league_id, list(FINISHED_STATUSES), limit))
    seasons: List[int] = []
    for r in rows:
        y = _safe_int(r.get("season"))
        if y is not None:
            seasons.append(y)
    return seasons


def _load_games_for_season(team_id: int, league_id: int, season: int) -> _Bucket:
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.away_team_id
        FROM hockey_games g
        WHERE
            g.status = ANY(%s)
            AND g.league_id = %s
            AND g.season = %s
            AND (g.home_team_id = %s OR g.away_team_id = %s)
        ORDER BY g.game_date DESC NULLS LAST, g.id DESC
        LIMIT 5000
    """
    rows = hockey_fetch_all(
        sql,
        (list(FINISHED_STATUSES), league_id, season, team_id, team_id),
    )

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

def _load_all_events(game_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    goal + penalty 이벤트를 전부 로딩 (comment 포함)
    - Full Time/Period 지표: goal/penalty/comment 기반으로 계산
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
            e.comment,
            e.event_order
        FROM hockey_game_events e
        WHERE
            e.type IN ('goal', 'penalty')
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


def _build_section(
    title: str,
    rows: List[Dict[str, Any]],
    counts: Optional[Dict[str, int]] = None,
    subtitle: Optional[str] = None,  # ✅ 추가
) -> Dict[str, Any]:
    out = {
        "title": title,
        "columns": ["Totals", "Home", "Away"],
        "rows": rows,
    }

    # ✅ subtitle 있으면 내려보냄 (앱에서 바로 표시 가능)
    if subtitle is not None and str(subtitle).strip() != "":
        out["subtitle"] = str(subtitle)

    if counts is not None:
        t = int(counts.get("totals", 0))
        h = int(counts.get("home", 0))
        a = int(counts.get("away", 0))

        # ✅ 기본: counts(totals/home/away)
        out["counts"] = {
            "totals": t,
            "total": t,   # ✅ 호환(혹시 total을 기대하는 앱 대비)
            "home": h,
            "away": a,
        }

        # ✅ 추가 호환: count(total/home/away) 형태를 기대하는 앱 대비
        out["count"] = {
            "total": t,
            "home": h,
            "away": a,
        }

    return out





def hockey_get_game_insights(
    game_id: int,
    team_id: Optional[int] = None,
    last_n: int = 10,
    last_minutes: int = 3,
    season: Optional[int] = None,  # ✅ 추가
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

    league_id = _safe_int(g.get("league_id"))
    if league_id is None:
        # league_id 없으면 기존 동작(최악의 경우라도 동작은 하게)
        bucket = _load_recent_games(sel_team_id, last_n)
        available_seasons: List[int] = []
        mode = "last_n"
    else:
        available_seasons = _load_available_seasons_for_league(league_id, limit=2)

        if season is not None:
            mode = "season"
            bucket = _load_games_for_season(sel_team_id, league_id, season)
        else:
            mode = "last_n"
            bucket = _load_recent_games(sel_team_id, last_n, league_id=league_id)

    game_ids = bucket.games

    goal_by_game = _load_goal_events(game_ids)
    all_events_by_game = _load_all_events(game_ids)

    # games 메타(홈/원정/상태/최종스코어) 로딩: OT/SO 판정 + 상대팀 id 필요
    game_meta: Dict[int, Dict[str, Any]] = {}
    if game_ids:
        placeholders = ", ".join(["%s"] * len(game_ids))
        sql = f"""
            SELECT
                id,
                home_team_id,
                away_team_id,
                status,
                score_json
            FROM hockey_games
            WHERE id IN ({placeholders})
        """
        rows = hockey_fetch_all(sql, tuple(game_ids))
        for r in rows:
            gid = _safe_int(r.get("id"))
            if gid is None:
                continue
            game_meta[gid] = r

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

    def _count_last3_state(state: str) -> Dict[str, int]:
        """
        섹션 헤더에 표시할 분모 경기수(T/H/A)
        = 해당 bucket 경기들 중에서
          '3P 남은 3:00 시점(=P3 minute<threshold_minute) 스코어차(diff0)'가
          state에 해당하는 경기 수
        """
        out: Dict[str, int] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
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

                if ok:
                    denom += 1

            out[b] = denom
        return out

    def _last3_title(state_label: str) -> str:
        # 섹션 타이틀(축약형)
        return f"3P Clutch · L3 · {state_label}"

    # state 라벨 매핑(표기용)
    _STATE_LABEL = {
        "LEAD1": "Lead1",
        "LEAD2": "Lead2",
        "TRAIL1": "Trail1",
        "TRAIL2": "Trail2",
        "TIED": "Tied",
    }

    def _build_last3_section(state: str) -> Dict[str, Any]:
        lab = _STATE_LABEL.get(state, state)
        cnt = _count_last3_state(state)

        return _build_section(
            title=_last3_title(lab),
            counts=cnt,  # ✅ 헤더 T/H/A 경기수
            subtitle=f"T={cnt['totals']} / H={cnt['home']} / A={cnt['away']}",  # ✅ 앱 fallback용
            rows=[
                {"label": f"L3 · {lab} · Score",   "values": _triple(last_minutes_score_prob(state))},
                {"label": f"L3 · {lab} · Concede", "values": _triple(last_minutes_concede_prob(state))},
            ],
        )


    sec_last_lead1 = _build_last3_section("LEAD1")
    sec_last_lead2 = _build_last3_section("LEAD2")
    sec_last_trail1 = _build_last3_section("TRAIL1")
    sec_last_trail2 = _build_last3_section("TRAIL2")
    sec_last_tied  = _build_last3_section("TIED")



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
    # D) Goal Timing Distribution (2-Minute Intervals)
    # ─────────────────────────────────────────
    def goal_timing_distribution(period: str) -> Dict[str, List[Optional[float]]]:
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

    # ─────────────────────────────────────────
    # NEW) 공통 계산 유틸 (Full Time / Period / OT/SO / Transition)
    # ─────────────────────────────────────────
    REG_PERIODS = ("P1", "P2", "P3")

    def _is_reg_period(p: Any) -> bool:
        return _norm_period(p) in REG_PERIODS

    def _team_and_opp_ids(gid: int) -> Tuple[Optional[int], Optional[int]]:
        gm = game_meta.get(gid) or {}
        h = _safe_int(gm.get("home_team_id"))
        a = _safe_int(gm.get("away_team_id"))
        if h is None or a is None:
            return None, None
        if sel_team_id == h:
            return h, a
        if sel_team_id == a:
            return a, h
        return sel_team_id, (a if h == sel_team_id else h)

    def _reg_scores(gid: int) -> Tuple[int, int]:
        evs = goal_by_game.get(gid, [])
        gf, ga = _score_after_regulation(evs, sel_team_id)
        return gf, ga

    def _period_scores(gid: int, period: str) -> Tuple[int, int]:
        evs = goal_by_game.get(gid, [])
        gf = 0
        ga = 0
        p = _norm_period(period)
        for ev in evs:
            if _norm_period(ev.get("period")) != p:
                continue
            tid = _safe_int(ev.get("team_id"))
            if tid is None:
                continue
            if tid == sel_team_id:
                gf += 1
            else:
                ga += 1
        return gf, ga

    def _result_from_scores(gf: int, ga: int) -> str:
        if gf > ga:
            return "W"
        if gf < ga:
            return "L"
        return "D"

    def _bool_prob(predicate) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = len(ids)
            if denom <= 0:
                out[b] = None
                continue
            num = 0
            for gid in ids:
                if predicate(gid):
                    num += 1
            out[b] = num / denom
        return out

    def _count_ge_prob(threshold: int, getter) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = len(ids)
            if denom <= 0:
                out[b] = None
                continue
            num = 0
            for gid in ids:
                v = getter(gid)
                if v >= threshold:
                    num += 1
            out[b] = num / denom
        return out

    def _evs(gid: int) -> List[Dict[str, Any]]:
        return all_events_by_game.get(gid, [])

    def _penalty_count_by_team_reg(gid: int, team: int) -> int:
        c = 0
        for ev in _evs(gid):
            if ev.get("type") != "penalty":
                continue
            if not _is_reg_period(ev.get("period")):
                continue
            tid = _safe_int(ev.get("team_id"))
            if tid == team:
                c += 1
        return c

    def _goal_count_by_comment_reg(gid: int, team: int, keyword: str) -> int:
        kw = (keyword or "").strip().lower()
        c = 0
        for ev in _evs(gid):
            if ev.get("type") != "goal":
                continue
            if not _is_reg_period(ev.get("period")):
                continue
            tid = _safe_int(ev.get("team_id"))
            if tid != team:
                continue
            com = (ev.get("comment") or "").strip().lower()
            if kw and kw in com:
                c += 1
        return c

    def _penalty_count_by_team_period(gid: int, team: int, period: str) -> int:
        p = _norm_period(period)
        c = 0
        for ev in _evs(gid):
            if ev.get("type") != "penalty":
                continue
            if _norm_period(ev.get("period")) != p:
                continue
            tid = _safe_int(ev.get("team_id"))
            if tid == team:
                c += 1
        return c

    def _goal_count_by_comment_period(gid: int, team: int, keyword: str, period: str) -> int:
        p = _norm_period(period)
        kw = (keyword or "").strip().lower()
        c = 0
        for ev in _evs(gid):
            if ev.get("type") != "goal":
                continue
            if _norm_period(ev.get("period")) != p:
                continue
            tid = _safe_int(ev.get("team_id"))
            if tid != team:
                continue
            com = (ev.get("comment") or "").strip().lower()
            if kw and kw in com:
                c += 1
        return c


    def _first_goal_scored_by_team(gid: int, team: int, period: Optional[str] = None) -> Optional[bool]:
        evs = goal_by_game.get(gid, [])
        if not evs:
            return None
        if period is not None:
            p = _norm_period(period)
            pevs = [e for e in evs if _norm_period(e.get("period")) == p]
            if not pevs:
                return None
            first = min(pevs, key=_event_sort_key)
        else:
            first = min(evs, key=_event_sort_key)

        tid = _safe_int(first.get("team_id"))
        if tid is None:
            return None
        return tid == team

    def _avg_by_bucket(get_count) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            n = len(ids)
            if n <= 0:
                out[b] = None
                continue
            s = 0
            for gid in ids:
                s += int(get_count(gid))
            out[b] = float(s) / float(n)
        return out

    def _rate_by_bucket(get_num, get_den) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            num_sum = 0
            den_sum = 0
            for gid in ids:
                num_sum += int(get_num(gid))
                den_sum += int(get_den(gid))
            out[b] = _safe_div(num_sum, den_sum)  # den_sum==0 -> None
        return out

    # ─────────────────────────────────────────
    # NEW) 섹션: Full Time (Regular Time)  ✅ 네가 준 항목(AVG/Rate) 포함
    # ─────────────────────────────────────────
    def _ft_result_prob(result: str) -> Dict[str, Optional[float]]:
        def pred(gid: int) -> bool:
            gf, ga = _reg_scores(gid)
            return _result_from_scores(gf, ga) == result
        return _bool_prob(pred)

    def _ft_team_goals(gid: int) -> int:
        gf, _ = _reg_scores(gid)
        return gf

    def _ft_opp_goals(gid: int) -> int:
        _, ga = _reg_scores(gid)
        return ga

    def _ft_total_goals(gid: int) -> int:
        gf, ga = _reg_scores(gid)
        return gf + ga

    def _ft_first_goal_scored_prob() -> Dict[str, Optional[float]]:
        """
        정규시간(P1~P3)에서 '첫 득점' 팀이 sel_team_id 인 비율.
        ✅ 정규시간 0-0(첫 득점 없음) 경기도 분모(N)에 포함한다.
           - 즉, 분모 = bucket 경기 수
           - 0-0 경기는 '우리가 선제득점 못함'으로 처리(분자 증가 없음)
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = len(ids)
            if denom <= 0:
                out[b] = None
                continue

            num = 0
            for gid in ids:
                evs = goal_by_game.get(gid, [])
                fg = _first_goal_of_game(evs)  # P1~P3만, 없으면 정규시간 0-0
                if not fg:
                    continue  # ✅ 분모에는 포함, 분자는 증가 안함(=0으로 반영)
                tid = _safe_int(fg.get("team_id"))
                if tid == sel_team_id:
                    num += 1

            out[b] = _safe_div(num, denom)
        return out


    def _ft_pp_opportunities(gid: int) -> int:
        # 우리 PP 기회 = 상대 팀 penalty 수 (정규시간)
        _, opp = _team_and_opp_ids(gid)
        if opp is None:
            return 0
        return _penalty_count_by_team_reg(gid, opp)

    def _ft_pk_opportunities(gid: int) -> int:
        # 우리 PK 기회 = 우리 penalty 수 (정규시간)
        return _penalty_count_by_team_reg(gid, sel_team_id)

    def _ft_team_pp_goals(gid: int) -> int:
        return _goal_count_by_comment_reg(gid, sel_team_id, "power")

    def _ft_team_sh_goals(gid: int) -> int:
        return _goal_count_by_comment_reg(gid, sel_team_id, "short")

    def _ft_opp_pp_goals(gid: int) -> int:
        _, opp = _team_and_opp_ids(gid)
        if opp is None:
            return 0
        return _goal_count_by_comment_reg(gid, opp, "power")

    def _ft_opp_sh_goals(gid: int) -> int:
        _, opp = _team_and_opp_ids(gid)
        if opp is None:
            return 0
        return _goal_count_by_comment_reg(gid, opp, "short")

    sec_full_time = _build_section(
        title="Full Time (Regular Time)",
        rows=[
            {"label": "RT W", "values": _triple(_ft_result_prob("W"))},
            {"label": "RT D", "values": _triple(_ft_result_prob("D"))},
            {"label": "RT L", "values": _triple(_ft_result_prob("L"))},

            {"label": "RT TG 0.5+", "values": _triple(_count_ge_prob(1, _ft_team_goals))},
            {"label": "RT TG 1.5+", "values": _triple(_count_ge_prob(2, _ft_team_goals))},
            {"label": "RT TG 2.5+", "values": _triple(_count_ge_prob(3, _ft_team_goals))},
            {"label": "RT TG 3.5+", "values": _triple(_count_ge_prob(4, _ft_team_goals))},
            {"label": "RT TG 4.5+", "values": _triple(_count_ge_prob(5, _ft_team_goals))},

            {"label": "RT Total 1.5+", "values": _triple(_count_ge_prob(2, _ft_total_goals))},
            {"label": "RT Total 2.5+", "values": _triple(_count_ge_prob(3, _ft_total_goals))},
            {"label": "RT Total 3.5+", "values": _triple(_count_ge_prob(4, _ft_total_goals))},
            {"label": "RT Total 4.5+", "values": _triple(_count_ge_prob(5, _ft_total_goals))},
            {"label": "RT Total 5.5+", "values": _triple(_count_ge_prob(6, _ft_total_goals))},

            {"label": "RT BTTS 1+", "values": _triple(_bool_prob(lambda gid: (_ft_team_goals(gid) >= 1 and _ft_opp_goals(gid) >= 1)))},
            {"label": "RT BTTS 2+", "values": _triple(_bool_prob(lambda gid: (_ft_team_goals(gid) >= 2 and _ft_opp_goals(gid) >= 2)))},
            {"label": "RT BTTS 3+", "values": _triple(_bool_prob(lambda gid: (_ft_team_goals(gid) >= 3 and _ft_opp_goals(gid) >= 3)))},

            {"label": "RT W & Total 1.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_total_goals(gid) >= 2)))},
            {"label": "RT W & Total 2.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_total_goals(gid) >= 3)))},
            {"label": "RT W & Total 3.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_total_goals(gid) >= 4)))},
            {"label": "RT W & Total 4.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_total_goals(gid) >= 5)))},
            {"label": "RT W & Total 5.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_total_goals(gid) >= 6)))},

            {"label": "RT W & BTTS 1+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_team_goals(gid) >= 1 and _ft_opp_goals(gid) >= 1)))},
            {"label": "RT W & BTTS 2+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_team_goals(gid) >= 2 and _ft_opp_goals(gid) >= 2)))},
            {"label": "RT W & BTTS 3+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_reg_scores(gid)) == "W" and _ft_team_goals(gid) >= 3 and _ft_opp_goals(gid) >= 3)))},

            {"label": "RT First Goal", "values": _triple(_ft_first_goal_scored_prob())},

            {"label": "RT PP Occ (AVG)", "values": _triple(_avg_by_bucket(_ft_pp_opportunities))},
            {"label": "RT Penalty (AVG)", "values": _triple(_avg_by_bucket(_ft_pk_opportunities))},

            {"label": "RT PPG/PP", "values": _triple(_rate_by_bucket(_ft_team_pp_goals, _ft_pp_opportunities))},
            {"label": "RT SHGA/PP", "values": _triple(_rate_by_bucket(_ft_opp_sh_goals, _ft_pp_opportunities))},

            {"label": "RT SHG/PK", "values": _triple(_rate_by_bucket(_ft_team_sh_goals, _ft_pk_opportunities))},
            {"label": "RT PPGA/PK", "values": _triple(_rate_by_bucket(_ft_opp_pp_goals, _ft_pk_opportunities))},
        ],
    )



    # ─────────────────────────────────────────
    # NEW) 섹션: Period (1P/2P/3P)
    # ─────────────────────────────────────────
    def _period_section(period: str, title: str) -> Dict[str, Any]:
        p = _norm_period(period)

        # "P1" -> "1P", "P2" -> "2P", "P3" -> "3P"
        prefix = (p[1:] + "P") if p.startswith("P") else p

        def team_goals(gid: int) -> int:
            gf, _ = _period_scores(gid, p)
            return gf

        def opp_goals(gid: int) -> int:
            _, ga = _period_scores(gid, p)
            return ga

        def total_goals(gid: int) -> int:
            gf, ga = _period_scores(gid, p)
            return gf + ga

        def result_prob(res: str) -> Dict[str, Optional[float]]:
            def pred(gid: int) -> bool:
                gf, ga = _period_scores(gid, p)
                return _result_from_scores(gf, ga) == res
            return _bool_prob(pred)

        def first_goal_prob() -> Dict[str, Optional[float]]:
            # ✅ 해당 period에 골이 하나도 없으면 v=None -> False 처리
            # ✅ 분모(N)에서 제외하지 않음 (0 처리)
            def pred(gid: int) -> bool:
                v = _first_goal_scored_by_team(gid, sel_team_id, period=p)
                return v is True
            return _bool_prob(pred)

        # ── PP/PK (period 전용) ─────────────────────
        def _pp_opportunities(gid: int) -> int:
            # 우리 PP 기회 = 상대 팀 penalty 이벤트 수 (해당 period)
            _, opp = _team_and_opp_ids(gid)
            if opp is None:
                return 0
            return _penalty_count_by_team_period(gid, opp, p)

        def _pk_opportunities(gid: int) -> int:
            # 우리 PK 기회 = 우리 penalty 이벤트 수 (해당 period)
            return _penalty_count_by_team_period(gid, sel_team_id, p)

        def _team_pp_goals(gid: int) -> int:
            return _goal_count_by_comment_period(gid, sel_team_id, "power", p)

        def _team_sh_goals(gid: int) -> int:
            return _goal_count_by_comment_period(gid, sel_team_id, "short", p)

        def _opp_pp_goals(gid: int) -> int:
            _, opp = _team_and_opp_ids(gid)
            if opp is None:
                return 0
            return _goal_count_by_comment_period(gid, opp, "power", p)

        def _opp_sh_goals(gid: int) -> int:
            _, opp = _team_and_opp_ids(gid)
            if opp is None:
                return 0
            return _goal_count_by_comment_period(gid, opp, "short", p)

        rows: List[Dict[str, Any]] = [
            # 1) W/D/L
            {"label": f"{prefix} W", "values": _triple(result_prob("W"))},
            {"label": f"{prefix} D", "values": _triple(result_prob("D"))},
            {"label": f"{prefix} L", "values": _triple(result_prob("L"))},

            # 2) Team Goals Over (TG)
            {"label": f"{prefix} TG 0.5+", "values": _triple(_count_ge_prob(1, team_goals))},
            {"label": f"{prefix} TG 1.5+", "values": _triple(_count_ge_prob(2, team_goals))},

            # 3) Total Goals Over (Total)
            {"label": f"{prefix} Total 0.5+", "values": _triple(_count_ge_prob(1, total_goals))},
            {"label": f"{prefix} Total 1.5+", "values": _triple(_count_ge_prob(2, total_goals))},

            # 4) BTTS
            {"label": f"{prefix} BTTS 0.5+", "values": _triple(_bool_prob(lambda gid: (team_goals(gid) >= 1 and opp_goals(gid) >= 1)))},
            {"label": f"{prefix} BTTS 1.5+", "values": _triple(_bool_prob(lambda gid: (team_goals(gid) >= 2 and opp_goals(gid) >= 2)))},

            # 5) W & Total
            {"label": f"{prefix} W & Total 0.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_period_scores(gid, p)) == "W" and total_goals(gid) >= 1)))},
            {"label": f"{prefix} W & Total 1.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_period_scores(gid, p)) == "W" and total_goals(gid) >= 2)))},

            # 6) W & BTTS
            {"label": f"{prefix} W & BTTS 0.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_period_scores(gid, p)) == "W" and team_goals(gid) >= 1 and opp_goals(gid) >= 1)))},
            {"label": f"{prefix} W & BTTS 1.5+", "values": _triple(_bool_prob(lambda gid: (_result_from_scores(*_period_scores(gid, p)) == "W" and team_goals(gid) >= 2 and opp_goals(gid) >= 2)))},

            # 7) First Goal (해당 period 내 첫 골)
            {"label": f"{prefix} First Goal", "values": _triple(first_goal_prob())},

            # 8) PP/PK (AVG / Rate)
            {"label": f"{prefix} PP Occ (AVG)", "values": _triple(_avg_by_bucket(_pp_opportunities))},
            {"label": f"{prefix} Penalty (AVG)", "values": _triple(_avg_by_bucket(_pk_opportunities))},

            {"label": f"{prefix} PPG/PP", "values": _triple(_rate_by_bucket(_team_pp_goals, _pp_opportunities))},
            {"label": f"{prefix} SHGA/PP", "values": _triple(_rate_by_bucket(_opp_sh_goals, _pp_opportunities))},

            {"label": f"{prefix} SHG/PK", "values": _triple(_rate_by_bucket(_team_sh_goals, _pk_opportunities))},
            {"label": f"{prefix} PPGA/PK", "values": _triple(_rate_by_bucket(_opp_pp_goals, _pk_opportunities))},
        ]

        return _build_section(title=title, rows=rows)


    sec_1p = _period_section("P1", "1st Period (1P)")
    sec_2p = _period_section("P2", "2nd Period (2P)")
    sec_3p_period = _period_section("P3", "3rd Period (3P)")

    # ─────────────────────────────────────────
    # NEW) 섹션: Overtime (OT) / Shootout (SO)
    # ─────────────────────────────────────────
    def _final_winner_is_team(gid: int, team: int) -> Optional[bool]:
        gm = game_meta.get(gid) or {}
        h = _safe_int(gm.get("home_team_id"))
        a = _safe_int(gm.get("away_team_id"))
        sj = gm.get("score_json") or {}
        hs = sj.get("home")
        as_ = sj.get("away")
        if h is None or a is None or hs is None or as_ is None:
            return None
        if hs == as_:
            return None
        winner = h if hs > as_ else a
        return winner == team

    def _status(gid: int) -> str:
        gm = game_meta.get(gid) or {}
        return (gm.get("status") or "").strip()

    def _count_by_bucket(pred) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            out[b] = sum(1 for gid in ids if pred(gid))
        return out

    def _cond_rate(num_pred, denom_pred) -> Dict[str, Optional[float]]:
        """
        조건부 비율:
        - 분모: denom_pred를 만족하는 경기 수
        - 분자: num_pred를 만족하는 경기 수
        - 분모==0 => None (앱에서 '-' 로 표시)
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids = iter_bucket(b)
            denom = 0
            num = 0
            for gid in ids:
                if denom_pred(gid):
                    denom += 1
                    if num_pred(gid):
                        num += 1
            out[b] = (float(num) / float(denom)) if denom > 0 else None
        return out

    # ✅ 판정(네 DB 기준): AOT=OT 결판, AP=SO 결판
    is_ot_decided = lambda gid: (_status(gid) == "AOT")
    is_so_decided = lambda gid: (_status(gid) == "AP")
    is_ot_or_so_decided = lambda gid: (_status(gid) in ("AOT", "AP"))

    # ✅ count(표기용 n)
    # OT 섹션의 표기용 n(=오른쪽 T/H/A)은 "OT에 간 경기(OT 또는 SO로 결판)"가 분모가 되어야 함
    ot_n = _count_by_bucket(is_ot_or_so_decided)  # OT에 간 경기 수(OT+SO)
    so_n = _count_by_bucket(is_so_decided)        # SO 결판 경기 수

    # subtitle은 UI에서 OT/SO는 숨기고 있지만(혹시 fallback 파싱 대비) 기존 포맷 유지
    ot_subtitle = f"n OT {ot_n['totals']}/{ot_n['home']}/{ot_n['away']} · SO {so_n['totals']}/{so_n['home']}/{so_n['away']}"
    so_subtitle = f"n SO {so_n['totals']}/{so_n['home']}/{so_n['away']}"


    sec_ot = _build_section(
        title="Overtime (OT)",
        subtitle=ot_subtitle,  # ✅ 추가
        counts=ot_n,
        rows=[
            {
                "label": "OT W",
                "values": _triple(
                    _cond_rate(
                        # 분자: OT로 결판 + 해당팀 승
                        num_pred=lambda gid: (
                            is_ot_decided(gid) and _final_winner_is_team(gid, sel_team_id) is True
                        ),
                        # 분모: OT에 간 경기(OT 또는 SO로 결판)
                        denom_pred=is_ot_or_so_decided,
                    )
                ),
            },
            {
                "label": "OT L",
                "values": _triple(
                    _cond_rate(
                        # 분자: OT로 결판 + 해당팀 패
                        num_pred=lambda gid: (
                            is_ot_decided(gid) and _final_winner_is_team(gid, sel_team_id) is False
                        ),
                        # 분모: OT에 간 경기(OT 또는 SO로 결판)
                        denom_pred=is_ot_or_so_decided,
                    )
                ),
            },

            {
                "label": "OT→SO Rate",
                "values": _triple(
                    _cond_rate(
                        num_pred=is_so_decided,
                        denom_pred=is_ot_or_so_decided,
                    )
                ),
            },
        ],
    )

    sec_so = _build_section(
        title="Shootout (SO)",
        subtitle=so_subtitle,  # ✅ 추가
        counts=so_n,
        rows=[
            {
                "label": "SO W",
                "values": _triple(
                    _cond_rate(
                        num_pred=lambda gid: (
                            is_so_decided(gid) and _final_winner_is_team(gid, sel_team_id) is True
                        ),
                        denom_pred=is_so_decided,
                    )
                ),
            },
            {
                "label": "SO L",
                "values": _triple(
                    _cond_rate(
                        num_pred=lambda gid: (
                            is_so_decided(gid) and _final_winner_is_team(gid, sel_team_id) is False
                        ),
                        denom_pred=is_so_decided,
                    )
                ),
            },
        ],
    )



    sections = [
        sec_full_time,

        sec_1p,
        sec_2p,
        sec_3p_period,

        sec_ot,
        sec_so,

        # 3rd Period Clutch Situations (state별 분리)
        sec_last_lead1,
        sec_last_lead2,
        sec_last_trail1,
        sec_last_trail2,
        sec_last_tied,
        sec_p3,

        # Goal Timing (이미 구현됨)
        sec_goal_time,
    ]


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
            "mode": mode,  # ✅ "last_n" or "season"
            "selected_season": season,  # ✅ 선택한 시즌(없으면 null)
            "available_seasons": available_seasons,  # ✅ DB에서 최신 2개 자동
            "sample_sizes": {
                "totals": len(totals_ids),
                "home": len(home_ids),
                "away": len(away_ids),
            },
        },
    }

