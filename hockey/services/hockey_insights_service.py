# hockey/services/hockey_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


FINISHED_STATUSES = ("FT", "AOT", "AP")  # Regular FT, After OT, After Penalty(SO)


@dataclass(frozen=True)
class TeamSide:
    team_id: int
    side: str  # "home" or "away"


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _safe_str(v: Any) -> str:
    return (v or "").strip()


def _pct(num: int, den: int) -> Optional[float]:
    if den <= 0:
        return None
    return round(num / den, 4)


def _to_iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _bin_2min(minute: Optional[int]) -> Optional[int]:
    """
    20분 period 기준 2분 bin index (0~9)
    minute가 1~20 형태면:
      1-2 -> 0, 3-4 -> 1, ... 19-20 -> 9
    minute가 0~19 형태라면:
      0-1 -> 0, 2-3 -> 1, ... 18-19 -> 9
    """
    if minute is None:
        return None
    m = int(minute)
    if m <= 0:
        return 0
    if 1 <= m <= 20:
        return min((m - 1) // 2, 9)
    return min(m // 2, 9)


def _normalize_period(p: str) -> str:
    p2 = (p or "").strip().upper()
    # API-sports는 보통 P1/P2/P3/OT/SO
    return p2


def _event_comment_text(row: Dict[str, Any]) -> str:
    # comment 컬럼 + raw_json의 comment까지 합쳐서 판정에 사용
    c1 = _safe_str(row.get("comment"))
    rj = row.get("raw_json") or {}
    c2 = _safe_str(rj.get("comment")) if isinstance(rj, dict) else ""
    merged = " ".join([x for x in [c1, c2] if x]).strip()
    return merged


def _is_pp_goal(goal_row: Dict[str, Any], active_pp_windows: List[Tuple[int, int, int]]) -> bool:
    """
    Power Play Goal 판정:
    1) goal comment에 PPG / Power Play / PP / Powerplay 같은 텍스트가 있으면 True
    2) 없으면, "상대 팀 페널티로 만든 PP window 안에서 나온 골"이면 True (휴리스틱)
       - window: (beneficiary_team_id, start_minute, end_minute) for same period
       - start_minute = penalty_minute
       - end_minute = penalty_minute + 2 (2분 PP 가정)
    """
    txt = _event_comment_text(goal_row).lower()
    if any(k in txt for k in ["ppg", "power play", "powerplay", "pp goal", "pp-goal", "pp "]):
        return True

    period = _normalize_period(goal_row.get("period"))
    minute = _safe_int(goal_row.get("minute"))
    team_id = _safe_int(goal_row.get("team_id"))
    if minute is None or team_id is None:
        return False

    # active_pp_windows는 "현재 period"의 windows만 넘어오게 구성해도 되지만, 안전하게 필터
    for (benef_team_id, st, en) in active_pp_windows:
        if benef_team_id != team_id:
            continue
        if st <= minute <= en:
            return True

    return False


def _build_pp_windows_for_period(
    penalty_rows: List[Dict[str, Any]],
    home_team_id: int,
    away_team_id: int,
    period: str,
) -> List[Tuple[int, int, int]]:
    """
    penalty 이벤트만으로 PP window 구성 (휴리스틱):
    - penalty는 해당 팀이 "페널티를 받은 팀"
    - 따라서 PP beneficiary(상대팀) = other team
    - window는 penalty_minute부터 penalty_minute+2
    """
    out: List[Tuple[int, int, int]] = []
    for r in penalty_rows:
        if _normalize_period(r.get("period")) != period:
            continue
        pen_team_id = _safe_int(r.get("team_id"))
        minute = _safe_int(r.get("minute"))
        if pen_team_id is None or minute is None:
            continue

        if pen_team_id == home_team_id:
            benef = away_team_id
        elif pen_team_id == away_team_id:
            benef = home_team_id
        else:
            continue

        out.append((benef, minute, minute + 2))
    return out


def _fetch_sample_games(league_id: int, season: int, exclude_game_id: int, sample_size: int) -> List[Dict[str, Any]]:
    return hockey_fetch_all(
        """
        SELECT
            id AS game_id,
            league_id,
            season,
            home_team_id,
            away_team_id,
            status,
            game_date
        FROM hockey_games
        WHERE league_id = %s
          AND season = %s
          AND status = ANY(%s)
          AND id <> %s
        ORDER BY game_date DESC NULLS LAST
        LIMIT %s
        """,
        (league_id, season, list(FINISHED_STATUSES), exclude_game_id, sample_size),
    )


def _fetch_events_for_games(game_ids: List[int]) -> List[Dict[str, Any]]:
    if not game_ids:
        return []
    placeholders = ", ".join(["%s"] * len(game_ids))
    sql = f"""
        SELECT
            e.game_id,
            UPPER(TRIM(e.period)) AS period,
            e.minute,
            e.team_id,
            LOWER(TRIM(e.type)) AS type_norm,
            e.comment,
            e.event_order,
            e.raw_json,
            g.home_team_id,
            g.away_team_id
        FROM hockey_game_events e
        JOIN hockey_games g ON g.id = e.game_id
        WHERE e.game_id IN ({placeholders})
          AND LOWER(TRIM(e.type)) IN ('goal', 'penalty')
        ORDER BY
            e.game_id ASC,
            e.period ASC,
            e.minute ASC NULLS LAST,
            e.event_order ASC
    """
    return hockey_fetch_all(sql, tuple(game_ids))


def _group_events_by_game(events: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in events:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        out.setdefault(gid, []).append(r)
    return out


def _game_period_scores_from_events(game_events: List[Dict[str, Any]], home_id: int, away_id: int) -> Dict[str, Dict[str, int]]:
    """
    period -> {home: goals, away: goals}
    """
    scores: Dict[str, Dict[str, int]] = {}
    for ev in game_events:
        if ev.get("type_norm") != "goal":
            continue
        p = _normalize_period(ev.get("period"))
        tid = _safe_int(ev.get("team_id"))
        if not p or tid is None:
            continue
        scores.setdefault(p, {"home": 0, "away": 0})
        if tid == home_id:
            scores[p]["home"] += 1
        elif tid == away_id:
            scores[p]["away"] += 1
    return scores


def _sum_periods(scores: Dict[str, Dict[str, int]], periods: List[str]) -> Tuple[int, int]:
    h = 0
    a = 0
    for p in periods:
        pa = scores.get(p, {"home": 0, "away": 0})
        h += int(pa.get("home", 0))
        a += int(pa.get("away", 0))
    return h, a


def _period_result(h: int, a: int) -> str:
    if h > a:
        return "W"
    if h < a:
        return "L"
    return "D"


def _team_side_in_game(game_row: Dict[str, Any], team_id: int) -> Optional[str]:
    h = _safe_int(game_row.get("home_team_id"))
    a = _safe_int(game_row.get("away_team_id"))
    if h is not None and team_id == h:
        return "home"
    if a is not None and team_id == a:
        return "away"
    return None


def _as_three_row(key: str, label: str, total: Optional[float], home: Optional[float], away: Optional[float]) -> Dict[str, Any]:
    return {"key": key, "label": label, "values": {"total": total, "home": home, "away": away}}


def _as_single_row(key: str, label: str, value: Optional[float]) -> Dict[str, Any]:
    return {"key": key, "label": label, "value": value}


def hockey_get_game_insights(game_id: int, sample_size: int = 200) -> Dict[str, Any]:
    # 1) 현재 게임 메타
    g = hockey_fetch_one(
        """
        SELECT
            id AS game_id,
            league_id,
            season,
            home_team_id,
            away_team_id
        FROM hockey_games
        WHERE id = %s
        LIMIT 1
        """,
        (game_id,),
    )
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    league_id = _safe_int(g.get("league_id"))
    season = _safe_int(g.get("season"))
    home_id = _safe_int(g.get("home_team_id"))
    away_id = _safe_int(g.get("away_team_id"))

    if league_id is None or season is None or home_id is None or away_id is None:
        raise ValueError("BAD_GAME_DATA")

    # 2) 샘플 경기(리그+시즌 종료경기)
    sample_games = _fetch_sample_games(league_id, season, game_id, sample_size)
    sample_ids = [_safe_int(x.get("game_id")) for x in sample_games]
    sample_ids = [x for x in sample_ids if x is not None]

    # 3) 샘플 이벤트 일괄 로드(Goal/Penalty)
    events = _fetch_events_for_games(sample_ids)
    ev_by_game = _group_events_by_game(events)

    # -----------------------------
    # 공통 카운터들 (match-level)
    # -----------------------------
    match_den = len(sample_ids)

    # Total goals over (RT)
    match_total_over = {1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0, 5.5: 0}
    match_btts = {1: 0, 2: 0, 3: 0}

    # OT/SO
    ot_den = 0
    ot_win = 0
    ot_draw_reach_so = 0
    ot_loss = 0

    so_den = 0
    so_win = 0
    so_loss = 0

    # Period transitions (home perspective): P1 result -> P2 result, P2 -> P3
    # condition distribution
    trans_1_to_2 = {
        "W": {"W": 0, "D": 0, "L": 0, "den": 0},
        "D": {"W": 0, "D": 0, "L": 0, "den": 0},
        "L": {"W": 0, "D": 0, "L": 0, "den": 0},
    }
    trans_2_to_3 = {
        "W": {"W": 0, "D": 0, "L": 0, "den": 0},
        "D": {"W": 0, "D": 0, "L": 0, "den": 0},
        "L": {"W": 0, "D": 0, "L": 0, "den": 0},
    }

    # 3P start score impact (RT): leading/tied/trailing at start of P3 -> RT W/D/L (home perspective)
    start3_impact = {
        "leading": {"W": 0, "D": 0, "L": 0, "den": 0},
        "tied": {"W": 0, "D": 0, "L": 0, "den": 0},
        "trailing": {"W": 0, "D": 0, "L": 0, "den": 0},
    }

    # First goal impact (RT): scenarios -> RT W/D/L (home perspective)
    fg_impact = {
        "1p_first_goal": {"W": 0, "D": 0, "L": 0, "den": 0},          # home scored first goal in P1
        "1p_conceded_first_goal": {"W": 0, "D": 0, "L": 0, "den": 0},  # home conceded first goal in P1
        "2p_0_0_first_goal": {"W": 0, "D": 0, "L": 0, "den": 0},       # P1 ended 0-0 then first goal in P2 by home
        "2p_0_0_first_goal_conceded": {"W": 0, "D": 0, "L": 0, "den": 0},
        "3p_0_0_first_goal": {"W": 0, "D": 0, "L": 0, "den": 0},       # P1+P2 ended 0-0 then first goal in P3 by home
        "3p_0_0_first_goal_conceded": {"W": 0, "D": 0, "L": 0, "den": 0},
    }

    # Goal timing distribution (2-min bins) for P1/P2/P3, based on all goals in sample (match-level)
    timing_bins = {
        "P1": [0] * 10,
        "P2": [0] * 10,
        "P3": [0] * 10,
    }
    timing_totals = {
        "P1": 0,
        "P2": 0,
        "P3": 0,
    }

    # 3rd period clutch situations: last 3 minutes (minute >= 17), margin 1~2
    # home perspective: leading1/leading2/trailing1/trailing2/tied -> score prob, concede prob
    clutch = {
        "leading_1": {"score": 0, "concede": 0, "den": 0},
        "leading_2": {"score": 0, "concede": 0, "den": 0},
        "trailing_1": {"score": 0, "concede": 0, "den": 0},
        "trailing_2": {"score": 0, "concede": 0, "den": 0},
        "tied": {"score": 0, "concede": 0, "den": 0},
    }

    # -----------------------------
    # 팀별 카운터 (team-level; current home team / current away team)
    # -----------------------------
    def init_team_counters() -> Dict[str, Any]:
        return {
            "den": 0,
            "rt": {
                "W": 0, "D": 0, "L": 0,
                "first_goal": 0, "first_goal_den": 0,
                "clean_sheet": 0,
                "penalty_occurred": 0,
                "pp_occurred": 0,
                "pp_goal": 0,
                "team_over": {0.5: 0, 1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0},
                "win_over": {1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0, 5.5: 0},
                "win_btts": {1: 0, 2: 0, 3: 0},
            },
            "p": {
                "P1": _init_period_block(),
                "P2": _init_period_block(),
                "P3": _init_period_block(),
            },
        }

    def _init_period_block() -> Dict[str, Any]:
        return {
            "W": 0, "D": 0, "L": 0,
            "first_goal": 0, "first_goal_den": 0,
            "clean_sheet": 0,
            "penalty_occurred": 0,
            "pp_occurred": 0,
            "pp_goal": 0,
            "team_over": {0.5: 0, 1.5: 0, 2.5: 0},
            "win_over": {1.5: 0, 2.5: 0},
            "win_btts": {1: 0, 2: 0, 3: 0},
            "btts": {1: 0, 2: 0},
            "total_over": {0.5: 0, 1.5: 0, 2.5: 0},
        }

    home_team = init_team_counters()
    away_team = init_team_counters()

    # 팀 샘플을 "같은 팀이 참여한 경기"만 대상으로 잡을 때, 표본이 너무 작아질 수 있어서
    # 현재는 "리그+시즌 샘플" 안에서 그 팀이 등장한 경기만 집계 (너 출력처럼 sample 10 나오는 게 정상)
    def collect_team_pairs(team_id: int) -> List[Tuple[Dict[str, Any], str]]:
        pairs: List[Tuple[Dict[str, Any], str]] = []
        for gr in sample_games:
            side = _team_side_in_game(gr, team_id)
            if side:
                pairs.append((gr, side))
        return pairs

    home_pairs = collect_team_pairs(home_id)
    away_pairs = collect_team_pairs(away_id)

    def goals_for_side(scores: Dict[str, Dict[str, int]], periods: List[str], side: str) -> Tuple[int, int]:
        h, a = _sum_periods(scores, periods)
        if side == "home":
            return h, a
        return a, h

    def first_goal_side_in_game(game_events: List[Dict[str, Any]], home_team_id: int, away_team_id: int) -> Optional[str]:
        # period order: P1,P2,P3,OT,SO
        order = {"P1": 1, "P2": 2, "P3": 3, "OT": 4, "SO": 5}
        goals = [e for e in game_events if e.get("type_norm") == "goal"]
        if not goals:
            return None
        def keyf(e: Dict[str, Any]) -> Tuple[int, int, int]:
            p = _normalize_period(e.get("period"))
            pi = order.get(p, 99)
            m = _safe_int(e.get("minute")) or 999
            eo = _safe_int(e.get("event_order")) or 0
            return (pi, m, eo)

        first = sorted(goals, key=keyf)[0]
        tid = _safe_int(first.get("team_id"))
        if tid == home_team_id:
            return "home"
        if tid == away_team_id:
            return "away"
        return None

    def first_goal_in_period_side(game_events: List[Dict[str, Any]], period: str, home_team_id: int, away_team_id: int) -> Optional[str]:
        goals = [e for e in game_events if e.get("type_norm") == "goal" and _normalize_period(e.get("period")) == period]
        if not goals:
            return None
        def keyf(e: Dict[str, Any]) -> Tuple[int, int]:
            m = _safe_int(e.get("minute")) or 999
            eo = _safe_int(e.get("event_order")) or 0
            return (m, eo)
        first = sorted(goals, key=keyf)[0]
        tid = _safe_int(first.get("team_id"))
        if tid == home_team_id:
            return "home"
        if tid == away_team_id:
            return "away"
        return None

    # -----------------------------
    # 메인 루프: sample_ids 전부를 돌며 match-level 통계 + home-perspective 통계 누적
    # -----------------------------
    game_row_by_id = { _safe_int(r.get("game_id")): r for r in sample_games if _safe_int(r.get("game_id")) is not None }

    for gid in sample_ids:
        gr = game_row_by_id.get(gid) or {}
        h_id = _safe_int(gr.get("home_team_id"))
        a_id = _safe_int(gr.get("away_team_id"))
        status = _safe_str(gr.get("status")).upper()

        if h_id is None or a_id is None:
            continue

        ge = ev_by_game.get(gid, [])
        scores = _game_period_scores_from_events(ge, h_id, a_id)

        # (A) Goal timing distribution (P1/P2/P3)
        for ev in ge:
            if ev.get("type_norm") != "goal":
                continue
            p = _normalize_period(ev.get("period"))
            if p not in ("P1", "P2", "P3"):
                continue
            b = _bin_2min(_safe_int(ev.get("minute")))
            if b is None:
                continue
            timing_bins[p][b] += 1
            timing_totals[p] += 1

        # Regular Time score
        rt_h, rt_a = _sum_periods(scores, ["P1", "P2", "P3"])
        rt_res_home = _period_result(rt_h, rt_a)

        # (B) Match totals: total over / btts (RT)
        total = rt_h + rt_a
        for th in match_total_over:
            if total > th:
                match_total_over[th] += 1
        for n in match_btts:
            if rt_h >= n and rt_a >= n:
                match_btts[n] += 1

        # (C) OT / SO (match-level total)
        if status in ("AOT", "AP"):
            ot_den += 1
            if status == "AP":
                ot_draw_reach_so += 1
            else:
                # AOT: OT에서 승패가 결정됨 (최종 승패를 OT win/loss로 취급)
                # final winner 판단은 "RT + OT" 목표. 여기서는 OT 득점만 알 수 없으니,
                # scores에 OT가 있으면 포함, 없으면 "status만"으로는 승패 불명.
                # => 가장 안전: OT period goals를 포함한 최종 비교
                fin_h = rt_h + scores.get("OT", {"home": 0, "away": 0}).get("home", 0)
                fin_a = rt_a + scores.get("OT", {"home": 0, "away": 0}).get("away", 0)
                if fin_h > fin_a:
                    ot_win += 1
                elif fin_h < fin_a:
                    ot_loss += 1
                else:
                    # 희귀 케이스(데이터 불완전): shootout reached로 처리
                    ot_draw_reach_so += 1

        if status == "AP":
            so_den += 1
            # shootout winner는 최종 득점이 누가 큰지로 판정(OT/SO 득점이 score_json에 들어오는 경우도 있으나,
            # 여기서는 이벤트 goal만 가지고 있으므로, 실제로는 RT+OT goals로 final이 동률일 가능성 높음.
            # => 안전한 방식: score_json에서 최종 스코어를 읽는다 (있으면)
            #    없으면 RT를 기준으로 동률이면 home을 win으로 두지 않고 제외(den 감소)하면 안되니,
            #    여기서는 score_json을 추가 조회하지 않는 구조라서, AP는 "승/패 50/50" 같은 임의 금지.
            # => 따라서 score_json을 한 번 더 조회해서 확정 판정한다.
            gscore = hockey_fetch_one("SELECT score_json FROM hockey_games WHERE id=%s LIMIT 1", (gid,))
            score_json = (gscore or {}).get("score_json") or {}
            # score_json 구조가 리그별로 다를 수 있어 key들을 최대한 넓게 커버
            # 기대: score_json["fulltime"]["home"], score_json["fulltime"]["away"] 같은 형태
            def _extract_final(sc: Any) -> Tuple[Optional[int], Optional[int]]:
                if not isinstance(sc, dict):
                    return None, None
                # 후보 키들
                candidates = [
                    ("fulltime", "home", "away"),
                    ("final", "home", "away"),
                    ("total", "home", "away"),
                    ("scores", "home", "away"),
                ]
                for k, hk, ak in candidates:
                    obj = sc.get(k)
                    if isinstance(obj, dict):
                        hh = _safe_int(obj.get(hk))
                        aa = _safe_int(obj.get(ak))
                        if hh is not None and aa is not None:
                            return hh, aa
                # fallback: direct
                hh = _safe_int(sc.get("home"))
                aa = _safe_int(sc.get("away"))
                return hh, aa
            fin_h2, fin_a2 = _extract_final(score_json)
            if fin_h2 is not None and fin_a2 is not None:
                if fin_h2 > fin_a2:
                    so_win += 1
                elif fin_h2 < fin_a2:
                    so_loss += 1
                else:
                    # 동률이면 데이터 부족. den에서 제외는 하면 안되므로, 여기서는 loss로 치지 않고 둘 다 증가 X.
                    pass

        # (D) Period transitions (home perspective)
        p1_h, p1_a = _sum_periods(scores, ["P1"])
        p2_h, p2_a = _sum_periods(scores, ["P2"])
        p3_h, p3_a = _sum_periods(scores, ["P3"])
        r1 = _period_result(p1_h, p1_a)
        r2 = _period_result(p2_h, p2_a)
        r3 = _period_result(p3_h, p3_a)
        trans_1_to_2[r1]["den"] += 1
        trans_1_to_2[r1][r2] += 1
        trans_2_to_3[r2]["den"] += 1
        trans_2_to_3[r2][r3] += 1

        # (E) 3P start score impact (home perspective, RT result)
        p12_h, p12_a = _sum_periods(scores, ["P1", "P2"])
        if p12_h > p12_a:
            k = "leading"
        elif p12_h < p12_a:
            k = "trailing"
        else:
            k = "tied"
        start3_impact[k]["den"] += 1
        start3_impact[k][rt_res_home] += 1

        # (F) First goal impact (home perspective)
        fg_side = first_goal_side_in_game(ge, h_id, a_id)
        fg_p1 = first_goal_in_period_side(ge, "P1", h_id, a_id)

        # 1P first goal / conceded first goal
        if fg_p1 is not None:
            fg_impact["1p_first_goal"]["den"] += 1
            fg_impact["1p_conceded_first_goal"]["den"] += 1
            if fg_p1 == "home":
                fg_impact["1p_first_goal"][rt_res_home] += 1
            else:
                fg_impact["1p_conceded_first_goal"][rt_res_home] += 1

        # 2P start 0-0 then first goal in P2
        if p1_h == 0 and p1_a == 0:
            fg_p2 = first_goal_in_period_side(ge, "P2", h_id, a_id)
            if fg_p2 is not None:
                fg_impact["2p_0_0_first_goal"]["den"] += 1
                fg_impact["2p_0_0_first_goal_conceded"]["den"] += 1
                if fg_p2 == "home":
                    fg_impact["2p_0_0_first_goal"][rt_res_home] += 1
                else:
                    fg_impact["2p_0_0_first_goal_conceded"][rt_res_home] += 1

        # 3P start 0-0 then first goal in P3
        if p12_h == 0 and p12_a == 0:
            fg_p3 = first_goal_in_period_side(ge, "P3", h_id, a_id)
            if fg_p3 is not None:
                fg_impact["3p_0_0_first_goal"]["den"] += 1
                fg_impact["3p_0_0_first_goal_conceded"]["den"] += 1
                if fg_p3 == "home":
                    fg_impact["3p_0_0_first_goal"][rt_res_home] += 1
                else:
                    fg_impact["3p_0_0_first_goal_conceded"][rt_res_home] += 1

        # (G) 3rd period clutch (home perspective)
        # score at start of minute 17 in P3 (after P1+P2 + P3 goals with minute < 17)
        p3_goals = [e for e in ge if e.get("type_norm") == "goal" and _normalize_period(e.get("period")) == "P3"]
        # score before window
        pre_h = p12_h
        pre_a = p12_a
        for e in p3_goals:
            m = _safe_int(e.get("minute"))
            tid = _safe_int(e.get("team_id"))
            if m is None or tid is None:
                continue
            if m < 17:
                if tid == h_id:
                    pre_h += 1
                elif tid == a_id:
                    pre_a += 1

        diff = pre_h - pre_a
        # only 1~2 goal margin or tied
        if diff == 1:
            ck = "leading_1"
        elif diff == 2:
            ck = "leading_2"
        elif diff == -1:
            ck = "trailing_1"
        elif diff == -2:
            ck = "trailing_2"
        elif diff == 0:
            ck = "tied"
        else:
            ck = ""  # 제외

        if ck:
            clutch[ck]["den"] += 1
            scored = False
            conceded = False
            for e in p3_goals:
                m = _safe_int(e.get("minute"))
                tid = _safe_int(e.get("team_id"))
                if m is None or tid is None:
                    continue
                if m >= 17:
                    if tid == h_id:
                        scored = True
                    elif tid == a_id:
                        conceded = True
            if scored:
                clutch[ck]["score"] += 1
            if conceded:
                clutch[ck]["concede"] += 1

    # -----------------------------
    # 팀별 집계: 현재 경기 Home/Away 팀 기준으로 RT + period metrics
    # -----------------------------
    def apply_team_metrics(team_obj: Dict[str, Any], pairs: List[Tuple[Dict[str, Any], str]]) -> None:
        team_obj["den"] = len(pairs)
        for gr, side in pairs:
            gid = _safe_int(gr.get("game_id"))
            if gid is None:
                continue
            h_id = _safe_int(gr.get("home_team_id"))
            a_id = _safe_int(gr.get("away_team_id"))
            status = _safe_str(gr.get("status")).upper()
            if h_id is None or a_id is None:
                continue

            ge = ev_by_game.get(gid, [])
            scores = _game_period_scores_from_events(ge, h_id, a_id)

            # RT goals for this side
            tg, og = goals_for_side(scores, ["P1", "P2", "P3"], side)
            rt_total = tg + og
            rt_res = _period_result(tg, og)

            # W/D/L
            team_obj["rt"][rt_res] += 1

            # First goal (entire game)
            fg = first_goal_side_in_game(ge, h_id, a_id)
            if fg is not None:
                team_obj["rt"]["first_goal_den"] += 1
                if fg == side:
                    team_obj["rt"]["first_goal"] += 1

            # Clean sheet (RT)
            if og == 0:
                team_obj["rt"]["clean_sheet"] += 1

            # Penalty / PP occurred / PPG (RT)
            penalty_rows = [e for e in ge if e.get("type_norm") == "penalty"]
            # RT에서 "발생" 기준: P1~P3에서 하나라도 있으면 True
            rt_pen = any(_normalize_period(e.get("period")) in ("P1", "P2", "P3") for e in penalty_rows)
            if rt_pen:
                team_obj["rt"]["penalty_occurred"] += 1
                # PP occurred도 penalty로부터 발생한다고 정의(데이터에 PP 이벤트가 없으므로 정식 대체 정의)
                team_obj["rt"]["pp_occurred"] += 1

            # PPG: P1~P3에서 PP window 안에서 나온 골이 "그 팀"의 골이면 True
            # windows는 period별로 구성
            rt_ppg = False
            for per in ("P1", "P2", "P3"):
                windows = _build_pp_windows_for_period(penalty_rows, h_id, a_id, per)
                goals_per = [e for e in ge if e.get("type_norm") == "goal" and _normalize_period(e.get("period")) == per]
                for g1 in goals_per:
                    if _is_pp_goal(g1, windows):
                        tid = _safe_int(g1.get("team_id"))
                        if tid is None:
                            continue
                        # 누구 골인지 체크 -> side에 따라
                        if side == "home" and tid == h_id:
                            rt_ppg = True
                        if side == "away" and tid == a_id:
                            rt_ppg = True
            if rt_ppg:
                team_obj["rt"]["pp_goal"] += 1

            # Team over thresholds (RT)
            for th in list(team_obj["rt"]["team_over"].keys()):
                if tg > th:
                    team_obj["rt"]["team_over"][th] += 1

            # Win & Over / Win & BTTS (RT)
            if rt_res == "W":
                for th in list(team_obj["rt"]["win_over"].keys()):
                    if rt_total > th:
                        team_obj["rt"]["win_over"][th] += 1
                for n in list(team_obj["rt"]["win_btts"].keys()):
                    if tg >= n and og >= n:
                        team_obj["rt"]["win_btts"][n] += 1

            # Period metrics (P1/P2/P3)
            for per in ("P1", "P2", "P3"):
                block = team_obj["p"][per]
                ptg, pog = goals_for_side(scores, [per], side)
                pres = _period_result(ptg, pog)
                block[pres] += 1

                # first goal in period
                fg_p = first_goal_in_period_side(ge, per, h_id, a_id)
                if fg_p is not None:
                    block["first_goal_den"] += 1
                    if fg_p == side:
                        block["first_goal"] += 1

                # clean sheet in period (opp period goals = 0)
                if pog == 0:
                    block["clean_sheet"] += 1

                # penalty occurred / PP occurred (in that period)
                p_pen = any(_normalize_period(e.get("period")) == per for e in penalty_rows)
                if p_pen:
                    block["penalty_occurred"] += 1
                    block["pp_occurred"] += 1

                # PPG in that period
                windows = _build_pp_windows_for_period(penalty_rows, h_id, a_id, per)
                p_ppg = False
                for g1 in [e for e in ge if e.get("type_norm") == "goal" and _normalize_period(e.get("period")) == per]:
                    if _is_pp_goal(g1, windows):
                        tid = _safe_int(g1.get("team_id"))
                        if tid is None:
                            continue
                        if side == "home" and tid == h_id:
                            p_ppg = True
                        if side == "away" and tid == a_id:
                            p_ppg = True
                if p_ppg:
                    block["pp_goal"] += 1

                # team over in period
                for th in list(block["team_over"].keys()):
                    if ptg > th:
                        block["team_over"][th] += 1

                # total goals over in period
                ptotal = ptg + pog
                for th in list(block["total_over"].keys()):
                    if ptotal > th:
                        block["total_over"][th] += 1

                # BTTS in period
                if ptg >= 1 and pog >= 1:
                    block["btts"][1] += 1
                if ptg >= 2 and pog >= 2:
                    block["btts"][2] += 1

                # win & over / win & btts (period)
                if pres == "W":
                    for th in list(block["win_over"].keys()):
                        if ptotal > th:
                            block["win_over"][th] += 1
                    # 요청에 1+/2+/3+ 모두 있으므로 3+도 포함
                    for n in list(block["win_btts"].keys()):
                        if ptg >= n and pog >= n:
                            block["win_btts"][n] += 1

    apply_team_metrics(home_team, home_pairs)
    apply_team_metrics(away_team, away_pairs)

    # -----------------------------
    # JSON sections 구성: “요청한 섹션/metric 전부”
    # -----------------------------
    sections: List[Dict[str, Any]] = []

    # Helper: team metrics getter
    def tget(team_obj: Dict[str, Any], path: List[str], default: Any = None) -> Any:
        cur: Any = team_obj
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    # Full Time (Regular Time)
    den_home = int(home_team["den"])
    den_away = int(away_team["den"])

    sections.append(
        {
            "id": "rt_full_time",
            "title": "Full Time (Regular Time)",
            "layout": "three_cols",
            "columns": ["Total", "Home", "Away"],
            "rows": [
                _as_three_row("win", "Win", None, _pct(tget(home_team, ["rt", "W"], 0), den_home), _pct(tget(away_team, ["rt", "W"], 0), den_away)),
                _as_three_row("draw", "Draw", None, _pct(tget(home_team, ["rt", "D"], 0), den_home), _pct(tget(away_team, ["rt", "D"], 0), den_away)),
                _as_three_row("loss", "Loss", None, _pct(tget(home_team, ["rt", "L"], 0), den_home), _pct(tget(away_team, ["rt", "L"], 0), den_away)),

                _as_three_row("first_goal_scored", "First Goal Scored", None,
                              _pct(tget(home_team, ["rt", "first_goal"], 0), max(int(tget(home_team, ["rt", "first_goal_den"], 0)), 0)),
                              _pct(tget(away_team, ["rt", "first_goal"], 0), max(int(tget(away_team, ["rt", "first_goal_den"], 0)), 0))),

                _as_three_row("power_play_occurred", "Power Play Occurred", None,
                              _pct(tget(home_team, ["rt", "pp_occurred"], 0), den_home),
                              _pct(tget(away_team, ["rt", "pp_occurred"], 0), den_away)),
                _as_three_row("power_play_goal", "Power Play Goal", None,
                              _pct(tget(home_team, ["rt", "pp_goal"], 0), den_home),
                              _pct(tget(away_team, ["rt", "pp_goal"], 0), den_away)),
                _as_three_row("penalty_occurred", "Penalty Occurred", None,
                              _pct(tget(home_team, ["rt", "penalty_occurred"], 0), den_home),
                              _pct(tget(away_team, ["rt", "penalty_occurred"], 0), den_away)),

                _as_three_row("clean_sheet", "Clean Sheet", None,
                              _pct(tget(home_team, ["rt", "clean_sheet"], 0), den_home),
                              _pct(tget(away_team, ["rt", "clean_sheet"], 0), den_away)),

                _as_three_row("team_over_0_5", "Team Over 0.5 Goals", None,
                              _pct(tget(home_team, ["rt", "team_over", 0.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "team_over", 0.5], 0), den_away)),
                _as_three_row("team_over_1_5", "Team Over 1.5 Goals", None,
                              _pct(tget(home_team, ["rt", "team_over", 1.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "team_over", 1.5], 0), den_away)),
                _as_three_row("team_over_2_5", "Team Over 2.5 Goals", None,
                              _pct(tget(home_team, ["rt", "team_over", 2.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "team_over", 2.5], 0), den_away)),
                _as_three_row("team_over_3_5", "Team Over 3.5 Goals", None,
                              _pct(tget(home_team, ["rt", "team_over", 3.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "team_over", 3.5], 0), den_away)),
                _as_three_row("team_over_4_5", "Team Over 4.5 Goals", None,
                              _pct(tget(home_team, ["rt", "team_over", 4.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "team_over", 4.5], 0), den_away)),

                _as_three_row("total_over_1_5", "Total Goals Over 1.5", _pct(match_total_over[1.5], match_den), None, None),
                _as_three_row("total_over_2_5", "Total Goals Over 2.5", _pct(match_total_over[2.5], match_den), None, None),
                _as_three_row("total_over_3_5", "Total Goals Over 3.5", _pct(match_total_over[3.5], match_den), None, None),
                _as_three_row("total_over_4_5", "Total Goals Over 4.5", _pct(match_total_over[4.5], match_den), None, None),
                _as_three_row("total_over_5_5", "Total Goals Over 5.5", _pct(match_total_over[5.5], match_den), None, None),

                _as_three_row("btts_1", "Both Teams to Score 1+", _pct(match_btts[1], match_den), None, None),
                _as_three_row("btts_2", "Both Teams to Score 2+", _pct(match_btts[2], match_den), None, None),
                _as_three_row("btts_3", "Both Teams to Score 3+", _pct(match_btts[3], match_den), None, None),

                _as_three_row("win_over_1_5", "Win & Over 1.5 Goals", None,
                              _pct(tget(home_team, ["rt", "win_over", 1.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_over", 1.5], 0), den_away)),
                _as_three_row("win_over_2_5", "Win & Over 2.5 Goals", None,
                              _pct(tget(home_team, ["rt", "win_over", 2.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_over", 2.5], 0), den_away)),
                _as_three_row("win_over_3_5", "Win & Over 3.5 Goals", None,
                              _pct(tget(home_team, ["rt", "win_over", 3.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_over", 3.5], 0), den_away)),
                _as_three_row("win_over_4_5", "Win & Over 4.5 Goals", None,
                              _pct(tget(home_team, ["rt", "win_over", 4.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_over", 4.5], 0), den_away)),
                _as_three_row("win_over_5_5", "Win & Over 5.5 Goals", None,
                              _pct(tget(home_team, ["rt", "win_over", 5.5], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_over", 5.5], 0), den_away)),

                _as_three_row("win_btts_1", "Win & Both Teams to Score 1+", None,
                              _pct(tget(home_team, ["rt", "win_btts", 1], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_btts", 1], 0), den_away)),
                _as_three_row("win_btts_2", "Win & Both Teams to Score 2+", None,
                              _pct(tget(home_team, ["rt", "win_btts", 2], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_btts", 2], 0), den_away)),
                _as_three_row("win_btts_3", "Win & Both Teams to Score 3+", None,
                              _pct(tget(home_team, ["rt", "win_btts", 3], 0), den_home),
                              _pct(tget(away_team, ["rt", "win_btts", 3], 0), den_away)),
            ],
        }
    )

    # Period sections builder
    def add_period_section(period: str, title: str) -> None:
        den_h = den_home
        den_a = den_away
        sections.append(
            {
                "id": f"{period.lower()}_section",
                "title": title,
                "layout": "three_cols",
                "columns": ["Total", "Home", "Away"],
                "rows": [
                    _as_three_row(f"{period.lower()}_win", "Win", None,
                                 _pct(tget(home_team, ["p", period, "W"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "W"], 0), den_a)),
                    _as_three_row(f"{period.lower()}_draw", "Draw", None,
                                 _pct(tget(home_team, ["p", period, "D"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "D"], 0), den_a)),
                    _as_three_row(f"{period.lower()}_loss", "Loss", None,
                                 _pct(tget(home_team, ["p", period, "L"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "L"], 0), den_a)),

                    _as_three_row(f"{period.lower()}_first_goal", f"First Goal in {title.split('(')[0].strip()}", None,
                                 _pct(tget(home_team, ["p", period, "first_goal"], 0), max(int(tget(home_team, ["p", period, "first_goal_den"], 0)), 0)),
                                 _pct(tget(away_team, ["p", period, "first_goal"], 0), max(int(tget(away_team, ["p", period, "first_goal_den"], 0)), 0))),

                    _as_three_row(f"{period.lower()}_power_play_occurred", "Power Play Occurred", None,
                                 _pct(tget(home_team, ["p", period, "pp_occurred"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "pp_occurred"], 0), den_a)),
                    _as_three_row(f"{period.lower()}_power_play_goal", "Power Play Goal", None,
                                 _pct(tget(home_team, ["p", period, "pp_goal"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "pp_goal"], 0), den_a)),
                    _as_three_row(f"{period.lower()}_penalty_occurred", "Penalty Occurred", None,
                                 _pct(tget(home_team, ["p", period, "penalty_occurred"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "penalty_occurred"], 0), den_a)),

                    _as_three_row(f"{period.lower()}_clean_sheet", "Clean Sheet", None,
                                 _pct(tget(home_team, ["p", period, "clean_sheet"], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "clean_sheet"], 0), den_a)),

                    _as_three_row(f"{period.lower()}_team_over_0_5", "Team Over 0.5 Goals", None,
                                 _pct(tget(home_team, ["p", period, "team_over", 0.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "team_over", 0.5], 0), den_a)),
                    _as_three_row(f"{period.lower()}_team_over_1_5", "Team Over 1.5 Goals", None,
                                 _pct(tget(home_team, ["p", period, "team_over", 1.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "team_over", 1.5], 0), den_a)),
                    _as_three_row(f"{period.lower()}_team_over_2_5", "Team Over 2.5 Goals", None,
                                 _pct(tget(home_team, ["p", period, "team_over", 2.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "team_over", 2.5], 0), den_a)),

                    _as_three_row(f"{period.lower()}_total_over_0_5", "Total Goals Over 0.5", None,
                                 _pct(tget(home_team, ["p", period, "total_over", 0.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "total_over", 0.5], 0), den_a)),
                    _as_three_row(f"{period.lower()}_total_over_1_5", "Total Goals Over 1.5", None,
                                 _pct(tget(home_team, ["p", period, "total_over", 1.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "total_over", 1.5], 0), den_a)),
                    _as_three_row(f"{period.lower()}_total_over_2_5", "Total Goals Over 2.5", None,
                                 _pct(tget(home_team, ["p", period, "total_over", 2.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "total_over", 2.5], 0), den_a)),

                    _as_three_row(f"{period.lower()}_btts_1", "Both Teams to Score 1+", None,
                                 _pct(tget(home_team, ["p", period, "btts", 1], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "btts", 1], 0), den_a)),
                    _as_three_row(f"{period.lower()}_btts_2", "Both Teams to Score 2+", None,
                                 _pct(tget(home_team, ["p", period, "btts", 2], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "btts", 2], 0), den_a)),

                    _as_three_row(f"{period.lower()}_win_over_1_5", "Win & Over 1.5 Goals", None,
                                 _pct(tget(home_team, ["p", period, "win_over", 1.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "win_over", 1.5], 0), den_a)),
                    _as_three_row(f"{period.lower()}_win_over_2_5", "Win & Over 2.5 Goals", None,
                                 _pct(tget(home_team, ["p", period, "win_over", 2.5], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "win_over", 2.5], 0), den_a)),

                    _as_three_row(f"{period.lower()}_win_btts_1", "Win & Both Teams to Score 1+", None,
                                 _pct(tget(home_team, ["p", period, "win_btts", 1], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "win_btts", 1], 0), den_a)),
                    _as_three_row(f"{period.lower()}_win_btts_2", "Win & Both Teams to Score 2+", None,
                                 _pct(tget(home_team, ["p", period, "win_btts", 2], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "win_btts", 2], 0), den_a)),
                    _as_three_row(f"{period.lower()}_win_btts_3", "Win & Both Teams to Score 3+", None,
                                 _pct(tget(home_team, ["p", period, "win_btts", 3], 0), den_h),
                                 _pct(tget(away_team, ["p", period, "win_btts", 3], 0), den_a)),
                ],
            }
        )

    add_period_section("P1", "1st Period (1P)")
    add_period_section("P2", "2nd Period (2P)")
    add_period_section("P3", "3rd Period (3P)")

    # Overtime (OT)
    sections.append(
        {
            "id": "ot_section",
            "title": "Overtime (OT)",
            "layout": "single_col",
            "rows": [
                _as_single_row("ot_win", "Overtime Win", _pct(ot_win, ot_den)),
                _as_single_row("ot_draw", "Overtime Draw (Shootout Reached)", _pct(ot_draw_reach_so, ot_den)),
                _as_single_row("ot_loss", "Overtime Loss", _pct(ot_loss, ot_den)),
            ],
        }
    )

    # Shootout (SO)
    sections.append(
        {
            "id": "so_section",
            "title": "Shootout (SO)",
            "layout": "single_col",
            "rows": [
                _as_single_row("so_win", "Shootout Win", _pct(so_win, so_den)),
                _as_single_row("so_loss", "Shootout Loss", _pct(so_loss, so_den)),
            ],
        }
    )

    # Period Result Transitions
    def trans_rows(title_prefix: str, src: str, tbl: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for from_r in ("W", "D", "L"):
            den = tbl[from_r]["den"]
            rows.append(_as_single_row(f"{src}_{from_r}_to_W", f"{title_prefix} {from_r} \u2192 {src} Win Probability", _pct(tbl[from_r]["W"], den)))
            rows.append(_as_single_row(f"{src}_{from_r}_to_D", f"{title_prefix} {from_r} \u2192 {src} Draw Probability", _pct(tbl[from_r]["D"], den)))
            rows.append(_as_single_row(f"{src}_{from_r}_to_L", f"{title_prefix} {from_r} \u2192 {src} Loss Probability", _pct(tbl[from_r]["L"], den)))
        return rows

    sections.append(
        {
            "id": "period_transitions",
            "title": "Period Result Transitions",
            "layout": "single_col",
            "rows": (
                trans_rows("1P", "2P", trans_1_to_2) +
                trans_rows("2P", "3P", trans_2_to_3)
            ),
        }
    )

    # 3rd Period Clutch Situations
    sections.append(
        {
            "id": "clutch_3p_last3",
            "title": "3rd Period Clutch Situations",
            "subtitle": "(Last 3 Minutes \u00b7 1\u20132 Goal Margin)",
            "layout": "single_col",
            "rows": [
                _as_single_row("clutch_lead1_score", "Leading by 1 Goal \u2013 Score Probability (Last 3 Minutes)", _pct(clutch["leading_1"]["score"], clutch["leading_1"]["den"])),
                _as_single_row("clutch_lead1_concede", "Leading by 1 Goal \u2013 Concede Probability (Last 3 Minutes)", _pct(clutch["leading_1"]["concede"], clutch["leading_1"]["den"])),

                _as_single_row("clutch_lead2_score", "Leading by 2 Goals \u2013 Score Probability (Last 3 Minutes)", _pct(clutch["leading_2"]["score"], clutch["leading_2"]["den"])),
                _as_single_row("clutch_lead2_concede", "Leading by 2 Goals \u2013 Concede Probability (Last 3 Minutes)", _pct(clutch["leading_2"]["concede"], clutch["leading_2"]["den"])),

                _as_single_row("clutch_trail1_score", "Trailing by 1 Goal \u2013 Score Probability (Last 3 Minutes)", _pct(clutch["trailing_1"]["score"], clutch["trailing_1"]["den"])),
                _as_single_row("clutch_trail1_concede", "Trailing by 1 Goal \u2013 Concede Probability (Last 3 Minutes)", _pct(clutch["trailing_1"]["concede"], clutch["trailing_1"]["den"])),

                _as_single_row("clutch_trail2_score", "Trailing by 2 Goals \u2013 Score Probability (Last 3 Minutes)", _pct(clutch["trailing_2"]["score"], clutch["trailing_2"]["den"])),
                _as_single_row("clutch_trail2_concede", "Trailing by 2 Goals \u2013 Concede Probability (Last 3 Minutes)", _pct(clutch["trailing_2"]["concede"], clutch["trailing_2"]["den"])),

                _as_single_row("clutch_tied_score", "Tied Game \u2013 Score Probability (Last 3 Minutes)", _pct(clutch["tied"]["score"], clutch["tied"]["den"])),
                _as_single_row("clutch_tied_concede", "Tied Game \u2013 Concede Probability (Last 3 Minutes)", _pct(clutch["tied"]["concede"], clutch["tied"]["den"])),
            ],
        }
    )

    # 3rd Period Start Score Impact (Regular Time)
    def impact_rows(label_prefix: str, key: str) -> List[Dict[str, Any]]:
        den = start3_impact[key]["den"]
        return [
            _as_single_row(f"start3_{key}_win", f"{label_prefix} \u2192 Win Probability", _pct(start3_impact[key]["W"], den)),
            _as_single_row(f"start3_{key}_draw", f"{label_prefix} \u2192 Draw Probability", _pct(start3_impact[key]["D"], den)),
            _as_single_row(f"start3_{key}_loss", f"{label_prefix} \u2192 Loss Probability", _pct(start3_impact[key]["L"], den)),
        ]

    sections.append(
        {
            "id": "start3_impact",
            "title": "3rd Period Start Score Impact (Regular Time)",
            "layout": "single_col",
            "rows": (
                impact_rows("Leading at 3P Start", "leading") +
                impact_rows("Tied at 3P Start", "tied") +
                impact_rows("Trailing at 3P Start", "trailing")
            ),
        }
    )

    # First Goal Impact (Regular Time)
    def fg_rows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        def add_block(key: str, title: str) -> None:
            den = fg_impact[key]["den"]
            rows.append(_as_single_row(f"{key}_win", f"{title} \u2192 Win Probability", _pct(fg_impact[key]["W"], den)))
            rows.append(_as_single_row(f"{key}_draw", f"{title} \u2192 Draw Probability", _pct(fg_impact[key]["D"], den)))
            rows.append(_as_single_row(f"{key}_loss", f"{title} \u2192 Loss Probability", _pct(fg_impact[key]["L"], den)))
        add_block("1p_first_goal", "1P First Goal")
        add_block("1p_conceded_first_goal", "1P Conceded First Goal")
        add_block("2p_0_0_first_goal", "2P Start 0\u20130 \u2192 First Goal")
        add_block("2p_0_0_first_goal_conceded", "2P Start 0\u20130 \u2192 First Goal Conceded")
        add_block("3p_0_0_first_goal", "3P Start 0\u20130 \u2192 First Goal")
        add_block("3p_0_0_first_goal_conceded", "3P Start 0\u20130 \u2192 First Goal Conceded")
        return rows

    sections.append(
        {
            "id": "first_goal_impact",
            "title": "First Goal Impact (Regular Time)",
            "layout": "single_col",
            "rows": fg_rows(),
        }
    )

    # Goal Timing Distribution
    def timing_section(period: str, title: str) -> Dict[str, Any]:
        den = timing_totals[period]
        # distribution = bin_count / total goals in that period
        rows = []
        for i in range(10):
            st = i * 2
            en = st + 2
            label = f"{st:02d}\u2013{en:02d} min"
            rows.append(_as_single_row(f"{period.lower()}_bin_{i}", label, _pct(timing_bins[period][i], den)))
        return {"id": f"timing_{period.lower()}", "title": title, "layout": "single_col", "rows": rows}

    sections.append(
        {
            "id": "goal_timing_distribution",
            "title": "Goal Timing Distribution",
            "layout": "grouped",
            "groups": [
                timing_section("P1", "1st Period Goal Timing Distribution (2-Minute Intervals)"),
                timing_section("P2", "2nd Period Goal Timing Distribution (2-Minute Intervals)"),
                timing_section("P3", "3rd Period Goal Timing Distribution (2-Minute Intervals)"),
            ],
        }
    )

    return {
        "ok": True,
        "game_id": game_id,
        "league_id": league_id,
        "season": season,
        "sections": sections,
        "meta": {
            "source": "db_empirical_goal_penalty",
            "sample_size_league_season": match_den,
            "sample_size_home_team": den_home,
            "sample_size_away_team": den_away,
            "generated_at": _to_iso_utc_now(),
            "notes": {
                "power_play_definition": "PP occurred is inferred from 'penalty' events (no explicit PP event type in DB).",
                "pp_goal_definition": "PP goal inferred by (comment contains PPG/PP/Power Play) OR (goal within 2 minutes after opponent penalty in same period).",
                "so_winner_definition": "For AP games, shootout winner is decided by hockey_games.score_json if available.",
            },
        },
    }
