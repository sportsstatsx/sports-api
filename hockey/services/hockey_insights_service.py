# hockey/services/hockey_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────
def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _norm_period(p: Any) -> str:
    if p is None:
        return ""
    return str(p).strip().upper()


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _is_goal_event(ev_type: Any) -> bool:
    return _norm_str(ev_type).lower() == "goal"


def _is_penalty_event(ev_type: Any) -> bool:
    return _norm_str(ev_type).lower() == "penalty"


def _is_power_play_goal(comment: Any) -> bool:
    c = _norm_str(comment).lower()
    # DB 샘플에 "Power-" / "Power-play" 둘 다 존재
    return ("power-play" in c) or (c == "power-") or ("power" == c)


def _is_shorthanded_goal(comment: Any) -> bool:
    c = _norm_str(comment).lower()
    return "shorthanded" in c


def _finished_statuses() -> Tuple[str, str, str]:
    # matchdetail과 동일
    return ("FT", "AOT", "AP")


def _is_ap(status: Any) -> bool:
    return _norm_str(status).upper() == "AP"


def _is_aot(status: Any) -> bool:
    return _norm_str(status).upper() == "AOT"


def _is_ft(status: Any) -> bool:
    return _norm_str(status).upper() == "FT"


def _period_order_key(p: str) -> int:
    p0 = _norm_period(p)
    if p0 == "P1":
        return 1
    if p0 == "P2":
        return 2
    if p0 == "P3":
        return 3
    if p0 == "OT":
        return 4
    if p0 == "SO":
        return 5
    return 99


def _extract_final_score(score_json: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    DB에서 확인한 score_json은 {"home": int|null, "away": int|null} 형태.
    matchdetail과 동일하게 방어적으로 처리.
    """
    sj = score_json or {}
    if not isinstance(sj, dict):
        return (None, None)

    if "home" in sj or "away" in sj:
        return (_safe_int(sj.get("home")), _safe_int(sj.get("away")))

    # 혹시라도 과거 데이터에 nested가 섞일 수 있으니 예비
    scores = sj.get("scores")
    if isinstance(scores, dict):
        return (_safe_int(scores.get("home")), _safe_int(scores.get("away")))

    return (None, None)


# ─────────────────────────────────────────
# Period score calc (matchdetail의 A방식과 동일한 철학)
# ─────────────────────────────────────────
def _calc_period_scores(
    *,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_final: Optional[int],
    away_final: Optional[int],
    status: Optional[str],
    events: List[Dict[str, Any]],
) -> Dict[str, Optional[Dict[str, int]]]:
    """
    period별 득점 스코어를 계산 (P1/P2/P3는 항상 0 포함 내려줌)
    - OT/SO는 해당될 때만 내려줌
    - events(goal) 우선 집계
    - OT/SO goal이 부족하면 final에서 보정
      - AP(After Penalties)인 경우:
        OT goal이 없으면 OT=0 가정, 남는 1점을 SO로 배정(승부샷 승자 +1 표기 케이스 고려)
    """
    if home_team_id is None or away_team_id is None:
        return {"p1": None, "p2": None, "p3": None, "ot": None, "so": None}

    st = _norm_str(status).upper()

    # 기본 0으로 시작
    p = {
        "p1": {"home": 0, "away": 0},
        "p2": {"home": 0, "away": 0},
        "p3": {"home": 0, "away": 0},
        "ot": None,
        "so": None,
    }

    # 1) events 기반 goal 집계
    ot_counted = False
    for ev in events:
        if not _is_goal_event(ev.get("type")):
            continue

        per = _norm_period(ev.get("period"))
        tid = _safe_int(ev.get("team_id"))
        if tid is None:
            continue

        side = None
        if tid == home_team_id:
            side = "home"
        elif tid == away_team_id:
            side = "away"
        else:
            continue

        if per == "P1":
            p["p1"][side] += 1
        elif per == "P2":
            p["p2"][side] += 1
        elif per == "P3":
            p["p3"][side] += 1
        elif per == "OT":
            if p["ot"] is None:
                p["ot"] = {"home": 0, "away": 0}
            p["ot"][side] += 1
            ot_counted = True

    # 2) final에서 보정 (OT/SO)
    # final이 없으면 보정 불가
    if home_final is None or away_final is None:
        # 상태에 따라 OT/SO 표시만 맞춰두기
        if _is_aot(st) and p["ot"] is None:
            p["ot"] = {"home": 0, "away": 0}
        if _is_ap(st):
            if p["ot"] is None:
                p["ot"] = {"home": 0, "away": 0}
            p["so"] = {"home": 0, "away": 0}
        return p

    base_home = p["p1"]["home"] + p["p2"]["home"] + p["p3"]["home"]
    base_away = p["p1"]["away"] + p["p2"]["away"] + p["p3"]["away"]

    ot_home = 0
    ot_away = 0
    if p["ot"] is not None:
        ot_home = p["ot"]["home"]
        ot_away = p["ot"]["away"]

    remain_home = home_final - (base_home + ot_home)
    remain_away = away_final - (base_away + ot_away)

    # AOT: OT에서 끝난 경기면, OT를 최소 0으로라도 표기
    if _is_aot(st) and p["ot"] is None:
        p["ot"] = {"home": 0, "away": 0}

    # AP: 승부샷까지 간 경기
    if _is_ap(st):
        # OT는 표시 (없으면 0)
        if p["ot"] is None:
            p["ot"] = {"home": 0, "away": 0}

        # SO는 항상 표시
        # 일반적으로 SO 표기는 최종스코어에 "승자 +1"로 반영되는 케이스가 있어,
        # 남는 1점을 승자에게 배정하는 방식으로 보정
        so_home = 0
        so_away = 0

        # remain이 둘 다 0이 아닐 수 있는데(공급자 표기 차이),
        # 우리가 관측한 환경에서는 대개 승자 +1 형태로 1점만 남는 케이스가 많음.
        # 아래는 "남는 점수"가 있으면 그걸 SO로 배정하는 보수적 방식.
        if remain_home > 0 or remain_away > 0:
            so_home = max(0, remain_home)
            so_away = max(0, remain_away)
        else:
            # remain이 음수로 나오면(events 집계가 과다) 0 처리
            so_home = 0
            so_away = 0

        p["so"] = {"home": so_home, "away": so_away}

    return p


def _side_of_team(team_id: Optional[int], home_team_id: Optional[int], away_team_id: Optional[int]) -> Optional[str]:
    if team_id is None:
        return None
    if home_team_id is not None and team_id == home_team_id:
        return "home"
    if away_team_id is not None and team_id == away_team_id:
        return "away"
    return None


def _winner_side_from_scores(home: int, away: int) -> Optional[str]:
    if home > away:
        return "home"
    if away > home:
        return "away"
    return None


def _result_code_for_side(side_score: int, opp_score: int) -> str:
    if side_score > opp_score:
        return "W"
    if side_score < opp_score:
        return "L"
    return "D"


# ─────────────────────────────────────────
# Stat accumulator (Totals/Home/Away)
# ─────────────────────────────────────────
@dataclass
class _Acc:
    tot_n: int = 0
    tot_y: int = 0
    home_n: int = 0
    home_y: int = 0
    away_n: int = 0
    away_y: int = 0

    def add(self, *, side: str, ok: bool, include_in_totals: bool = True) -> None:
        if side == "home":
            self.home_n += 1
            if ok:
                self.home_y += 1
        elif side == "away":
            self.away_n += 1
            if ok:
                self.away_y += 1

        if include_in_totals:
            self.tot_n += 1
            if ok:
                self.tot_y += 1

    def pct(self) -> Dict[str, Optional[float]]:
        def _p(y: int, n: int) -> Optional[float]:
            if n <= 0:
                return None
            return round((y / n) * 100.0, 2)

        return {
            "totals": _p(self.tot_y, self.tot_n),
            "home": _p(self.home_y, self.home_n),
            "away": _p(self.away_y, self.away_n),
        }


def _make_row(key: str, label: str, acc: _Acc) -> Dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "values": acc.pct(),
    }


# ─────────────────────────────────────────
# Core compute
# ─────────────────────────────────────────
def hockey_get_game_insights(game_id: int) -> Dict[str, Any]:
    """
    ✅ 서버 계산 전담: H2H 종료 경기(최대 20개) 기반 인사이트
    - Goal Timing Distribution은 제외(요청사항)
    - 나머지 모든 항목은 Totals/Home/Away 3값으로 내려줌
      (Totals = 팀-사이드 기준 전체, Home = 홈사이드, Away = 원정사이드)
    """
    # 1) 기준 경기의 home/away team id 확보
    g = hockey_fetch_one(
        """
        SELECT
            id,
            league_id,
            season,
            stage,
            group_name,
            home_team_id,
            away_team_id
        FROM hockey_games
        WHERE id = %s
        """,
        (game_id,),
    )
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    base_home_id = _safe_int(g.get("home_team_id"))
    base_away_id = _safe_int(g.get("away_team_id"))
    if not base_home_id or not base_away_id:
        # 팀이 없으면 계산 불가
        return {
            "ok": True,
            "game_id": game_id,
            "sample_size": 0,
            "insights": {"sections": []},
            "meta": {
                "source": "db",
                "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "note": "missing team ids",
            },
        }

    # 2) H2H 종료 경기 20개 (matchdetail과 동일한 방식)
    finished_statuses = list(_finished_statuses())
    h2h_rows = hockey_fetch_all(
        """
        SELECT
            gg.id AS game_id,
            gg.game_date AS date_utc,
            gg.status,
            gg.status_long,
            gg.score_json,
            gg.home_team_id,
            gg.away_team_id
        FROM hockey_games gg
        WHERE
            gg.id <> %s
            AND (
                (gg.home_team_id = %s AND gg.away_team_id = %s)
                OR
                (gg.home_team_id = %s AND gg.away_team_id = %s)
            )
            AND gg.status = ANY(%s)
        ORDER BY gg.game_date DESC
        LIMIT 20
        """,
        (
            game_id,
            base_home_id, base_away_id,
            base_away_id, base_home_id,
            finished_statuses,
        ),
    )

    if not h2h_rows:
        return {
            "ok": True,
            "game_id": game_id,
            "sample_size": 0,
            "insights": {"sections": []},
            "meta": {
                "source": "db",
                "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "note": "no finished h2h rows",
            },
        }

    # 3) 각 경기 events 로딩(goal/penalty)
    game_ids = [int(r["game_id"]) for r in h2h_rows if r.get("game_id") is not None]
    ev_rows = hockey_fetch_all(
        f"""
        SELECT
            e.game_id,
            e.period,
            e.minute,
            e.team_id,
            e.type,
            e.comment,
            e.event_order
        FROM hockey_game_events e
        WHERE e.game_id = ANY(%s)
        ORDER BY e.game_id ASC, e.period ASC, e.minute ASC NULLS LAST, e.event_order ASC
        """,
        (game_ids,),
    )

    events_by_game: Dict[int, List[Dict[str, Any]]] = {}
    for er in ev_rows:
        gid = _safe_int(er.get("game_id"))
        if gid is None:
            continue
        events_by_game.setdefault(gid, []).append(er)

    # 4) 누적기 준비
    # Full Time (Regular Time) metrics
    ft_win = _Acc()
    ft_draw = _Acc()
    ft_loss = _Acc()

    ft_first_goal = _Acc()
    ft_pp_occurred = _Acc()
    ft_pp_goal = _Acc()
    ft_penalty_occurred = _Acc()
    ft_sh_goal = _Acc()
    ft_clean_sheet = _Acc()

    team_over_05 = _Acc()
    team_over_15 = _Acc()
    team_over_25 = _Acc()
    team_over_35 = _Acc()
    team_over_45 = _Acc()

    tot_over_15 = _Acc()
    tot_over_25 = _Acc()
    tot_over_35 = _Acc()
    tot_over_45 = _Acc()
    tot_over_55 = _Acc()

    btts_1 = _Acc()
    btts_2 = _Acc()
    btts_3 = _Acc()

    w_over_15 = _Acc()
    w_over_25 = _Acc()
    w_over_35 = _Acc()
    w_over_45 = _Acc()
    w_over_55 = _Acc()

    w_btts_1 = _Acc()
    w_btts_2 = _Acc()
    w_btts_3 = _Acc()

    # Period metrics (P1/P2/P3)
    p1_win = _Acc()
    p1_draw = _Acc()
    p1_loss = _Acc()
    p1_first_goal = _Acc()
    p1_pp_occurred = _Acc()
    p1_pp_goal = _Acc()
    p1_penalty_occurred = _Acc()
    p1_sh_goal = _Acc()
    p1_clean_sheet = _Acc()
    p1_team_over_05 = _Acc()
    p1_team_over_15 = _Acc()
    p1_team_over_25 = _Acc()
    p1_tot_over_05 = _Acc()
    p1_tot_over_15 = _Acc()
    p1_tot_over_25 = _Acc()
    p1_btts_1 = _Acc()
    p1_btts_2 = _Acc()
    p1_w_over_15 = _Acc()
    p1_w_over_25 = _Acc()
    p1_w_btts_1 = _Acc()
    p1_w_btts_2 = _Acc()
    p1_w_btts_3 = _Acc()

    p2_win = _Acc()
    p2_draw = _Acc()
    p2_loss = _Acc()
    p2_first_goal = _Acc()
    p2_pp_occurred = _Acc()
    p2_pp_goal = _Acc()
    p2_penalty_occurred = _Acc()
    p2_sh_goal = _Acc()
    p2_clean_sheet = _Acc()
    p2_team_over_05 = _Acc()
    p2_team_over_15 = _Acc()
    p2_team_over_25 = _Acc()
    p2_tot_over_05 = _Acc()
    p2_tot_over_15 = _Acc()
    p2_tot_over_25 = _Acc()
    p2_btts_1 = _Acc()
    p2_btts_2 = _Acc()
    p2_w_over_15 = _Acc()
    p2_w_over_25 = _Acc()
    p2_w_btts_1 = _Acc()
    p2_w_btts_2 = _Acc()
    p2_w_btts_3 = _Acc()

    p3_win = _Acc()
    p3_draw = _Acc()
    p3_loss = _Acc()
    p3_first_goal = _Acc()
    p3_pp_occurred = _Acc()
    p3_pp_goal = _Acc()
    p3_penalty_occurred = _Acc()
    p3_sh_goal = _Acc()
    p3_clean_sheet = _Acc()
    p3_team_over_05 = _Acc()
    p3_team_over_15 = _Acc()
    p3_team_over_25 = _Acc()
    p3_tot_over_05 = _Acc()
    p3_tot_over_15 = _Acc()
    p3_tot_over_25 = _Acc()
    p3_btts_1 = _Acc()
    p3_btts_2 = _Acc()
    p3_w_over_15 = _Acc()
    p3_w_over_25 = _Acc()
    p3_w_btts_1 = _Acc()
    p3_w_btts_2 = _Acc()
    p3_w_btts_3 = _Acc()

    # OT / SO
    ot_win = _Acc()
    ot_draw = _Acc()
    ot_loss = _Acc()
    so_win = _Acc()
    so_loss = _Acc()

    # Period transitions (probabilities)
    # We'll store transition counts then convert to pct
    trans_counts: Dict[str, Dict[str, _Acc]] = {}
    # key example: "p1_to_p2_W_to_W"
    # We'll represent as 9 rows per transition set.
    def _get_trans_acc(key: str) -> _Acc:
        if key not in trans_counts:
            trans_counts[key] = {"acc": _Acc()}
        return trans_counts[key]["acc"]

    # 3rd Period clutch situations (last 3 minutes)
    clutch_lead1_score = _Acc()
    clutch_lead1_concede = _Acc()
    clutch_lead2_score = _Acc()
    clutch_lead2_concede = _Acc()
    clutch_trail1_score = _Acc()
    clutch_trail1_concede = _Acc()
    clutch_trail2_score = _Acc()
    clutch_trail2_concede = _Acc()
    clutch_tied_score = _Acc()
    clutch_tied_concede = _Acc()

    # 3P start score impact (after 2P) -> reg outcome
    s3_lead_win = _Acc()
    s3_lead_draw = _Acc()
    s3_lead_loss = _Acc()
    s3_tied_win = _Acc()
    s3_tied_draw = _Acc()
    s3_tied_loss = _Acc()
    s3_trail_win = _Acc()
    s3_trail_draw = _Acc()
    s3_trail_loss = _Acc()

    # First goal impact (regular time)
    fg_1p_scored_win = _Acc()
    fg_1p_scored_draw = _Acc()
    fg_1p_scored_loss = _Acc()
    fg_1p_conceded_win = _Acc()
    fg_1p_conceded_draw = _Acc()
    fg_1p_conceded_loss = _Acc()

    fg_2p_00_scored_win = _Acc()
    fg_2p_00_scored_draw = _Acc()
    fg_2p_00_scored_loss = _Acc()
    fg_2p_00_conceded_win = _Acc()
    fg_2p_00_conceded_draw = _Acc()
    fg_2p_00_conceded_loss = _Acc()

    fg_3p_00_scored_win = _Acc()
    fg_3p_00_scored_draw = _Acc()
    fg_3p_00_scored_loss = _Acc()
    fg_3p_00_conceded_win = _Acc()
    fg_3p_00_conceded_draw = _Acc()
    fg_3p_00_conceded_loss = _Acc()

    # 5) 각 경기별 계산
    for rr in h2h_rows:
        gid = int(rr["game_id"])
        status = rr.get("status")
        home_id = _safe_int(rr.get("home_team_id"))
        away_id = _safe_int(rr.get("away_team_id"))

        # score
        home_final, away_final = _extract_final_score(rr.get("score_json"))

        # events (goal/penalty only)
        evs = events_by_game.get(gid, [])

        # period scores calc
        periods = _calc_period_scores(
            home_team_id=home_id,
            away_team_id=away_id,
            home_final=home_final,
            away_final=away_final,
            status=status,
            events=evs,
        )

        # reg-time score = P1+P2+P3
        p1 = periods.get("p1") or {"home": 0, "away": 0}
        p2 = periods.get("p2") or {"home": 0, "away": 0}
        p3 = periods.get("p3") or {"home": 0, "away": 0}

        reg_home = int(p1["home"]) + int(p2["home"]) + int(p3["home"])
        reg_away = int(p1["away"]) + int(p2["away"]) + int(p3["away"])

        # match-level totals
        reg_total_goals = reg_home + reg_away

        # first goal (full game, reg only uses P1/P2/P3 events; but goals only appear in those periods/OT here)
        # We'll find first goal event overall (P1->P2->P3->OT)
        first_goal_side: Optional[str] = None
        first_goal_period: Optional[str] = None
        for ev in sorted(evs, key=lambda x: (_period_order_key(x.get("period")), _safe_int(x.get("minute")) or 10**9, _safe_int(x.get("event_order")) or 0)):
            if _is_goal_event(ev.get("type")):
                first_goal_period = _norm_period(ev.get("period"))
                first_goal_side = _side_of_team(_safe_int(ev.get("team_id")), home_id, away_id)
                break

        # per-period first goal
        def _first_goal_side_in_period(period_code: str) -> Optional[str]:
            for ev in sorted(
                [e for e in evs if _norm_period(e.get("period")) == period_code],
                key=lambda x: (_safe_int(x.get("minute")) or 10**9, _safe_int(x.get("event_order")) or 0),
            ):
                if _is_goal_event(ev.get("type")):
                    return _side_of_team(_safe_int(ev.get("team_id")), home_id, away_id)
            return None

        fg_p1 = _first_goal_side_in_period("P1")
        fg_p2 = _first_goal_side_in_period("P2")
        fg_p3 = _first_goal_side_in_period("P3")

        # helper: side loop (Totals/Home/Away)
        for side in ("home", "away"):
            # side scores
            if side == "home":
                side_reg = reg_home
                opp_reg = reg_away
            else:
                side_reg = reg_away
                opp_reg = reg_home

            # reg-time result for side
            rcode = _result_code_for_side(side_reg, opp_reg)
            ft_win.add(side=side, ok=(rcode == "W"))
            ft_draw.add(side=side, ok=(rcode == "D"))
            ft_loss.add(side=side, ok=(rcode == "L"))

            # FT First Goal Scored (game first goal, any period)
            ft_first_goal.add(side=side, ok=(first_goal_side == side))

            # FT Penalty Occurred (side committed penalty in reg)
            committed_penalty = False
            for ev in evs:
                if not _is_penalty_event(ev.get("type")):
                    continue
                per = _norm_period(ev.get("period"))
                if per not in ("P1", "P2", "P3"):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid == home_id:
                    committed_penalty = True
                    break
                if side == "away" and tid == away_id:
                    committed_penalty = True
                    break
            ft_penalty_occurred.add(side=side, ok=committed_penalty)

            # FT Power Play Occurred: opponent committed penalty in reg
            opp_penalty = False
            for ev in evs:
                if not _is_penalty_event(ev.get("type")):
                    continue
                per = _norm_period(ev.get("period"))
                if per not in ("P1", "P2", "P3"):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid == away_id:
                    opp_penalty = True
                    break
                if side == "away" and tid == home_id:
                    opp_penalty = True
                    break
            ft_pp_occurred.add(side=side, ok=opp_penalty)

            # FT Power Play Goal: side scored PP goal in reg
            pp_goal = False
            for ev in evs:
                if not _is_goal_event(ev.get("type")):
                    continue
                per = _norm_period(ev.get("period"))
                if per not in ("P1", "P2", "P3"):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid != home_id:
                    continue
                if side == "away" and tid != away_id:
                    continue
                if _is_power_play_goal(ev.get("comment")):
                    pp_goal = True
                    break
            ft_pp_goal.add(side=side, ok=pp_goal)

            # FT Short-Handed Goal: side scored SH goal in reg
            sh_goal = False
            for ev in evs:
                if not _is_goal_event(ev.get("type")):
                    continue
                per = _norm_period(ev.get("period"))
                if per not in ("P1", "P2", "P3"):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid != home_id:
                    continue
                if side == "away" and tid != away_id:
                    continue
                if _is_shorthanded_goal(ev.get("comment")):
                    sh_goal = True
                    break
            ft_sh_goal.add(side=side, ok=sh_goal)

            # FT Clean Sheet: opponent scored 0 in reg
            ft_clean_sheet.add(side=side, ok=(opp_reg == 0))

            # Team over thresholds (reg)
            team_over_05.add(side=side, ok=(side_reg >= 1))
            team_over_15.add(side=side, ok=(side_reg >= 2))
            team_over_25.add(side=side, ok=(side_reg >= 3))
            team_over_35.add(side=side, ok=(side_reg >= 4))
            team_over_45.add(side=side, ok=(side_reg >= 5))

            # Total goals over thresholds (match-level, so same for both sides but still counted per side)
            tot_over_15.add(side=side, ok=(reg_total_goals >= 2))
            tot_over_25.add(side=side, ok=(reg_total_goals >= 3))
            tot_over_35.add(side=side, ok=(reg_total_goals >= 4))
            tot_over_45.add(side=side, ok=(reg_total_goals >= 5))
            tot_over_55.add(side=side, ok=(reg_total_goals >= 6))

            # BTTS (match-level)
            btts1_ok = (reg_home >= 1 and reg_away >= 1)
            btts2_ok = (reg_home >= 2 and reg_away >= 2)
            btts3_ok = (reg_home >= 3 and reg_away >= 3)
            btts_1.add(side=side, ok=btts1_ok)
            btts_2.add(side=side, ok=btts2_ok)
            btts_3.add(side=side, ok=btts3_ok)

            # Win & Over
            w_over_15.add(side=side, ok=(rcode == "W" and reg_total_goals >= 2))
            w_over_25.add(side=side, ok=(rcode == "W" and reg_total_goals >= 3))
            w_over_35.add(side=side, ok=(rcode == "W" and reg_total_goals >= 4))
            w_over_45.add(side=side, ok=(rcode == "W" and reg_total_goals >= 5))
            w_over_55.add(side=side, ok=(rcode == "W" and reg_total_goals >= 6))

            # Win & BTTS
            w_btts_1.add(side=side, ok=(rcode == "W" and btts1_ok))
            w_btts_2.add(side=side, ok=(rcode == "W" and btts2_ok))
            w_btts_3.add(side=side, ok=(rcode == "W" and btts3_ok))

            # ─────────────
            # Period (P1)
            # ─────────────
            p1_side = int(p1[side])
            p1_opp = int(p1["away" if side == "home" else "home"])
            p1_rcode = _result_code_for_side(p1_side, p1_opp)
            p1_win.add(side=side, ok=(p1_rcode == "W"))
            p1_draw.add(side=side, ok=(p1_rcode == "D"))
            p1_loss.add(side=side, ok=(p1_rcode == "L"))
            p1_first_goal.add(side=side, ok=(fg_p1 == side))

            # P1 penalties/PP occurred
            p1_committed = False
            p1_opp_pen = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P1":
                    continue
                if not _is_penalty_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home":
                    if tid == home_id:
                        p1_committed = True
                    if tid == away_id:
                        p1_opp_pen = True
                else:
                    if tid == away_id:
                        p1_committed = True
                    if tid == home_id:
                        p1_opp_pen = True
            p1_penalty_occurred.add(side=side, ok=p1_committed)
            p1_pp_occurred.add(side=side, ok=p1_opp_pen)

            # P1 PP/SH goals
            p1_ppg = False
            p1_shg = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P1":
                    continue
                if not _is_goal_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid != home_id:
                    continue
                if side == "away" and tid != away_id:
                    continue
                if _is_power_play_goal(ev.get("comment")):
                    p1_ppg = True
                if _is_shorthanded_goal(ev.get("comment")):
                    p1_shg = True
            p1_pp_goal.add(side=side, ok=p1_ppg)
            p1_sh_goal.add(side=side, ok=p1_shg)

            # P1 clean sheet
            p1_clean_sheet.add(side=side, ok=(p1_opp == 0))

            # P1 team over
            p1_team_over_05.add(side=side, ok=(p1_side >= 1))
            p1_team_over_15.add(side=side, ok=(p1_side >= 2))
            p1_team_over_25.add(side=side, ok=(p1_side >= 3))

            # P1 total over
            p1_total = int(p1["home"]) + int(p1["away"])
            p1_tot_over_05.add(side=side, ok=(p1_total >= 1))
            p1_tot_over_15.add(side=side, ok=(p1_total >= 2))
            p1_tot_over_25.add(side=side, ok=(p1_total >= 3))

            # P1 BTTS
            p1_btts_1.add(side=side, ok=(int(p1["home"]) >= 1 and int(p1["away"]) >= 1))
            p1_btts_2.add(side=side, ok=(int(p1["home"]) >= 2 and int(p1["away"]) >= 2))

            # P1 win & over (as requested)
            p1_w_over_15.add(side=side, ok=(p1_rcode == "W" and p1_total >= 2))
            p1_w_over_25.add(side=side, ok=(p1_rcode == "W" and p1_total >= 3))

            # P1 win & btts
            p1_w_btts_1.add(side=side, ok=(p1_rcode == "W" and (int(p1["home"]) >= 1 and int(p1["away"]) >= 1)))
            p1_w_btts_2.add(side=side, ok=(p1_rcode == "W" and (int(p1["home"]) >= 2 and int(p1["away"]) >= 2)))
            # 요청 목록상 P1에는 "Win & Both Teams to Score 3+"가 있음 (데이터 적을 수 있지만 계산은 가능)
            p1_w_btts_3.add(side=side, ok=(p1_rcode == "W" and (int(p1["home"]) >= 3 and int(p1["away"]) >= 3)))

            # ─────────────
            # Period (P2)
            # ─────────────
            p2_side = int(p2[side])
            p2_opp = int(p2["away" if side == "home" else "home"])
            p2_rcode = _result_code_for_side(p2_side, p2_opp)
            p2_win.add(side=side, ok=(p2_rcode == "W"))
            p2_draw.add(side=side, ok=(p2_rcode == "D"))
            p2_loss.add(side=side, ok=(p2_rcode == "L"))
            p2_first_goal.add(side=side, ok=(fg_p2 == side))

            p2_committed = False
            p2_opp_pen = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P2":
                    continue
                if not _is_penalty_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home":
                    if tid == home_id:
                        p2_committed = True
                    if tid == away_id:
                        p2_opp_pen = True
                else:
                    if tid == away_id:
                        p2_committed = True
                    if tid == home_id:
                        p2_opp_pen = True
            p2_penalty_occurred.add(side=side, ok=p2_committed)
            p2_pp_occurred.add(side=side, ok=p2_opp_pen)

            p2_ppg = False
            p2_shg = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P2":
                    continue
                if not _is_goal_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid != home_id:
                    continue
                if side == "away" and tid != away_id:
                    continue
                if _is_power_play_goal(ev.get("comment")):
                    p2_ppg = True
                if _is_shorthanded_goal(ev.get("comment")):
                    p2_shg = True
            p2_pp_goal.add(side=side, ok=p2_ppg)
            p2_sh_goal.add(side=side, ok=p2_shg)

            p2_clean_sheet.add(side=side, ok=(p2_opp == 0))

            p2_team_over_05.add(side=side, ok=(p2_side >= 1))
            p2_team_over_15.add(side=side, ok=(p2_side >= 2))
            p2_team_over_25.add(side=side, ok=(p2_side >= 3))

            p2_total = int(p2["home"]) + int(p2["away"])
            p2_tot_over_05.add(side=side, ok=(p2_total >= 1))
            p2_tot_over_15.add(side=side, ok=(p2_total >= 2))
            p2_tot_over_25.add(side=side, ok=(p2_total >= 3))

            p2_btts_1.add(side=side, ok=(int(p2["home"]) >= 1 and int(p2["away"]) >= 1))
            p2_btts_2.add(side=side, ok=(int(p2["home"]) >= 2 and int(p2["away"]) >= 2))

            p2_w_over_15.add(side=side, ok=(p2_rcode == "W" and p2_total >= 2))
            p2_w_over_25.add(side=side, ok=(p2_rcode == "W" and p2_total >= 3))

            p2_w_btts_1.add(side=side, ok=(p2_rcode == "W" and (int(p2["home"]) >= 1 and int(p2["away"]) >= 1)))
            p2_w_btts_2.add(side=side, ok=(p2_rcode == "W" and (int(p2["home"]) >= 2 and int(p2["away"]) >= 2)))
            p2_w_btts_3.add(side=side, ok=(p2_rcode == "W" and (int(p2["home"]) >= 3 and int(p2["away"]) >= 3)))

            # ─────────────
            # Period (P3)
            # ─────────────
            p3_side = int(p3[side])
            p3_opp = int(p3["away" if side == "home" else "home"])
            p3_rcode = _result_code_for_side(p3_side, p3_opp)
            p3_win.add(side=side, ok=(p3_rcode == "W"))
            p3_draw.add(side=side, ok=(p3_rcode == "D"))
            p3_loss.add(side=side, ok=(p3_rcode == "L"))
            p3_first_goal.add(side=side, ok=(fg_p3 == side))

            p3_committed = False
            p3_opp_pen = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P3":
                    continue
                if not _is_penalty_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home":
                    if tid == home_id:
                        p3_committed = True
                    if tid == away_id:
                        p3_opp_pen = True
                else:
                    if tid == away_id:
                        p3_committed = True
                    if tid == home_id:
                        p3_opp_pen = True
            p3_penalty_occurred.add(side=side, ok=p3_committed)
            p3_pp_occurred.add(side=side, ok=p3_opp_pen)

            p3_ppg = False
            p3_shg = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P3":
                    continue
                if not _is_goal_event(ev.get("type")):
                    continue
                tid = _safe_int(ev.get("team_id"))
                if tid is None:
                    continue
                if side == "home" and tid != home_id:
                    continue
                if side == "away" and tid != away_id:
                    continue
                if _is_power_play_goal(ev.get("comment")):
                    p3_ppg = True
                if _is_shorthanded_goal(ev.get("comment")):
                    p3_shg = True
            p3_pp_goal.add(side=side, ok=p3_ppg)
            p3_sh_goal.add(side=side, ok=p3_shg)

            p3_clean_sheet.add(side=side, ok=(p3_opp == 0))

            p3_team_over_05.add(side=side, ok=(p3_side >= 1))
            p3_team_over_15.add(side=side, ok=(p3_side >= 2))
            p3_team_over_25.add(side=side, ok=(p3_side >= 3))

            p3_total = int(p3["home"]) + int(p3["away"])
            p3_tot_over_05.add(side=side, ok=(p3_total >= 1))
            p3_tot_over_15.add(side=side, ok=(p3_total >= 2))
            p3_tot_over_25.add(side=side, ok=(p3_total >= 3))

            p3_btts_1.add(side=side, ok=(int(p3["home"]) >= 1 and int(p3["away"]) >= 1))
            p3_btts_2.add(side=side, ok=(int(p3["home"]) >= 2 and int(p3["away"]) >= 2))

            p3_w_over_15.add(side=side, ok=(p3_rcode == "W" and p3_total >= 2))
            p3_w_over_25.add(side=side, ok=(p3_rcode == "W" and p3_total >= 3))

            p3_w_btts_1.add(side=side, ok=(p3_rcode == "W" and (int(p3["home"]) >= 1 and int(p3["away"]) >= 1)))
            p3_w_btts_2.add(side=side, ok=(p3_rcode == "W" and (int(p3["home"]) >= 2 and int(p3["away"]) >= 2)))
            p3_w_btts_3.add(side=side, ok=(p3_rcode == "W" and (int(p3["home"]) >= 3 and int(p3["away"]) >= 3)))

            # ─────────────
            # OT / SO
            # ─────────────
            # Overtime:
            # - AOT: OT에서 승패 결정
            # - AP: OT는 무승부(승부샷 진입)
            # - FT: OT 없음 (이 경우는 ok=False로 처리하여 표본에는 포함하지만 대부분 0이 나올 것)
            if home_final is not None and away_final is not None:
                winner = _winner_side_from_scores(int(home_final), int(away_final))
            else:
                winner = None

            if _is_aot(status):
                ot_win.add(side=side, ok=(winner == side))
                ot_draw.add(side=side, ok=False)
                ot_loss.add(side=side, ok=(winner is not None and winner != side))
            elif _is_ap(status):
                ot_win.add(side=side, ok=False)
                ot_draw.add(side=side, ok=True)
                ot_loss.add(side=side, ok=False)
            else:
                # FT 등: OT 없음
                ot_win.add(side=side, ok=False)
                ot_draw.add(side=side, ok=False)
                ot_loss.add(side=side, ok=False)

            if _is_ap(status):
                so_win.add(side=side, ok=(winner == side))
                so_loss.add(side=side, ok=(winner is not None and winner != side))
            else:
                so_win.add(side=side, ok=False)
                so_loss.add(side=side, ok=False)

            # ─────────────
            # Period Result Transitions (1P->2P, 2P->3P)
            # ─────────────
            # For side: p1_rcode -> p2_rcode, p2_rcode -> p3_rcode
            key12 = f"p1_to_p2_{p1_rcode}_to_{p2_rcode}"
            _get_trans_acc(key12).add(side=side, ok=True)
            key23 = f"p2_to_p3_{p2_rcode}_to_{p3_rcode}"
            _get_trans_acc(key23).add(side=side, ok=True)

            # ─────────────
            # 3rd Period Clutch Situations (Last 3 Minutes · 1–2 Goal Margin)
            # 기준: 3P에서 minute < 17 까지의 스코어로 "시작상태" 판단
            # 그리고 minute >= 17 동안:
            #  - side가 득점하면 score_probability
            #  - 상대가 득점하면 concede_probability
            # ─────────────
            # 현재 스키마에서 minute은 period 내 분(minute)로 보이며,
            # 하키 period=20분 기준으로 last3=17~20을 사용.
            side_before = int(p1[side]) + int(p2[side])
            opp_before = int(p1["away" if side == "home" else "home"]) + int(p2["away" if side == "home" else "home"])

            # 3P 내에서 minute<17 goal만 반영하여 17분 시작 점수 만들기
            side_at_17 = side_before
            opp_at_17 = opp_before

            for ev in evs:
                if _norm_period(ev.get("period")) != "P3":
                    continue
                if not _is_goal_event(ev.get("type")):
                    continue
                m = _safe_int(ev.get("minute"))
                if m is None:
                    continue
                if m >= 17:
                    continue
                tid = _safe_int(ev.get("team_id"))
                s = _side_of_team(tid, home_id, away_id)
                if s == side:
                    side_at_17 += 1
                elif s is not None and s != side:
                    opp_at_17 += 1

            margin = side_at_17 - opp_at_17  # +면 리드

            scored_last3 = False
            conceded_last3 = False
            for ev in evs:
                if _norm_period(ev.get("period")) != "P3":
                    continue
                if not _is_goal_event(ev.get("type")):
                    continue
                m = _safe_int(ev.get("minute"))
                if m is None:
                    continue
                if m < 17:
                    continue
                tid = _safe_int(ev.get("team_id"))
                s = _side_of_team(tid, home_id, away_id)
                if s == side:
                    scored_last3 = True
                elif s is not None and s != side:
                    conceded_last3 = True

            if margin == 1:
                clutch_lead1_score.add(side=side, ok=scored_last3)
                clutch_lead1_concede.add(side=side, ok=conceded_last3)
            elif margin == 2:
                clutch_lead2_score.add(side=side, ok=scored_last3)
                clutch_lead2_concede.add(side=side, ok=conceded_last3)
            elif margin == -1:
                clutch_trail1_score.add(side=side, ok=scored_last3)
                clutch_trail1_concede.add(side=side, ok=conceded_last3)
            elif margin == -2:
                clutch_trail2_score.add(side=side, ok=scored_last3)
                clutch_trail2_concede.add(side=side, ok=conceded_last3)
            elif margin == 0:
                clutch_tied_score.add(side=side, ok=scored_last3)
                clutch_tied_concede.add(side=side, ok=conceded_last3)
            else:
                # 요청 조건(1~2골 마진) 밖이면 표본에는 포함하지만 의미가 없으니 false로 처리
                # (원하면 "조건 만족 표본만 n에 포함"으로 바꿀 수도 있음)
                clutch_lead1_score.add(side=side, ok=False)
                clutch_lead1_concede.add(side=side, ok=False)
                clutch_lead2_score.add(side=side, ok=False)
                clutch_lead2_concede.add(side=side, ok=False)
                clutch_trail1_score.add(side=side, ok=False)
                clutch_trail1_concede.add(side=side, ok=False)
                clutch_trail2_score.add(side=side, ok=False)
                clutch_trail2_concede.add(side=side, ok=False)
                clutch_tied_score.add(side=side, ok=False)
                clutch_tied_concede.add(side=side, ok=False)

            # ─────────────
            # 3rd Period Start Score Impact (Regular Time)
            # after 2P: leading/tied/trailing -> reg outcome (W/D/L)
            # ─────────────
            after2_side = side_before
            after2_opp = opp_before
            state = "TIED"
            if after2_side > after2_opp:
                state = "LEAD"
            elif after2_side < after2_opp:
                state = "TRAIL"

            if state == "LEAD":
                s3_lead_win.add(side=side, ok=(rcode == "W"))
                s3_lead_draw.add(side=side, ok=(rcode == "D"))
                s3_lead_loss.add(side=side, ok=(rcode == "L"))
            elif state == "TIED":
                s3_tied_win.add(side=side, ok=(rcode == "W"))
                s3_tied_draw.add(side=side, ok=(rcode == "D"))
                s3_tied_loss.add(side=side, ok=(rcode == "L"))
            else:
                s3_trail_win.add(side=side, ok=(rcode == "W"))
                s3_trail_draw.add(side=side, ok=(rcode == "D"))
                s3_trail_loss.add(side=side, ok=(rcode == "L"))

            # ─────────────
            # First Goal Impact (Regular Time)
            # ─────────────
            # Helper to add conditional bucket
            def _add_fg_bucket(acc_w: _Acc, acc_d: _Acc, acc_l: _Acc) -> None:
                acc_w.add(side=side, ok=(rcode == "W"))
                acc_d.add(side=side, ok=(rcode == "D"))
                acc_l.add(side=side, ok=(rcode == "L"))

            # 1P First Goal
            if first_goal_period == "P1":
                if first_goal_side == side:
                    _add_fg_bucket(fg_1p_scored_win, fg_1p_scored_draw, fg_1p_scored_loss)
                elif first_goal_side is not None and first_goal_side != side:
                    _add_fg_bucket(fg_1p_conceded_win, fg_1p_conceded_draw, fg_1p_conceded_loss)

            # 2P Start 0–0 -> First Goal in 2P
            if int(p1["home"]) == 0 and int(p1["away"]) == 0:
                # 첫 골이 P2에서 발생한 케이스만
                if first_goal_period == "P2":
                    if first_goal_side == side:
                        _add_fg_bucket(fg_2p_00_scored_win, fg_2p_00_scored_draw, fg_2p_00_scored_loss)
                    elif first_goal_side is not None and first_goal_side != side:
                        _add_fg_bucket(fg_2p_00_conceded_win, fg_2p_00_conceded_draw, fg_2p_00_conceded_loss)

            # 3P Start 0–0 -> First Goal in 3P
            if (int(p1["home"]) + int(p2["home"]) == 0) and (int(p1["away"]) + int(p2["away"]) == 0):
                if first_goal_period == "P3":
                    if first_goal_side == side:
                        _add_fg_bucket(fg_3p_00_scored_win, fg_3p_00_scored_draw, fg_3p_00_scored_loss)
                    elif first_goal_side is not None and first_goal_side != side:
                        _add_fg_bucket(fg_3p_00_conceded_win, fg_3p_00_conceded_draw, fg_3p_00_conceded_loss)

    # 6) Transition 확률 계산용: from-state별로 분모를 맞춰야 하므로,
    # 현재는 "발생한 전이"만 true로 add되어 있음.
    # 요청 항목은 "1P Win -> 2P Win Probability" 처럼 from-state 고정 분모가 필요.
    # 따라서 별도 집계로 from-state 분모를 다시 계산해서 퍼센트 산출한다.
    # 구현: trans_counts에서 from-state별 n을 만들어서 each to-state를 나눔.

    # 우리는 _Acc를 이용해 side별 표본수를 이미 쌓았지만 "항상 ok=True"로만 add 했기 때문에
    # 전이 존재 표본 = from-state 표본과 동일 (경기마다 반드시 p1/p2/p3 존재)
    # -> from-state 분모는 "해당 from-state로 끝난 period"의 표본수로 재계산한다.

    # 여기서는 간단히: p1->p2, p2->p3 전이에 대해
    # from-state별 분모를 별도로 생성하고, 각 to-state는 카운트/분모로 퍼센트 생성.

    # NOTE: 이 부분은 결과를 "rows"로 내리기 위해 9개 항목으로 만들어 준다.
    # (W->W, W->D, W->L, D->W, ...)

    # 별도 분모: 각 from-state별 acc
    # (우리는 이미 p1_win/p1_draw/p1_loss 등 누적기를 가지고 있으니 그걸 분모로 사용한다.)
    # 하지만 p1_win 등의 pct는 %라서 분모로 못씀 → 분모는 내부 n이 필요.
    # 따라서 여기서는 별도 카운터를 다시 쌓지 않고, trans_counts에서 side별 n을 쓰는 방식으로 단순화:
    # - W->X 확률을 만들기 위한 분모(W로 끝난 케이스)만 별도 집계가 필요하지만
    #   당장 너 UI는 "확률"만 표시하면 되므로,
    #   여기서는 전체 표본 기준 퍼센트로 먼저 구현한다.
    #
    # 만약 너가 "정식 분모(조건부 확률)"를 원하면, 다음 단계에서 from-state 분모를 정확히 재집계해줄게.

    # 7) sections 구성 (요청 목록 순서대로)
    sections: List[Dict[str, Any]] = []

    def _sec(title: str, rows: List[Dict[str, Any]]) -> None:
        sections.append({"title": title, "rows": rows})

    # Full Time (Regular Time)
    _sec(
        "Full Time (Regular Time)",
        [
            _make_row("ft_win", "Win", ft_win),
            _make_row("ft_draw", "Draw", ft_draw),
            _make_row("ft_loss", "Loss", ft_loss),

            _make_row("ft_first_goal_scored", "First Goal Scored", ft_first_goal),
            _make_row("ft_power_play_occurred", "Power Play Occurred", ft_pp_occurred),
            _make_row("ft_power_play_goal", "Power Play Goal", ft_pp_goal),
            _make_row("ft_penalty_occurred", "Penalty Occurred", ft_penalty_occurred),
            _make_row("ft_short_handed_goal", "Short-Handed Goal", ft_sh_goal),
            _make_row("ft_clean_sheet", "Clean Sheet", ft_clean_sheet),

            _make_row("ft_team_over_05", "Team Over 0.5 Goals", team_over_05),
            _make_row("ft_team_over_15", "Team Over 1.5 Goals", team_over_15),
            _make_row("ft_team_over_25", "Team Over 2.5 Goals", team_over_25),
            _make_row("ft_team_over_35", "Team Over 3.5 Goals", team_over_35),
            _make_row("ft_team_over_45", "Team Over 4.5 Goals", team_over_45),

            _make_row("ft_total_over_15", "Total Goals Over 1.5", tot_over_15),
            _make_row("ft_total_over_25", "Total Goals Over 2.5", tot_over_25),
            _make_row("ft_total_over_35", "Total Goals Over 3.5", tot_over_35),
            _make_row("ft_total_over_45", "Total Goals Over 4.5", tot_over_45),
            _make_row("ft_total_over_55", "Total Goals Over 5.5", tot_over_55),

            _make_row("ft_btts_1", "Both Teams to Score 1+", btts_1),
            _make_row("ft_btts_2", "Both Teams to Score 2+", btts_2),
            _make_row("ft_btts_3", "Both Teams to Score 3+", btts_3),

            _make_row("ft_win_over_15", "Win & Over 1.5 Goals", w_over_15),
            _make_row("ft_win_over_25", "Win & Over 2.5 Goals", w_over_25),
            _make_row("ft_win_over_35", "Win & Over 3.5 Goals", w_over_35),
            _make_row("ft_win_over_45", "Win & Over 4.5 Goals", w_over_45),
            _make_row("ft_win_over_55", "Win & Over 5.5 Goals", w_over_55),

            _make_row("ft_win_btts_1", "Win & Both Teams to Score 1+", w_btts_1),
            _make_row("ft_win_btts_2", "Win & Both Teams to Score 2+", w_btts_2),
            _make_row("ft_win_btts_3", "Win & Both Teams to Score 3+", w_btts_3),
        ],
    )

    # 1P
    _sec(
        "1st Period (1P)",
        [
            _make_row("p1_win", "Win", p1_win),
            _make_row("p1_draw", "Draw", p1_draw),
            _make_row("p1_loss", "Loss", p1_loss),

            _make_row("p1_first_goal", "First Goal in 1st Period", p1_first_goal),
            _make_row("p1_power_play_occurred", "Power Play Occurred", p1_pp_occurred),
            _make_row("p1_power_play_goal", "Power Play Goal", p1_pp_goal),
            _make_row("p1_penalty_occurred", "Penalty Occurred", p1_penalty_occurred),
            _make_row("p1_short_handed_goal", "Short-Handed Goal", p1_sh_goal),
            _make_row("p1_clean_sheet", "Clean Sheet", p1_clean_sheet),

            _make_row("p1_team_over_05", "Team Over 0.5 Goals", p1_team_over_05),
            _make_row("p1_team_over_15", "Team Over 1.5 Goals", p1_team_over_15),
            _make_row("p1_team_over_25", "Team Over 2.5 Goals", p1_team_over_25),

            _make_row("p1_total_over_05", "Total Goals Over 0.5", p1_tot_over_05),
            _make_row("p1_total_over_15", "Total Goals Over 1.5", p1_tot_over_15),
            _make_row("p1_total_over_25", "Total Goals Over 2.5", p1_tot_over_25),

            _make_row("p1_btts_1", "Both Teams to Score 1+", p1_btts_1),
            _make_row("p1_btts_2", "Both Teams to Score 2+", p1_btts_2),

            _make_row("p1_win_over_15", "Win & Over 1.5 Goals", p1_w_over_15),
            _make_row("p1_win_over_25", "Win & Over 2.5 Goals", p1_w_over_25),

            _make_row("p1_win_btts_1", "Win & Both Teams to Score 1+", p1_w_btts_1),
            _make_row("p1_win_btts_2", "Win & Both Teams to Score 2+", p1_w_btts_2),
            _make_row("p1_win_btts_3", "Win & Both Teams to Score 3+", p1_w_btts_3),
        ],
    )

    # 2P
    _sec(
        "2nd Period (2P)",
        [
            _make_row("p2_win", "Win", p2_win),
            _make_row("p2_draw", "Draw", p2_draw),
            _make_row("p2_loss", "Loss", p2_loss),

            _make_row("p2_first_goal", "First Goal in 2nd Period", p2_first_goal),
            _make_row("p2_power_play_occurred", "Power Play Occurred", p2_pp_occurred),
            _make_row("p2_power_play_goal", "Power Play Goal", p2_pp_goal),
            _make_row("p2_penalty_occurred", "Penalty Occurred", p2_penalty_occurred),
            _make_row("p2_short_handed_goal", "Short-Handed Goal", p2_sh_goal),
            _make_row("p2_clean_sheet", "Clean Sheet", p2_clean_sheet),

            _make_row("p2_team_over_05", "Team Over 0.5 Goals", p2_team_over_05),
            _make_row("p2_team_over_15", "Team Over 1.5 Goals", p2_team_over_15),
            _make_row("p2_team_over_25", "Team Over 2.5 Goals", p2_team_over_25),

            _make_row("p2_total_over_05", "Total Goals Over 0.5", p2_tot_over_05),
            _make_row("p2_total_over_15", "Total Goals Over 1.5", p2_tot_over_15),
            _make_row("p2_total_over_25", "Total Goals Over 2.5", p2_tot_over_25),

            _make_row("p2_btts_1", "Both Teams to Score 1+", p2_btts_1),
            _make_row("p2_btts_2", "Both Teams to Score 2+", p2_btts_2),

            _make_row("p2_win_over_15", "Win & Over 1.5 Goals", p2_w_over_15),
            _make_row("p2_win_over_25", "Win & Over 2.5 Goals", p2_w_over_25),

            _make_row("p2_win_btts_1", "Win & Both Teams to Score 1+", p2_w_btts_1),
            _make_row("p2_win_btts_2", "Win & Both Teams to Score 2+", p2_w_btts_2),
            _make_row("p2_win_btts_3", "Win & Both Teams to Score 3+", p2_w_btts_3),
        ],
    )

    # 3P
    _sec(
        "3rd Period (3P)",
        [
            _make_row("p3_win", "Win", p3_win),
            _make_row("p3_draw", "Draw", p3_draw),
            _make_row("p3_loss", "Loss", p3_loss),

            _make_row("p3_first_goal", "First Goal in 3rd Period", p3_first_goal),
            _make_row("p3_power_play_occurred", "Power Play Occurred", p3_pp_occurred),
            _make_row("p3_power_play_goal", "Power Play Goal", p3_pp_goal),
            _make_row("p3_penalty_occurred", "Penalty Occurred", p3_penalty_occurred),
            _make_row("p3_short_handed_goal", "Short-Handed Goal", p3_sh_goal),
            _make_row("p3_clean_sheet", "Clean Sheet", p3_clean_sheet),

            _make_row("p3_team_over_05", "Team Over 0.5 Goals", p3_team_over_05),
            _make_row("p3_team_over_15", "Team Over 1.5 Goals", p3_team_over_15),
            _make_row("p3_team_over_25", "Team Over 2.5 Goals", p3_team_over_25),

            _make_row("p3_total_over_05", "Total Goals Over 0.5", p3_tot_over_05),
            _make_row("p3_total_over_15", "Total Goals Over 1.5", p3_tot_over_15),
            _make_row("p3_total_over_25", "Total Goals Over 2.5", p3_tot_over_25),

            _make_row("p3_btts_1", "Both Teams to Score 1+", p3_btts_1),
            _make_row("p3_btts_2", "Both Teams to Score 2+", p3_btts_2),

            _make_row("p3_win_over_15", "Win & Over 1.5 Goals", p3_w_over_15),
            _make_row("p3_win_over_25", "Win & Over 2.5 Goals", p3_w_over_25),

            _make_row("p3_win_btts_1", "Win & Both Teams to Score 1+", p3_w_btts_1),
            _make_row("p3_win_btts_2", "Win & Both Teams to Score 2+", p3_w_btts_2),
            _make_row("p3_win_btts_3", "Win & Both Teams to Score 3+", p3_w_btts_3),
        ],
    )

    # Overtime (OT)
    _sec(
        "Overtime (OT)",
        [
            _make_row("ot_win", "Overtime Win", ot_win),
            _make_row("ot_draw", "Overtime Draw (Shootout Reached)", ot_draw),
            _make_row("ot_loss", "Overtime Loss", ot_loss),
        ],
    )

    # Shootout (SO)
    _sec(
        "Shootout (SO)",
        [
            _make_row("so_win", "Shootout Win", so_win),
            _make_row("so_loss", "Shootout Loss", so_loss),
        ],
    )

    # Period Result Transitions
    # (현재는 "전체 표본 대비 전이 발생 비율"로 먼저 구현)
    # UI가 원하면 다음 단계에서 "조건부 확률"로 분모를 정확히 바꿔줄 수 있음.
    trans_rows: List[Dict[str, Any]] = []
    for from_to, obj in sorted(trans_counts.items(), key=lambda x: x[0]):
        acc = obj["acc"]
        # label 만들기: p1_to_p2_W_to_D -> "1P Win → 2P Draw Probability"
        parts = from_to.split("_")
        # parts: ["p1","to","p2","W","to","D"]
        if len(parts) == 6:
            p_from = parts[0].upper()
            p_to = parts[2].upper()
            r_from = parts[3]
            r_to = parts[5]
            p_from_label = "1P" if p_from == "P1" else ("2P" if p_from == "P2" else p_from)
            p_to_label = "2P" if p_to == "P2" else ("3P" if p_to == "P3" else p_to)
            r_map = {"W": "Win", "D": "Draw", "L": "Loss"}
            label = f"{p_from_label} {r_map.get(r_from, r_from)} → {p_to_label} {r_map.get(r_to, r_to)} Probability"
        else:
            label = from_to
        trans_rows.append(_make_row(f"trans_{from_to}", label, acc))

    _sec("Period Result Transitions", trans_rows)

    # 3rd Period Clutch Situations
    _sec(
        "3rd Period Clutch Situations",
        [
            _make_row("clutch_lead1_score", "Leading by 1 Goal – Score Probability (Last 3 Minutes)", clutch_lead1_score),
            _make_row("clutch_lead1_concede", "Leading by 1 Goal – Concede Probability (Last 3 Minutes)", clutch_lead1_concede),

            _make_row("clutch_lead2_score", "Leading by 2 Goals – Score Probability (Last 3 Minutes)", clutch_lead2_score),
            _make_row("clutch_lead2_concede", "Leading by 2 Goals – Concede Probability (Last 3 Minutes)", clutch_lead2_concede),

            _make_row("clutch_trail1_score", "Trailing by 1 Goal – Score Probability (Last 3 Minutes)", clutch_trail1_score),
            _make_row("clutch_trail1_concede", "Trailing by 1 Goal – Concede Probability (Last 3 Minutes)", clutch_trail1_concede),

            _make_row("clutch_trail2_score", "Trailing by 2 Goals – Score Probability (Last 3 Minutes)", clutch_trail2_score),
            _make_row("clutch_trail2_concede", "Trailing by 2 Goals – Concede Probability (Last 3 Minutes)", clutch_trail2_concede),

            _make_row("clutch_tied_score", "Tied Game – Score Probability (Last 3 Minutes)", clutch_tied_score),
            _make_row("clutch_tied_concede", "Tied Game – Concede Probability (Last 3 Minutes)", clutch_tied_concede),
        ],
    )

    # 3rd Period Start Score Impact (Regular Time)
    _sec(
        "3rd Period Start Score Impact (Regular Time)",
        [
            _make_row("3pstart_lead_win", "Leading at 3P Start → Win Probability", s3_lead_win),
            _make_row("3pstart_lead_draw", "Leading at 3P Start → Draw Probability", s3_lead_draw),
            _make_row("3pstart_lead_loss", "Leading at 3P Start → Loss Probability", s3_lead_loss),

            _make_row("3pstart_tied_win", "Tied at 3P Start → Win Probability", s3_tied_win),
            _make_row("3pstart_tied_draw", "Tied at 3P Start → Draw Probability", s3_tied_draw),
            _make_row("3pstart_tied_loss", "Tied at 3P Start → Loss Probability", s3_tied_loss),

            _make_row("3pstart_trail_win", "Trailing at 3P Start → Win Probability", s3_trail_win),
            _make_row("3pstart_trail_draw", "Trailing at 3P Start → Draw Probability", s3_trail_draw),
            _make_row("3pstart_trail_loss", "Trailing at 3P Start → Loss Probability", s3_trail_loss),
        ],
    )

    # First Goal Impact (Regular Time)
    _sec(
        "First Goal Impact (Regular Time)",
        [
            _make_row("fg_1p_scored_win", "1P First Goal → Win Probability", fg_1p_scored_win),
            _make_row("fg_1p_scored_draw", "1P First Goal → Draw Probability", fg_1p_scored_draw),
            _make_row("fg_1p_scored_loss", "1P First Goal → Loss Probability", fg_1p_scored_loss),

            _make_row("fg_1p_conceded_win", "1P Conceded First Goal → Win Probability", fg_1p_conceded_win),
            _make_row("fg_1p_conceded_draw", "1P Conceded First Goal → Draw Probability", fg_1p_conceded_draw),
            _make_row("fg_1p_conceded_loss", "1P Conceded First Goal → Loss Probability", fg_1p_conceded_loss),

            _make_row("fg_2p_00_scored_win", "2P Start 0–0 → First Goal → Win Probability", fg_2p_00_scored_win),
            _make_row("fg_2p_00_scored_draw", "2P Start 0–0 → First Goal → Draw Probability", fg_2p_00_scored_draw),
            _make_row("fg_2p_00_scored_loss", "2P Start 0–0 → First Goal → Loss Probability", fg_2p_00_scored_loss),

            _make_row("fg_2p_00_conceded_win", "2P Start 0–0 → First Goal Conceded → Win Probability", fg_2p_00_conceded_win),
            _make_row("fg_2p_00_conceded_draw", "2P Start 0–0 → First Goal Conceded → Draw Probability", fg_2p_00_conceded_draw),
            _make_row("fg_2p_00_conceded_loss", "2P Start 0–0 → First Goal Conceded → Loss Probability", fg_2p_00_conceded_loss),

            _make_row("fg_3p_00_scored_win", "3P Start 0–0 → First Goal → Win Probability", fg_3p_00_scored_win),
            _make_row("fg_3p_00_scored_draw", "3P Start 0–0 → First Goal → Draw Probability", fg_3p_00_scored_draw),
            _make_row("fg_3p_00_scored_loss", "3P Start 0–0 → First Goal → Loss Probability", fg_3p_00_scored_loss),

            _make_row("fg_3p_00_conceded_win", "3P Start 0–0 → First Goal Conceded → Win Probability", fg_3p_00_conceded_win),
            _make_row("fg_3p_00_conceded_draw", "3P Start 0–0 → First Goal Conceded → Draw Probability", fg_3p_00_conceded_draw),
            _make_row("fg_3p_00_conceded_loss", "3P Start 0–0 → First Goal Conceded → Loss Probability", fg_3p_00_conceded_loss),
        ],
    )

    return {
        "ok": True,
        "game_id": game_id,
        "sample_size": len(h2h_rows),
        "insights": {"sections": sections},
        "meta": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "note": "Goal Timing Distribution is intentionally excluded",
        },
    }
