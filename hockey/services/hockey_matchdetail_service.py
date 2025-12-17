# hockey/services/hockey_matchdetail_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None
""



def _norm_period(p: Any) -> str:
    if p is None:
        return ""
    s = str(p).strip().upper()
    return s


def _is_goal_event(ev_type: Any) -> bool:
    if ev_type is None:
        return False
    return str(ev_type).strip().lower() == "goal"

def _is_meaningful_goal_event(e: Dict[str, Any]) -> bool:
    if not _is_goal_event(e.get("type")):
        return False

    detail = str(e.get("detail") or "").strip()
    players = e.get("players") or []
    assists = e.get("assists") or []

    if not isinstance(players, list):
        players = []
    if not isinstance(assists, list):
        assists = []

    players_clean = [str(x).strip() for x in players if str(x).strip()]
    assists_clean = [str(x).strip() for x in assists if str(x).strip()]

    # comment/players/assists 중 하나라도 있으면 "진짜 goal"로 취급
    return bool(detail) or len(players_clean) > 0 or len(assists_clean) > 0

def _goal_quality(e: Dict[str, Any]) -> int:
    """
    goal 이벤트 "신뢰도" 점수
    - 디테일/선수/어시스트가 있을수록 점수↑
    - 취소/오류로 남는 goal일수록 점수↓
    """
    detail = str(e.get("detail") or "").strip()
    players = e.get("players") or []
    assists = e.get("assists") or []

    if not isinstance(players, list):
        players = []
    if not isinstance(assists, list):
        assists = []

    players_clean = [str(x).strip() for x in players if str(x).strip()]
    assists_clean = [str(x).strip() for x in assists if str(x).strip()]

    score = 0
    if detail:
        score += 2
    if len(players_clean) > 0:
        score += 2
    if len(assists_clean) > 0:
        score += 1
    return score


def _filter_goals_to_match_final(
    *,
    events: List[Dict[str, Any]],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_final: Optional[int],
    away_final: Optional[int],
) -> List[Dict[str, Any]]:
    """
    취소골/정정골이 DB에 남는 케이스 방어:
    - (이미 빈 goal은 위에서 걸러지고 있지만) goal 수가 공식 점수보다 많으면
      '품질 낮은 goal'부터 제거해서 공식 점수에 맞춘다.
    """
    if home_team_id is None or away_team_id is None:
        return events
    if home_final is None or away_final is None:
        return events

    home_goals: List[Dict[str, Any]] = []
    away_goals: List[Dict[str, Any]] = []

    for e in events:
        if not _is_goal_event(e.get("type")):
            continue
        if not _is_meaningful_goal_event(e):
            continue
        tid = (e.get("team") or {}).get("id")
        if tid == home_team_id:
            home_goals.append(e)
        elif tid == away_team_id:
            away_goals.append(e)

    def keep_top(goals: List[Dict[str, Any]], final: int) -> set:
        if final < 0:
            final = 0
        if len(goals) <= final:
            return {g.get("id") for g in goals}

        def sort_key(g: Dict[str, Any]):
            q = _goal_quality(g)
            period = str(g.get("period") or "")
            minute = g.get("minute")
            try:
                minute_i = int(minute) if minute is not None else 9999
            except Exception:
                minute_i = 9999
            order = g.get("order")
            try:
                order_i = int(order) if order is not None else 9999
            except Exception:
                order_i = 9999
            return (-q, period, minute_i, order_i)

        goals_sorted = sorted(goals, key=sort_key)
        keep = goals_sorted[:final]
        return {g.get("id") for g in keep}

    home_keep_ids = keep_top(home_goals, int(home_final))
    away_keep_ids = keep_top(away_goals, int(away_final))

    # keep에 없는 goal은 제거
    out: List[Dict[str, Any]] = []
    for e in events:
        if _is_goal_event(e.get("type")) and _is_meaningful_goal_event(e):
            tid = (e.get("team") or {}).get("id")
            if tid == home_team_id and e.get("id") not in home_keep_ids:
                continue
            if tid == away_team_id and e.get("id") not in away_keep_ids:
                continue
        out.append(e)

    return out




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
    서버에서 period별 득점 스코어를 계산해서 내려준다 (A방식)
    - 기본은 P1/P2/P3 는 항상 내려줌 (0 포함)
    - OT/SO 는 '해당될 때만' 내려줌
      - 연장만 갔으면 OT만
      - 승부샷까지 갔으면 OT + SO
    - 소스:
      1) events에 period 정보가 있고 goal 이벤트가 있으면 우선 집계
      2) OT/SO goal 이벤트가 없을 때는 final - (P1~P3, OT) 차이로 보정
         - AP(After Penalties)인 경우: OT를 events에서 못찾으면 OT는 0으로 두고,
           남는 1점을 SO로 배정 (하키 최종 스코어는 SO 승자 +1로 표시되는 케이스 고려)
    """

    # 팀 id 없으면 안정적으로 None 반환
    if home_team_id is None or away_team_id is None:
        return {"p1": None, "p2": None, "p3": None, "ot": None, "so": None}

    # 상태 판단
    st = (status or "").strip().upper()

    # 넌 지금 status_long에 "After Penalties"가 내려오는 케이스를 확인했지(AP). (대화 로그 기준)
    # 일반적으로:
    # - AP: 승부샷(SO)까지 감 => OT + SO 표시
    # - AOT: OT에서 끝남 => OT만 표시
    # - FT: 정규 종료 => OT/SO 없음
    # 리그/공급자에 따라 코드값이 다를 수 있어서 폭넓게 잡음.
    AP_STATUSES = {"AP", "PEN", "SO", "AFTER PENALTIES", "AFTERPENALTIES"}
    OT_STATUSES = {"AOT", "OT", "AFTER OVER TIME", "AFTER OVERTIME", "AFTEROVERTIME"}

    show_ot = st in AP_STATUSES or st in OT_STATUSES
    show_so = st in AP_STATUSES  # 승부샷까지 갔을 때만

    # period별 집계(우선 events 기반)
    p1_home = p1_away = 0
    p2_home = p2_away = 0
    p3_home = p3_away = 0
    ot_home = ot_away = 0
    so_home = so_away = 0

    for e in events:
        if not _is_meaningful_goal_event(e):
            continue

        period = _norm_period(e.get("period"))
        team = e.get("team") or {}
        team_id = team.get("id")

        if team_id not in (home_team_id, away_team_id):
            continue

        is_home = team_id == home_team_id

        if period == "P1":
            if is_home:
                p1_home += 1
            else:
                p1_away += 1
        elif period == "P2":
            if is_home:
                p2_home += 1
            else:
                p2_away += 1
        elif period == "P3":
            if is_home:
                p3_home += 1
            else:
                p3_away += 1
        elif period in ("OT", "P4") or period.startswith("OT"):
            if is_home:
                ot_home += 1
            else:
                ot_away += 1
        elif period in ("SO", "PEN") or period.startswith("SO"):
            if is_home:
                so_home += 1
            else:
                so_away += 1

    # 정규합
    reg_home = p1_home + p2_home + p3_home
    reg_away = p1_away + p2_away + p3_away

    # final 점수가 없으면, events 집계까지만 신뢰
    if home_final is None or away_final is None:
        periods: Dict[str, Optional[Dict[str, int]]] = {
            "p1": {"home": p1_home, "away": p1_away},
            "p2": {"home": p2_home, "away": p2_away},
            "p3": {"home": p3_home, "away": p3_away},
            "ot": None,
            "so": None,
        }
        if show_ot:
            periods["ot"] = {"home": ot_home, "away": ot_away}
        if show_so:
            periods["so"] = {"home": so_home, "away": so_away}
        return periods

    # OT 계산 보정
    # - AP(승부샷): OT goal 이벤트가 없으면 OT는 0으로 두는 게 안전
    # - AOT(연장 종료): OT = final - reg (events OT가 없을 때)
    if show_ot:
        if ot_home == 0 and ot_away == 0:
            if st in OT_STATUSES:
                # 연장 종료: final에서 정규를 뺀 값이 OT 득점
                ot_home = max(0, home_final - reg_home)
                ot_away = max(0, away_final - reg_away)
            else:
                # 승부샷(AP)인데 OT 득점 이벤트가 없으면 OT는 0으로 둔다.
                ot_home = 0
                ot_away = 0

    # SO 계산 보정
    if show_so:
        # events 기반 SO가 없으면 final - (reg + ot)로 남는 점수를 SO로 처리
        if so_home == 0 and so_away == 0:
            so_home = max(0, home_final - (reg_home + (ot_home if show_ot else 0)))
            so_away = max(0, away_final - (reg_away + (ot_away if show_ot else 0)))

            # 일반적으로 SO는 승자만 +1 이라서 (1,0) 혹은 (0,1) 형태가 기대됨.
            # 다만 데이터가 깨진 경우 대비해서 음수는 이미 0 처리.

    periods_out: Dict[str, Optional[Dict[str, int]]] = {
        "p1": {"home": p1_home, "away": p1_away},
        "p2": {"home": p2_home, "away": p2_away},
        "p3": {"home": p3_home, "away": p3_away},
        "ot": None,
        "so": None,
    }

    if show_ot:
        periods_out["ot"] = {"home": ot_home, "away": ot_away}

    if show_so:
        periods_out["so"] = {"home": so_home, "away": so_away}

    return periods_out


def hockey_get_game_detail(game_id: int) -> Dict[str, Any]:
    """
    하키 경기 상세 (정식)
    - hockey_games + teams + leagues + countries JOIN
    - score_json을 공식 점수로 사용
    - events는 hockey_game_events 기반
    - periods(P1/P2/P3/OT/SO)는 서버에서 계산해서 내려줌 (A방식)
    """

    # -------------------------
    # 1) GAME HEADER
    # -------------------------
    game_sql = """
        SELECT
            g.id AS game_id,
            g.league_id,
            g.season,
            g.stage,
            g.group_name,
            g.game_date AS date_utc,
            g.status,
            g.status_long,
            g.live_timer,
            g.timezone AS game_timezone,
            g.score_json,


            l.id AS league_id2,
            l.name AS league_name,
            l.logo AS league_logo,
            c.name AS league_country,

            th.id AS home_id,
            th.name AS home_name,
            th.logo AS home_logo,

            ta.id AS away_id,
            ta.name AS away_name,
            ta.logo AS away_logo
        FROM hockey_games g
        JOIN hockey_leagues l ON l.id = g.league_id
        LEFT JOIN hockey_countries c ON c.id = l.country_id
        LEFT JOIN hockey_teams th ON th.id = g.home_team_id
        LEFT JOIN hockey_teams ta ON ta.id = g.away_team_id
        WHERE g.id = %s
        LIMIT 1
    """

    g = hockey_fetch_one(game_sql, (game_id,))
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    # ✅ LIVE일 때 status_long + timer 조합
    status = _safe_text(g.get("status"))
    status_long = _safe_text(g.get("status_long"))
    live_timer = _safe_text(g.get("live_timer"))

    clock_text = ""
    if live_timer:
        if ":" in live_timer:
            clock_text = live_timer
        else:
            try:
                clock_text = f"{int(live_timer):02d}:00"
            except Exception:
                clock_text = live_timer

    status_long_out = status_long
    if status in ("P1", "P2", "P3", "OT", "SO") and clock_text:
        status_long_out = f"{status_long} {clock_text}"


    score_json = g.get("score_json") or {}

    # score_json 구조가 다양한 경우를 대비해서 안전하게 추출
    # 예상: {"home": 2, "away": 3} 또는 {"scores":{"home":2,"away":3}} 등
    home_score = None
    away_score = None

    if isinstance(score_json, dict):
        if "home" in score_json or "away" in score_json:
            home_score = _safe_int(score_json.get("home"))
            away_score = _safe_int(score_json.get("away"))
        elif "scores" in score_json and isinstance(score_json.get("scores"), dict):
            s = score_json.get("scores") or {}
            home_score = _safe_int(s.get("home"))
            away_score = _safe_int(s.get("away"))

    # ✅ date_utc를 ISO8601(Z)로 고정
    dt = g.get("date_utc")
    if dt is not None:
        try:
            dt_iso = (
                dt.astimezone(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            dt_iso = str(dt)
    else:
        dt_iso = None

    game_obj: Dict[str, Any] = {
        "game_id": g["game_id"],
        "league": {
            "id": g["league_id2"],
            "name": g["league_name"],
            "logo": g["league_logo"],
            "country": g["league_country"],
        },
        "season": g["season"],
        "stage": g.get("stage"),
        "group_name": g.get("group_name"),
        "date_utc": dt_iso,
        "status": g.get("status"),
        "status_long": status_long_out,
        "clock": clock_text or None,
        "timer": live_timer or None,
        "timezone": g.get("game_timezone") or "UTC",
        "home": {
            "id": g.get("home_id"),
            "name": g.get("home_name"),
            "logo": g.get("home_logo"),
            "score": home_score,
        },
        "away": {
            "id": g.get("away_id"),
            "name": g.get("away_name"),
            "logo": g.get("away_logo"),
            "score": away_score,
        },
        # periods는 아래에서 계산해서 채운다
        "periods": {
            "p1": None,
            "p2": None,
            "p3": None,
            "ot": None,
            "so": None,
        },
    }

    # -------------------------
    # 2) EVENTS TIMELINE
    # -------------------------
    events_sql = """
        SELECT
            e.id,
            e.game_id,
            e.period,
            e.minute,
            e.team_id,
            e.type,
            e.comment,
            e.players,
            e.assists,
            e.event_order,

            t.name AS team_name,
            t.logo AS team_logo
        FROM hockey_game_events e
        LEFT JOIN hockey_teams t ON t.id = e.team_id
        WHERE e.game_id = %s
        ORDER BY e.period ASC, e.minute ASC NULLS LAST, e.event_order ASC
    """

    ev_rows = hockey_fetch_all(events_sql, (game_id,))
    events: List[Dict[str, Any]] = []

    for r in ev_rows:
        etype = str(r.get("type") or "").strip().lower()

        players = r.get("players") or []
        assists = r.get("assists") or []
        detail = str(r.get("comment") or "").strip()

        # ✅ "빈 goal" (comment/players/assists 모두 없음) 은 타임라인에서 제외
        if etype == "goal":
            if not isinstance(players, list):
                players = []
            if not isinstance(assists, list):
                assists = []

            players_clean = [str(x).strip() for x in players if str(x).strip()]
            assists_clean = [str(x).strip() for x in assists if str(x).strip()]

            if (not detail) and (len(players_clean) == 0) and (len(assists_clean) == 0):
                continue

            players = players_clean
            assists = assists_clean

        events.append(
            {
                "id": r["id"],
                "type": r.get("type"),
                "detail": r.get("comment"),
                "period": r.get("period"),
                "minute": r.get("minute"),
                "order": r.get("event_order"),
                "team": {
                    "id": r.get("team_id"),
                    "name": r.get("team_name"),
                    "logo": r.get("team_logo"),
                },
                # players/assists는 현재 text[]로 들어오므로 정식 구조는 "배열"로 고정
                "players": players,
                "assists": assists,
            }
        )

        # ✅ 취소/정정으로 DB에 goal이 남는 케이스 방어:
    # 공식 점수(score_json)에 맞춰 goal 이벤트 수를 제한
    events = _filter_goals_to_match_final(
        events=events,
        home_team_id=game_obj["home"]["id"],
        away_team_id=game_obj["away"]["id"],
        home_final=home_score,
        away_final=away_score,
    )



    # -------------------------
    # 3) PERIOD SCORES (A 방식)
    # -------------------------
    game_obj["periods"] = _calc_period_scores(
        home_team_id=game_obj["home"]["id"],
        away_team_id=game_obj["away"]["id"],
        home_final=home_score,
        away_final=away_score,
        status=game_obj.get("status"),
        events=events,
    )

    # -------------------------
    # 3) H2H (Head-to-Head)
    # -------------------------
    # ✅ 종료경기만 사용: FT / AOT / AP
    finished_statuses = ("FT", "AOT", "AP")

    home_id = g.get("home_id")
    away_id = g.get("away_id")

    h2h_rows: List[Dict[str, Any]] = []

    # 팀 ID가 없으면 h2h는 빈 배열 유지
    if home_id and away_id:
        h2h_sql = """
            SELECT
                gg.id AS game_id,
                gg.league_id,
                gg.season,
                gg.stage,
                gg.group_name,
                gg.game_date AS date_utc,
                gg.status,
                gg.status_long,
                gg.timezone AS game_timezone,
                gg.score_json,

                l.id AS league_id2,
                l.name AS league_name,
                l.logo AS league_logo,
                c.name AS league_country,

                th.id AS home_id,
                th.name AS home_name,
                th.logo AS home_logo,

                ta.id AS away_id,
                ta.name AS away_name,
                ta.logo AS away_logo
            FROM hockey_games gg
            JOIN hockey_leagues l ON l.id = gg.league_id
            LEFT JOIN hockey_countries c ON c.id = l.country_id
            LEFT JOIN hockey_teams th ON th.id = gg.home_team_id
            LEFT JOIN hockey_teams ta ON ta.id = gg.away_team_id
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
        """

        h2h_game_rows = hockey_fetch_all(
            h2h_sql,
            (
                game_id,
                home_id, away_id,
                away_id, home_id,
                list(finished_statuses),
            ),
        )

        for rr in h2h_game_rows:
            # score_json에서 home/away 점수 안전 추출 (현재 game header와 동일 규칙)
            sj = rr.get("score_json") or {}
            hs = None
            aws = None
            if isinstance(sj, dict):
                if "home" in sj or "away" in sj:
                    hs = _safe_int(sj.get("home"))
                    aws = _safe_int(sj.get("away"))
                elif "scores" in sj and isinstance(sj.get("scores"), dict):
                    ss = sj.get("scores") or {}
                    hs = _safe_int(ss.get("home"))
                    aws = _safe_int(ss.get("away"))

            # date_utc ISO8601(Z) 고정
            dtt = rr.get("date_utc")
            if dtt is not None:
                try:
                    dt_iso2 = (
                        dtt.astimezone(timezone.utc)
                        .replace(microsecond=0)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except Exception:
                    dt_iso2 = str(dtt)
            else:
                dt_iso2 = None

            h2h_rows.append(
                {
                    # ✅ 앱 쪽에서 “경기 선택 → 해당 경기 matchdetail 이동”에 쓰는 키
                    "game_id": rr["game_id"],
                    "league_id": rr["league_id"],
                    "season": rr["season"],
                    "stage": rr.get("stage"),
                    "group_name": rr.get("group_name"),
                    "date_utc": dt_iso2,
                    "status": rr.get("status"),
                    "status_long": rr.get("status_long"),
                    "timezone": rr.get("game_timezone") or "UTC",
                    "league": {
                        "id": rr["league_id2"],
                        "name": rr["league_name"],
                        "logo": rr["league_logo"],
                        "country": rr["league_country"],
                    },
                    "home": {
                        "id": rr.get("home_id"),
                        "name": rr.get("home_name"),
                        "logo": rr.get("home_logo"),
                        "score": hs,
                    },
                    "away": {
                        "id": rr.get("away_id"),
                        "name": rr.get("away_name"),
                        "logo": rr.get("away_logo"),
                        "score": aws,
                    },
                }
            )

    return {
        "ok": True,
        "game": game_obj,
        "events": events,
        "h2h": {
            "rows": h2h_rows
        },
        "meta": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    }

