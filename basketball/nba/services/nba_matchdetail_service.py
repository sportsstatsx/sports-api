# basketball/nba/services/nba_matchdetail_service.py
from __future__ import annotations

import os
import json
from datetime import timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg  # psycopg v3
    from psycopg.rows import dict_row as _dict_row

    def _nba_connect():
        dsn = (os.getenv("NBA_DATABASE_URL") or "").strip()
        if not dsn:
            raise RuntimeError("NBA_DATABASE_URL is not set")
        return psycopg.connect(dsn, row_factory=_dict_row)

except Exception:
    psycopg = None
    _dict_row = None

    import psycopg2
    import psycopg2.extras

    def _nba_connect():
        dsn = (os.getenv("NBA_DATABASE_URL") or "").strip()
        if not dsn:
            raise RuntimeError("NBA_DATABASE_URL is not set")
        return psycopg2.connect(dsn)


def nba_fetch_one(sql: str, params: tuple) -> Optional[Dict[str, Any]]:
    conn = _nba_connect()
    try:
        if psycopg is not None:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    row = cur.fetchone()
                    return dict(row) if row else None
        else:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    row = cur.fetchone()
                    return dict(row) if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def nba_fetch_all(sql: str, params: tuple) -> List[Dict[str, Any]]:
    conn = _nba_connect()
    try:
        if psycopg is not None:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() or []
                    return [dict(r) for r in rows]
        else:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() or []
                    return [dict(r) for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


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


def _to_utc_iso_z(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    try:
        return (
            dt.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except Exception:
        return str(dt)


def _jget(obj: Any, *path: str) -> Any:
    cur = obj
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _as_dict_json(v: Any) -> Optional[Dict[str, Any]]:
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            o = json.loads(v)
            return o if isinstance(o, dict) else None
        except Exception:
            return None
    return None


def _extract_points_from_game_raw(raw: Any) -> Dict[str, Optional[int]]:
    """
    nba_games.raw_json에서 점수 추출(구조가 일정치 않을 수 있으니 보수적으로)
    반환: {"home": int|None, "visitors": int|None}
    """
    out = {"home": None, "visitors": None}
    if not isinstance(raw, dict):
        return out

    # 자주 쓰이는 케이스: scores.home.points / scores.visitors.points
    for key_home in (("scores", "home", "points"), ("scores", "home", "total")):
        v = _jget(raw, *key_home)
        if v is not None:
            out["home"] = _safe_int(v)
            break

    for key_vis in (("scores", "visitors", "points"), ("scores", "visitors", "total")):
        v = _jget(raw, *key_vis)
        if v is not None:
            out["visitors"] = _safe_int(v)
            break

    # scores.home 자체가 숫자인 케이스 방어
    if out["home"] is None:
        v = _jget(raw, "scores", "home")
        if isinstance(v, (int, float, str)):
            out["home"] = _safe_int(v)

    if out["visitors"] is None:
        v = _jget(raw, "scores", "visitors")
        if isinstance(v, (int, float, str)):
            out["visitors"] = _safe_int(v)

    return out


def _extract_linescore_blob(raw: Any) -> Dict[str, Any]:
    """
    raw_json에 scores/linescore 등이 있으면 '있는 그대로' 내려줌(추측 가공 금지)
    """
    if not isinstance(raw, dict):
        return {}

    scores = raw.get("scores")
    if isinstance(scores, dict):
        return {"scores": scores}

    ls = raw.get("linescore")
    if ls is not None:
        return {"linescore": ls}

    return {}

# ✅ fixtures에서 검증된 NBA LIVE timer 생성 로직(이 파일에 그대로 이식)
def _count_filled_linescore(team_scores: Any) -> int:
    """
    scores.home.linescore / scores.visitors.linescore에서
    ""(빈값) 제외하고 채워진 쿼터 개수 카운트
    """
    if not isinstance(team_scores, dict):
        return 0
    ls = team_scores.get("linescore") or []
    if not isinstance(ls, list):
        return 0
    return sum(1 for x in ls if str(x).strip() != "")


def _build_nba_timer_text_from_game_raw(
    *,
    status_short: Optional[int],
    periods_obj: Any,
    clock_text: Optional[str],
) -> Optional[str]:
    """
    API-Sports 원본 periods(current/endOfPeriod) 기준으로만 timer 생성.
    - clock이 null이어도 In Play면 Break로 만들지 않는다.
    """
    if status_short != 2:
        return None

    p = periods_obj if isinstance(periods_obj, dict) else {}

    cur: Optional[int] = None
    eop: Optional[bool] = None

    try:
        v = p.get("current")
        cur = int(v) if v is not None else None
    except Exception:
        cur = None

    try:
        v = p.get("endOfPeriod")
        eop = bool(v) if v is not None else None
    except Exception:
        eop = None

    if eop is True and cur in (1, 2, 3, 4):
        return f"{cur}Q End Break"

    if cur in (1, 2, 3, 4):
        if clock_text and str(clock_text).strip():
            return f"Q{cur} {str(clock_text).strip()}"
        return f"Q{cur}"

    return None

def _safe_upper(v: Any) -> str:
    return _safe_text(v).upper()


def _jget_first(obj: Any, paths: List[Tuple[str, ...]]) -> Any:
    """
    여러 후보 경로 중 첫 번째로 값이 잡히는 걸 반환
    """
    for p in paths:
        v = _jget(obj, *p)
        if v is not None:
            return v
    return None


def _extract_nba_live_state(raw: Any) -> Dict[str, Any]:
    """
    matchdetail에서 내려줄 LIVE 상태(시간/쿼터/Break)를 raw_json에서 최대한 보수적으로 추출.
    - 절대 '없던 정보'를 만들어내지 않는다.
    - 있으면 정규화(time_text), 없으면 빈 값.
    반환 예:
      {
        "is_live": bool,
        "status_long": str,
        "status_short": int|None,
        "period": int|None,            # 1..4, 5=OT (추정은 최소)
        "clock": "4:30"|"04:30"|"" ,
        "time_text": "Q2 4:30" | "2Q End Break" | "" ,
        "raw": { ...원본에서 참고한 조각... }
      }
    """
    out: Dict[str, Any] = {
        "is_live": False,
        "status_long": "",
        "status_short": None,
        "period": None,
        "clock": "",
        "time_text": "",
        "raw": {},
    }

    if not isinstance(raw, dict):
        return out

    # ─────────────────────────────────────────
    # status
    # (API 공급원마다 status 구조가 달라서 여러 케이스를 허용)
    # ─────────────────────────────────────────
    status_obj = raw.get("status") if isinstance(raw.get("status"), dict) else {}
    status_long = _safe_text(status_obj.get("long") if isinstance(status_obj, dict) else raw.get("status_long") or raw.get("status"))
    status_short = _safe_int(status_obj.get("short") if isinstance(status_obj, dict) else raw.get("status_short"))

    out["status_long"] = status_long
    out["status_short"] = status_short

    up_long = status_long.upper()

    # LIVE 판정은 "Finished/Not Started" 같은 명확한 케이스 제외 후 보수적으로
    # (너의 matches/fixtures 쪽에서 LIVE 판단 이미 하고 있으니, 여기서는 보조값)
    if up_long and ("FINISHED" not in up_long) and ("NOT START" not in up_long) and ("SCHEDULE" not in up_long):
        # In Play / Live / Playing / Break 등은 live로 취급
        if ("IN PLAY" in up_long) or ("LIVE" in up_long) or ("PLAY" in up_long) or ("BREAK" in up_long) or ("HALF" in up_long):
            out["is_live"] = True

    # ─────────────────────────────────────────
    # period(쿼터) / clock 추출 후보들
    # (가능한 '있는 키'만)
    # ─────────────────────────────────────────
    period_val = _jget_first(raw, [
        ("period",),
        ("periods", "current"),
        ("game", "period"),
        ("game", "quarter"),
        ("status", "period"),
        ("status", "quarter"),
    ])
    period = _safe_int(period_val)

    # OT는 종종 "OT" 문자열로 들어오므로 방어
    if period is None:
        ptxt = _safe_upper(period_val)
        if ptxt == "OT" or "OVERTIME" in ptxt:
            period = 5

    out["period"] = period

    clock_val = _jget_first(raw, [
        ("clock",),
        ("timer",),
        ("time",),
        ("status", "timer"),
        ("status", "clock"),
        ("game", "clock"),
        ("game", "timer"),
    ])
    clock = _safe_text(clock_val)

    # "04:30" 형태만 남기고 싶으면 여기서 더 줄일 수도 있으나,
    # 앱에서 4' 변환을 하고 있으니 서버에서는 raw 기반으로만.
    out["clock"] = clock

    # ─────────────────────────────────────────
    # time_text 구성 (fixtures에서 하던 'Break 표기' 우선)
    # - Break면 "2Q End Break" 같은 텍스트를 만들어주되,
    #   period를 모르면 그냥 "Break"만
    # - clock이 있으면 "Q2 04:30" 형태
    # - 둘 다 없으면 빈 문자열
    # ─────────────────────────────────────────
    is_break = ("BREAK" in up_long) or ("HALFTIME" in up_long) or ("HALF TIME" in up_long)

    time_text = ""

    if is_break:
        if period is not None and 1 <= period <= 4:
            time_text = f"{period}Q End Break"
        elif period == 5:
            time_text = "OT End Break"
        else:
            time_text = "Break"
    else:
        # clock이 있을 때만 쿼터 라벨을 붙여준다(없는 정보 생성 금지)
        if clock:
            if period is not None and 1 <= period <= 4:
                time_text = f"Q{period} {clock}"
            elif period == 5:
                time_text = f"OT {clock}"
            else:
                # period를 모르면 clock만 내려준다(앱에서 그냥 표시 가능)
                time_text = clock

    out["time_text"] = time_text

    # 디버깅/검증용으로 어떤 원본을 참조했는지 최소 raw 조각만 포함
    out["raw"] = {
        "status": status_obj if isinstance(status_obj, dict) else None,
        "period_val": period_val,
        "clock_val": clock_val,
    }

    return out


def _build_game_header_row(g: Dict[str, Any]) -> Dict[str, Any]:
    game_raw = _as_dict_json(g.get("game_raw_json"))
    pts = _extract_points_from_game_raw(game_raw)
    linescore_blob = _extract_linescore_blob(game_raw)

    league_raw = _as_dict_json(g.get("league_raw_json"))

    header: Dict[str, Any] = {
        "game_id": int(g["game_id"]),
        "league": {
            "id": _safe_text(g.get("league_id") or g.get("league")),
            "name": _safe_text(_jget(league_raw, "name")),
            "logo": _jget(league_raw, "logo"),
            "raw": league_raw,
        },
        "season": _safe_int(g.get("season")),
        "stage": _safe_int(g.get("stage")),
        "date_utc": _to_utc_iso_z(g.get("date_start_utc")),
        "status_long": _safe_text(g.get("status_long")),
        "status_short": _safe_int(g.get("status_short")),
        "arena": {
            "name": _safe_text(g.get("arena_name")),
            "city": _safe_text(g.get("arena_city")),
            "state": _safe_text(g.get("arena_state")),
        },
        "home": {
            "id": _safe_int(g.get("home_id")),
            "name": _safe_text(g.get("home_name")),
            "nickname": _safe_text(g.get("home_nickname")),
            "code": _safe_text(g.get("home_code")),
            "city": _safe_text(g.get("home_city")),
            "logo": g.get("home_logo"),
            "score": pts.get("home"),
        },
        "visitors": {
            "id": _safe_int(g.get("visitor_id")),
            "name": _safe_text(g.get("visitor_name")),
            "nickname": _safe_text(g.get("visitor_nickname")),
            "code": _safe_text(g.get("visitor_code")),
            "city": _safe_text(g.get("visitor_city")),
            "logo": g.get("visitor_logo"),
            "score": pts.get("visitors"),
        },
        "raw": game_raw,
    }

    return {"header": header, "linescore": linescore_blob}


def _fetch_game_base(game_id: int) -> Dict[str, Any]:
    sql = """
        SELECT
            g.id AS game_id,
            g.league,
            g.season,
            g.stage,
            g.status_long,
            g.status_short,
            g.date_start_utc,
            g.home_team_id,
            g.visitor_team_id,
            g.arena_name,
            g.arena_city,
            g.arena_state,
            g.raw_json AS game_raw_json,

            lh.id AS league_id,
            lh.raw_json AS league_raw_json,

            th.id AS home_id,
            th.name AS home_name,
            th.nickname AS home_nickname,
            th.code AS home_code,
            th.city AS home_city,
            th.logo AS home_logo,

            tv.id AS visitor_id,
            tv.name AS visitor_name,
            tv.nickname AS visitor_nickname,
            tv.code AS visitor_code,
            tv.city AS visitor_city,
            tv.logo AS visitor_logo
        FROM nba_games g
        LEFT JOIN nba_leagues lh ON lh.id = g.league
        LEFT JOIN nba_teams th ON th.id = g.home_team_id
        LEFT JOIN nba_teams tv ON tv.id = g.visitor_team_id
        WHERE g.id = %s
        LIMIT 1
    """
    row = nba_fetch_one(sql, (game_id,))
    if not row:
        raise ValueError("GAME_NOT_FOUND")
    return row


def _fetch_team_stats(game_id: int, home_tid: Optional[int], vis_tid: Optional[int]) -> Dict[str, Any]:
    rows = nba_fetch_all(
        """
        SELECT game_id, team_id, raw_json
        FROM nba_game_team_stats
        WHERE game_id = %s
        """,
        (game_id,),
    )

    by_team_id: Dict[int, Any] = {}
    for r in rows:
        tid = _safe_int(r.get("team_id"))
        if tid is None:
            continue
        tj = r.get("raw_json")
        if isinstance(tj, str):
            try:
                tj = json.loads(tj)
            except Exception:
                pass
        by_team_id[int(tid)] = tj

    return {
        "home": by_team_id.get(int(home_tid)) if home_tid is not None else None,
        "visitors": by_team_id.get(int(vis_tid)) if vis_tid is not None else None,
        "by_team_id": by_team_id,
    }


def _fetch_player_stats(game_id: int, home_tid: Optional[int], vis_tid: Optional[int]) -> Dict[str, Any]:
    rows = nba_fetch_all(
        """
        SELECT
            s.game_id,
            s.player_id,
            s.team_id,
            s.raw_json AS stat_raw_json,
            p.firstname,
            p.lastname
        FROM nba_game_player_stats s
        LEFT JOIN nba_players p ON p.id = s.player_id
        WHERE s.game_id = %s
        """,
        (game_id,),
    )

    home_list: List[Dict[str, Any]] = []
    vis_list: List[Dict[str, Any]] = []
    all_list: List[Dict[str, Any]] = []

    for r in rows:
        pid = _safe_int(r.get("player_id"))
        tid = _safe_int(r.get("team_id"))
        stat_raw = r.get("stat_raw_json")

        if isinstance(stat_raw, str):
            try:
                stat_raw = json.loads(stat_raw)
            except Exception:
                pass

        first = _safe_text(r.get("firstname"))
        last = _safe_text(r.get("lastname"))
        full = (f"{first} {last}").strip()

        item = {
            "player_id": pid,
            "team_id": tid,
            "name": {"first": first, "last": last, "full": full},
            "raw": stat_raw,
        }
        all_list.append(item)

        if home_tid is not None and tid == home_tid:
            home_list.append(item)
        elif vis_tid is not None and tid == vis_tid:
            vis_list.append(item)

    return {"home": home_list, "visitors": vis_list, "all": all_list}

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _parse_pct(v: Any) -> Optional[float]:
    """
    DB raw_json에서 퍼센트가 "49" / "33.3" / 49 / 33.3 등으로 올 수 있어서 통일
    - 0~100 스케일의 float로 반환
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).strip().replace("%", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _parse_min_to_int(v: Any) -> Optional[int]:
    """
    선수 min: "19" / "19:00" / 19 등 케이스 방어 -> 분(int)
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    try:
        s = str(v).strip()
        if s == "":
            return None
        if ":" in s:
            # "19:00" -> 19
            s = s.split(":", 1)[0].strip()
        return int(float(s))
    except Exception:
        return None


def _extract_team_stat_obj(team_raw: Any) -> Optional[Dict[str, Any]]:
    """
    nba_game_team_stats.raw_json:
      {"team": {...}, "statistics": [ { ... } ] }
    statistics[0]만 사용 (추측 가공 금지, 존재하는 키만 core로 정규화)
    """
    if not isinstance(team_raw, dict):
        return None
    st = team_raw.get("statistics")
    if isinstance(st, list) and st:
        obj = st[0]
        return obj if isinstance(obj, dict) else None
    # 혹시 statistics가 dict로 오는 케이스 방어
    if isinstance(st, dict):
        return st
    return None


def _team_stats_core(team_raw: Any) -> Optional[Dict[str, Any]]:
    """
    '정확하게 보여줄 수 있는 것만' core로 뽑는다.
    (샘플 16319 기준으로 확실히 존재하는 키만 사용)
    """
    st = _extract_team_stat_obj(team_raw)
    if not isinstance(st, dict):
        return None

    fgm = _safe_int(st.get("fgm"))
    fga = _safe_int(st.get("fga"))
    fgp = _parse_pct(st.get("fgp"))

    tpm = _safe_int(st.get("tpm"))
    tpa = _safe_int(st.get("tpa"))
    tpp = _parse_pct(st.get("tpp"))

    ftm = _safe_int(st.get("ftm"))
    fta = _safe_int(st.get("fta"))
    ftp = _parse_pct(st.get("ftp"))

    off_reb = _safe_int(st.get("offReb"))
    def_reb = _safe_int(st.get("defReb"))
    tot_reb = _safe_int(st.get("totReb"))

    ast = _safe_int(st.get("assists"))
    to = _safe_int(st.get("turnovers"))
    stl = _safe_int(st.get("steals"))
    blk = _safe_int(st.get("blocks"))
    pf = _safe_int(st.get("pFouls"))

    ast_to: Optional[float] = None
    if ast is not None and to is not None:
        ast_to = float(ast) / float(max(1, to))

    return {
        "shooting": {
            "fg": {"m": fgm, "a": fga, "pct": fgp},
            "tp": {"m": tpm, "a": tpa, "pct": tpp},  # 3PT
            "ft": {"m": ftm, "a": fta, "pct": ftp},
        },
        "flow": {
            "reb": {"off": off_reb, "def": def_reb, "total": tot_reb},
            "ast": ast,
            "to": to,
            "ast_to": ast_to,  # assists / max(1, turnovers)
        },
        "defense": {
            "stl": stl,
            "blk": blk,
        },
        "discipline": {
            "pf": pf,
        },
    }


def _player_raw(item: Dict[str, Any]) -> Dict[str, Any]:
    r = item.get("raw")
    return r if isinstance(r, dict) else {}


def _is_played_player(item: Dict[str, Any]) -> bool:
    r = _player_raw(item)
    # DNP류는 comment에 들어오는 케이스가 있으니 방어
    c = r.get("comment")
    if isinstance(c, str) and c.strip():
        # comment가 있으면 대부분 미출전/특이케이스
        # (정확하게 보여줄 수 있는 것만: 미출전은 리더에서 제외)
        return False
    m = _parse_min_to_int(r.get("min"))
    return (m is not None) and (m > 0)


def _leader_pick(players: List[Dict[str, Any]], key: str) -> Optional[Dict[str, Any]]:
    """
    리더 선정: key(points/totReb/assists)
    타이브레이크: min desc -> fga desc -> player_id asc
    """
    cand: List[Tuple[int, int, int, Dict[str, Any]]] = []
    for it in players:
        if not _is_played_player(it):
            continue
        r = _player_raw(it)
        v = _safe_int(r.get(key))
        if v is None:
            continue
        m = _parse_min_to_int(r.get("min")) or 0
        fga = _safe_int(r.get("fga")) or 0
        pid = _safe_int(it.get("player_id")) or 0
        cand.append((v, m, fga, {"item": it, "value": v, "min": m, "fga": fga, "pid": pid}))

    if not cand:
        return None

    # sort: value desc, min desc, fga desc, pid asc
    cand.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]["pid"]))
    best = cand[0][3]["item"]
    r = _player_raw(best)

    name = (best.get("name") or {})
    full = _safe_text(name.get("full")) or _safe_text(_jget(r, "player", "firstname")) + " " + _safe_text(_jget(r, "player", "lastname"))
    full = full.strip()

    return {
        "player_id": _safe_int(best.get("player_id")),
        "name": full,
        "pos": _safe_text(r.get("pos")),
        "min": _parse_min_to_int(r.get("min")),
        "value": _safe_int(r.get(key)),
    }


def _leaders_core(player_stats_side: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "pts": _leader_pick(player_stats_side, "points"),
        "reb": _leader_pick(player_stats_side, "totReb"),
        "ast": _leader_pick(player_stats_side, "assists"),
    }



def _fetch_h2h_games(
    team_a: int,
    team_b: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    H2H: A/B/C 기준
    - status_long='Finished'만
    - league 상관없이 전부
    - 홈/원정 뒤바뀜 포함
    """
    limit = max(1, min(int(limit), 50))

    sql = f"""
        SELECT
            g.id AS game_id,
            g.league,
            g.season,
            g.stage,
            g.status_long,
            g.status_short,
            g.date_start_utc,
            g.home_team_id,
            g.visitor_team_id,
            g.arena_name,
            g.arena_city,
            g.arena_state,
            g.raw_json AS game_raw_json,

            lh.id AS league_id,
            lh.raw_json AS league_raw_json,

            th.id AS home_id,
            th.name AS home_name,
            th.nickname AS home_nickname,
            th.code AS home_code,
            th.city AS home_city,
            th.logo AS home_logo,

            tv.id AS visitor_id,
            tv.name AS visitor_name,
            tv.nickname AS visitor_nickname,
            tv.code AS visitor_code,
            tv.city AS visitor_city,
            tv.logo AS visitor_logo
        FROM nba_games g
        LEFT JOIN nba_leagues lh ON lh.id = g.league
        LEFT JOIN nba_teams th ON th.id = g.home_team_id
        LEFT JOIN nba_teams tv ON tv.id = g.visitor_team_id
        WHERE g.status_long = 'Finished'
          AND (
                (g.home_team_id = %s AND g.visitor_team_id = %s)
             OR (g.home_team_id = %s AND g.visitor_team_id = %s)
          )
        ORDER BY g.date_start_utc DESC
        LIMIT {limit}
    """

    rows = nba_fetch_all(sql, (team_a, team_b, team_b, team_a))
    out: List[Dict[str, Any]] = []
    for r in rows:
        built = _build_game_header_row(r)
        # H2H는 리스트용이니까 header + linescore만 담는다
        out.append(
            {
                "header": built["header"],
                "linescore": built["linescore"],
            }
        )
    return out


def nba_get_game_detail(game_id: int, h2h_limit: int = 5) -> Dict[str, Any]:
    """
    NBA game detail + H2H
    - override/admin 로직 없음
    - H2H 기본 5개, 더보기는 h2h_limit으로 재호출
    """
    g = _fetch_game_base(game_id)

    built = _build_game_header_row(g)
    header = built["header"]
    linescore_blob = built["linescore"]

    # ✅ fixtures와 동일한 방식으로 LIVE timer 생성 (추정/멀티경로 금지)
    game_raw = _as_dict_json(g.get("game_raw_json")) or {}
    status_obj = game_raw.get("status") if isinstance(game_raw.get("status"), dict) else {}
    periods_obj = game_raw.get("periods") if isinstance(game_raw.get("periods"), dict) else {}

    clock_text = None
    if isinstance(status_obj, dict):
        c = status_obj.get("clock")
        if isinstance(c, str) and c.strip():
            clock_text = c.strip()

    status_short = _safe_int(g.get("status_short"))

    timer_text = _build_nba_timer_text_from_game_raw(
        status_short=status_short,
        periods_obj=periods_obj,
        clock_text=clock_text,
    )

    home_tid = _safe_int(g.get("home_team_id"))
    vis_tid = _safe_int(g.get("visitor_team_id"))

    team_stats = _fetch_team_stats(game_id, home_tid, vis_tid)
    player_stats = _fetch_player_stats(game_id, home_tid, vis_tid)

    # H2H
    h2h_rows: List[Dict[str, Any]] = []
    if home_tid is not None and vis_tid is not None:
        h2h_rows = _fetch_h2h_games(home_tid, vis_tid, h2h_limit)

    stats_core = {
        "team": {
            "home": _team_stats_core(team_stats.get("home")),
            "visitors": _team_stats_core(team_stats.get("visitors")),
        },
        "leaders": {
            "home": _leaders_core(player_stats.get("home") or []),
            "visitors": _leaders_core(player_stats.get("visitors") or []),
        },
    }

    data: Dict[str, Any] = {
        "header": header,
        "linescore": linescore_blob,

        # ✅ fixtures와 동일한 LIVE 표기(단일 경로)
        # - 앱은 data.live.timer 를 MatchItem.timeText 로 매핑해서 쓰면 됨
        "live": {
            "clock": clock_text,
            "timer": timer_text,
            "status_short": status_short,
            # 디버깅용(원하면 나중에 제거 가능)
            "halftime": bool(status_obj.get("halftime") is True) if isinstance(status_obj, dict) else False,
        },

        "team_stats": team_stats,
        "player_stats": player_stats,
        "stats_core": stats_core,  # ✅ 앱 StatsTab은 이거만 써도 됨
        "h2h": {
            "limit": int(max(1, min(int(h2h_limit), 50))),
            "rows": h2h_rows,
        },
    }

    return {"ok": True, "data": data}

