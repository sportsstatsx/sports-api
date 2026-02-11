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

    home_tid = _safe_int(g.get("home_team_id"))
    vis_tid = _safe_int(g.get("visitor_team_id"))

    team_stats = _fetch_team_stats(game_id, home_tid, vis_tid)
    player_stats = _fetch_player_stats(game_id, home_tid, vis_tid)

    # H2H
    h2h_rows: List[Dict[str, Any]] = []
    if home_tid is not None and vis_tid is not None:
        h2h_rows = _fetch_h2h_games(home_tid, vis_tid, h2h_limit)

    data: Dict[str, Any] = {
        "header": header,
        "linescore": linescore_blob,
        "team_stats": team_stats,
        "player_stats": player_stats,
        "h2h": {
            "limit": int(max(1, min(int(h2h_limit), 50))),
            "rows": h2h_rows,
        },
    }

    return {"ok": True, "data": data}
