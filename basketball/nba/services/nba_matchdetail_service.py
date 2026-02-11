# basketball/nba/services/nba_matchdetail_service.py
from __future__ import annotations

import os
import json
from datetime import timezone
from typing import Any, Dict, List, Optional

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
        conn = psycopg2.connect(dsn)
        return conn


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
        # 이미 문자열이면 그대로 (DB에 text로 들어온 경우 방어)
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
    """
    dict 경로 안전 접근: _jget(d, "a","b","c")
    """
    cur = obj
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _extract_points_from_game_raw(raw: Any) -> Dict[str, Optional[int]]:
    """
    nba_games.raw_json 에서 점수 추출(구조가 바뀔 수 있으니 매우 보수적으로)
    - 반환: {"home": int|None, "visitors": int|None}
    """
    out = {"home": None, "visitors": None}

    if not isinstance(raw, dict):
        return out

    # 흔한 케이스들: scores.home.points / scores.visitors.points
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

    # 다른 케이스: scores.home / scores.visitors 가 숫자 바로인 경우
    if out["home"] is None:
        v = _jget(raw, "scores", "home")
        if isinstance(v, (int, float, str)):
            out["home"] = _safe_int(v)

    if out["visitors"] is None:
        v = _jget(raw, "scores", "visitors")
        if isinstance(v, (int, float, str)):
            out["visitors"] = _safe_int(v)

    return out


def _extract_linescore(raw: Any) -> Dict[str, Any]:
    """
    period/linescore가 raw_json에 있으면 그대로 내려줌(구조 추측 금지).
    """
    if not isinstance(raw, dict):
        return {}

    # 일반적으로 scores.*.linescore 같은 형태가 많음. 있으면 원본 유지.
    scores = raw.get("scores")
    if isinstance(scores, dict):
        return {"scores": scores}

    # 혹시 linescore 라는 키가 직접 있으면 그것도 유지
    ls = raw.get("linescore")
    if ls is not None:
        return {"linescore": ls}

    return {}


def nba_get_game_detail(game_id: int) -> Dict[str, Any]:
    """
    NBA game detail:
    - nba_games + nba_teams(홈/원정) + nba_leagues(raw_json)
    - nba_game_team_stats(raw_json) / nba_game_player_stats(raw_json + nba_players)
    - override/admin 로직 없음
    """
    game_sql = """
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

    g = nba_fetch_one(game_sql, (game_id,))
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    game_raw = g.get("game_raw_json")
    if isinstance(game_raw, str):
        # json string으로 들어온 케이스 방어
        try:
            game_raw = json.loads(game_raw)
        except Exception:
            game_raw = None

    points = _extract_points_from_game_raw(game_raw)
    linescore_blob = _extract_linescore(game_raw)

    game_obj: Dict[str, Any] = {
        "game_id": int(g["game_id"]),
        "league": {
            "id": _safe_text(g.get("league_id") or g.get("league")),
            # nba_leagues는 raw_json만 있으니 name/logo는 raw_json에서 있으면 사용, 없으면 빈값
            "name": _safe_text(_jget(g.get("league_raw_json"), "name")),
            "logo": _jget(g.get("league_raw_json"), "logo"),
            "raw": g.get("league_raw_json") if isinstance(g.get("league_raw_json"), dict) else None,
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
            "score": points.get("home"),
        },
        "visitors": {
            "id": _safe_int(g.get("visitor_id")),
            "name": _safe_text(g.get("visitor_name")),
            "nickname": _safe_text(g.get("visitor_nickname")),
            "code": _safe_text(g.get("visitor_code")),
            "city": _safe_text(g.get("visitor_city")),
            "logo": g.get("visitor_logo"),
            "score": points.get("visitors"),
        },
        # 원본도 필요하면 앱에서 사용 가능
        "raw": game_raw if isinstance(game_raw, dict) else None,
    }

    # -------------------------
    # 2) TEAM STATS (raw_json 그대로)
    # -------------------------
    team_stats_rows = nba_fetch_all(
        """
        SELECT game_id, team_id, raw_json
        FROM nba_game_team_stats
        WHERE game_id = %s
        """,
        (game_id,),
    )

    team_stats_map: Dict[int, Any] = {}
    for r in team_stats_rows:
        tid = _safe_int(r.get("team_id"))
        if tid is None:
            continue
        tj = r.get("raw_json")
        if isinstance(tj, str):
            try:
                tj = json.loads(tj)
            except Exception:
                pass
        team_stats_map[int(tid)] = tj

    home_tid = _safe_int(g.get("home_team_id"))
    vis_tid = _safe_int(g.get("visitor_team_id"))

    team_stats_out: Dict[str, Any] = {
        "home": team_stats_map.get(int(home_tid)) if home_tid is not None else None,
        "visitors": team_stats_map.get(int(vis_tid)) if vis_tid is not None else None,
        "by_team_id": team_stats_map,  # 혹시 팀ID 기준으로도 필요하면 그대로 사용
    }

    # -------------------------
    # 3) PLAYER STATS (raw_json + nba_players join)
    # -------------------------
    player_rows = nba_fetch_all(
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

    players_home: List[Dict[str, Any]] = []
    players_vis: List[Dict[str, Any]] = []
    players_all: List[Dict[str, Any]] = []

    for r in player_rows:
        pid = _safe_int(r.get("player_id"))
        tid = _safe_int(r.get("team_id"))
        stat_raw = r.get("stat_raw_json")

        if isinstance(stat_raw, str):
            try:
                stat_raw = json.loads(stat_raw)
            except Exception:
                pass

        item = {
            "player_id": pid,
            "team_id": tid,
            "name": {
                "first": _safe_text(r.get("firstname")),
                "last": _safe_text(r.get("lastname")),
                "full": (f"{_safe_text(r.get('firstname'))} {_safe_text(r.get('lastname'))}").strip(),
            },
            "raw": stat_raw,
        }

        players_all.append(item)

        if home_tid is not None and tid == home_tid:
            players_home.append(item)
        elif vis_tid is not None and tid == vis_tid:
            players_vis.append(item)

    players_out = {
        "home": players_home,
        "visitors": players_vis,
        "all": players_all,
    }

    # -------------------------
    # 4) FINAL RESPONSE
    # -------------------------
    data: Dict[str, Any] = {
        "header": game_obj,
        "linescore": linescore_blob,   # 있으면 내려주고 없으면 {}
        "team_stats": team_stats_out,  # raw_json 그대로
        "player_stats": players_out,   # raw_json 그대로
    }

    return {"ok": True, "data": data}
