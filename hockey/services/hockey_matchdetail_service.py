# hockey/services/hockey_matchdetail_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def hockey_get_game_detail(game_id: int) -> Dict[str, Any]:
    """
    하키 경기 상세 (정식)
    - hockey_games + teams + leagues + countries JOIN
    - score_json을 공식 점수로 사용
    - events는 hockey_game_events 기반
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

        # date_utc를 ISO8601(Z)로 고정
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
        "status_long": g.get("status_long"),
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
        # 앞으로 period별 점수/OT/SO를 넣을 수 있도록 자리만 고정
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
        players = r.get("players") or []
        assists = r.get("assists") or []

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

    return {
        "ok": True,
        "game": game_obj,
        "events": events,
        "meta": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    }
