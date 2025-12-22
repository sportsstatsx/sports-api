# hockey/routers/hockey_notifications_router.py

from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_one, hockey_fetch_all
import psycopg
from psycopg_pool import ConnectionPool
import os
import json


# ─────────────────────────────────────────
# 하키 DB 풀 (HOCKEY_DATABASE_URL만 사용)
# ─────────────────────────────────────────
HOCKEY_DATABASE_URL = (
    os.environ.get("HOCKEY_DATABASE_URL")
    or os.environ.get("HOCKEY_DATABASE_URL".upper())
    or os.environ.get("hockey_database_url")
)
if not HOCKEY_DATABASE_URL:
    raise RuntimeError("HOCKEY_DATABASE_URL is not set")

_pool = ConnectionPool(conninfo=HOCKEY_DATABASE_URL, open=True)


def hockey_execute(sql: str, params: tuple[Any, ...] = ()) -> int:
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


hockey_notifications_bp = Blueprint("hockey_notifications", __name__, url_prefix="/api/hockey/notifications")


@hockey_notifications_bp.post("/register_device")
def hockey_register_device():
    """
    body:
      {
        "device_id": "android-xxxxx",
        "fcm_token": "...",
        "platform": "android",
        "timezone": "Asia/Seoul",
        "locale": "ko-KR"
      }
    """
    body: Dict[str, Any] = request.get_json(silent=True) or {}

    device_id = str(body.get("device_id", "")).strip()
    fcm_token = str(body.get("fcm_token", "")).strip()
    platform = str(body.get("platform", "")).strip() or None
    timezone = str(body.get("timezone", "")).strip() or None
    language = str(body.get("language", "")).strip() or None


    if not device_id:
        return jsonify({"ok": False, "error": "device_id is required"}), 400
    if not fcm_token:
        return jsonify({"ok": False, "error": "fcm_token is required"}), 400

    # upsert
    hockey_execute(
        """
        INSERT INTO hockey_user_devices (device_id, fcm_token, platform, timezone, language)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (device_id)
        DO UPDATE SET
          fcm_token = EXCLUDED.fcm_token,
          platform = EXCLUDED.platform,
          timezone = EXCLUDED.timezone,
          language = EXCLUDED.language,
          updated_at = now()
        """,
        (device_id, fcm_token, platform, timezone, language),
    )

    return jsonify({"ok": True, "device_id": device_id})


@hockey_notifications_bp.post("/subscribe_game")
def hockey_subscribe_game():
    """
    body:
      {
        "device_id": "...",
        "game_id": 123,
        "notify_score": true,
        "notify_game_start": true,
        "notify_game_end": true
      }
    """
    body: Dict[str, Any] = request.get_json(silent=True) or {}

    device_id = str(body.get("device_id", "")).strip()
    game_id = body.get("game_id", None)

    if not device_id:
        return jsonify({"ok": False, "error": "device_id is required"}), 400
    if game_id is None:
        return jsonify({"ok": False, "error": "game_id is required"}), 400

    try:
        game_id_int = int(game_id)
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be int"}), 400

    notify_score = bool(body.get("notify_score", True))
    notify_game_start = bool(body.get("notify_game_start", True))
    notify_game_end = bool(body.get("notify_game_end", True))

    # device 존재 확인
    dev = hockey_fetch_one("SELECT device_id FROM hockey_user_devices WHERE device_id=%s", (device_id,))
    if not dev:
        return jsonify({"ok": False, "error": "device not registered. call /register_device first"}), 400

    # game 존재 확인
    g = hockey_fetch_one("SELECT id FROM hockey_games WHERE id=%s", (game_id_int,))
    if not g:
        return jsonify({"ok": False, "error": "game_id not found in hockey_games"}), 404

    # ✅ upsert + "이번 호출이 insert(처음 구독)인지" 여부를 반환
    up = hockey_fetch_one(
        """
        INSERT INTO hockey_game_notification_subscriptions
          (device_id, game_id, notify_score, notify_game_start, notify_game_end)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (device_id, game_id)
        DO UPDATE SET
          notify_score = EXCLUDED.notify_score,
          notify_game_start = EXCLUDED.notify_game_start,
          notify_game_end = EXCLUDED.notify_game_end,
          updated_at = now()
        RETURNING (xmax = 0) AS inserted
        """,
        (device_id, game_id_int, notify_score, notify_game_start, notify_game_end),
    )
    
    inserted = bool((up or {}).get("inserted"))

    # ✅ FSM: "처음 구독(즐겨찾기)"이면 '지금 DB 상태'를 그대로 state로 저장한다.
    #   - 이벤트 테이블(hockey_game_events) 절대 보지 않음
    #   - last_event_id / sent_event_keys는 사용 안 하더라도 스키마 호환상 0/[]로 유지
    if inserted:
        cur = hockey_fetch_one(
            "SELECT status, score_json FROM hockey_games WHERE id=%s",
            (game_id_int,),
        )
        cur_status = (cur or {}).get("status", None)

        score_json = (cur or {}).get("score_json")

        # score_json 구조가 문자열/딕트 등 다양하므로 안전 파싱
        def _to_int_local(x: Any, default: int = 0) -> int:
            try:
                if x is None:
                    return default
                if isinstance(x, bool):
                    return default
                if isinstance(x, (int, float)):
                    return int(x)
                s = str(x).strip()
                if not s:
                    return default
                return int(float(s))
            except Exception:
                return default

        def _parse_score_local(v: Any) -> tuple[int, int]:
            if v is None:
                return (0, 0)

            obj = v
            if isinstance(obj, str):
                try:
                    obj = json.loads(obj)
                except Exception:
                    return (0, 0)

            if not isinstance(obj, dict):
                return (0, 0)

            # 1) {"home":2,"away":1}
            if "home" in obj or "away" in obj:
                return (_to_int_local(obj.get("home"), 0), _to_int_local(obj.get("away"), 0))

            # 2) {"total":{"home":..,"away":..}} 등
            for k in ("total", "totals", "final", "score", "scores"):
                vv = obj.get(k)
                if isinstance(vv, dict) and ("home" in vv or "away" in vv):
                    return (_to_int_local(vv.get("home"), 0), _to_int_local(vv.get("away"), 0))

            # 3) periods 합산
            periods = obj.get("periods")
            if isinstance(periods, dict):
                h = 0
                a = 0
                any_found = False
                for pv in periods.values():
                    if isinstance(pv, dict) and ("home" in pv or "away" in pv):
                        any_found = True
                        h += _to_int_local(pv.get("home"), 0)
                        a += _to_int_local(pv.get("away"), 0)
                if any_found:
                    return (h, a)

            return (0, 0)

        home_score, away_score = _parse_score_local(score_json)

        hockey_execute(
            """
            INSERT INTO hockey_game_notification_states
              (device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, sent_event_keys, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (device_id, game_id) DO UPDATE SET
              last_status = EXCLUDED.last_status,
              last_home_score = EXCLUDED.last_home_score,
              last_away_score = EXCLUDED.last_away_score,
              last_event_id = EXCLUDED.last_event_id,
              updated_at = now()
            """,
            (device_id, game_id_int, cur_status, home_score, away_score, 0, []),
        )




    return jsonify({"ok": True, "device_id": device_id, "game_id": game_id_int})


@hockey_notifications_bp.post("/unsubscribe_game")
def hockey_unsubscribe_game():
    """
    body:
      { "device_id": "...", "game_id": 123 }
    """
    body: Dict[str, Any] = request.get_json(silent=True) or {}

    device_id = str(body.get("device_id", "")).strip()
    game_id = body.get("game_id", None)

    if not device_id:
        return jsonify({"ok": False, "error": "device_id is required"}), 400
    if game_id is None:
        return jsonify({"ok": False, "error": "game_id is required"}), 400

    try:
        game_id_int = int(game_id)
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be int"}), 400

    hockey_execute(
        "DELETE FROM hockey_game_notification_subscriptions WHERE device_id=%s AND game_id=%s",
        (device_id, game_id_int),
    )

    return jsonify({"ok": True, "device_id": device_id, "game_id": game_id_int})


@hockey_notifications_bp.get("/subscriptions")
def hockey_get_subscriptions():
    """
    /api/hockey/notifications/subscriptions?device_id=...
    """
    device_id = str(request.args.get("device_id", "")).strip()
    if not device_id:
        return jsonify({"ok": False, "error": "device_id query param required"}), 400

    rows = hockey_fetch_all(
        """
        SELECT s.game_id,
               s.notify_score,
               s.notify_game_start,
               s.notify_game_end,
               g.league_id,
               g.season,
               g.game_date,
               g.status,
               g.status_long
        FROM hockey_game_notification_subscriptions s
        JOIN hockey_games g ON g.id = s.game_id
        WHERE s.device_id=%s
        ORDER BY g.game_date DESC NULLS LAST
        """,
        (device_id,),
    )

    return jsonify({"ok": True, "device_id": device_id, "subscriptions": rows})
