# hockey/routers/hockey_notifications_router.py

from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_one, hockey_fetch_all
import psycopg
from psycopg_pool import ConnectionPool
import os


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


        # ✅ 옵션 A: 구독(즐겨찾기) 시점 기준으로 "커서(last_event_id)"를 현재 끝으로 맞춰서
    #    구독 이전 이벤트(이미 발생한 골/상태)를 밀린 알림처럼 보내지 않도록 한다.

    # 현재까지 이벤트의 마지막 id (없으면 0)
    last_ev = hockey_fetch_one(
        "SELECT COALESCE(MAX(id), 0) AS max_id FROM hockey_game_events WHERE game_id=%s",
        (game_id_int,),
    )
    max_event_id = int((last_ev or {}).get("max_id") or 0)

    # 현재 게임 상태/스코어도 같이 스냅샷 (없어도 되지만 상태알림(game_start)까지 깔끔해짐)
    cur = hockey_fetch_one(
        "SELECT status, score_json FROM hockey_games WHERE id=%s",
        (game_id_int,),
    )
    cur_status = (cur or {}).get("status", None)
    score_json = (cur or {}).get("score_json", None)

    # score_json은 포맷이 다양하니까, 여기서는 단순하게 0/0으로 두거나
    # (원하면 워커의 parse_score 로직을 라우터로 옮겨서 동일 계산 가능)
    # 일단 상태알림/이벤트 커서가 핵심이라 스코어는 0/0으로 둔다.
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
        (device_id, game_id_int, cur_status, 0, 0, max_event_id, []),
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
