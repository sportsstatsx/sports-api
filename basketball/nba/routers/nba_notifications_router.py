from __future__ import annotations

from datetime import datetime
from typing import Optional

from flask import Blueprint, jsonify, request

from basketball.nba.nba_db import nba_execute, nba_fetch_all, nba_fetch_one


nba_notifications_bp = Blueprint("nba_notifications", __name__)


@nba_notifications_bp.post("/api/nba/device")
def nba_register_device():
    """
    Register / upsert device for NBA notifications.
    body: { device_id, fcm_token, platform, app_version, timezone, language, notifications_enabled }
    """
    body = request.get_json(silent=True) or {}
    device_id = (body.get("device_id") or "").strip()
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400

    fcm_token = (body.get("fcm_token") or "").strip() or None
    platform = (body.get("platform") or "").strip() or None
    app_version = (body.get("app_version") or "").strip() or None
    tz = (body.get("timezone") or "").strip() or None
    lang = (body.get("language") or "").strip() or None
    notifications_enabled = body.get("notifications_enabled")
    if notifications_enabled is None:
        notifications_enabled = True
    notifications_enabled = bool(notifications_enabled)

    nba_execute(
        """
        INSERT INTO nba_user_devices
          (device_id, fcm_token, platform, app_version, timezone, language, notifications_enabled, created_at, updated_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (device_id)
        DO UPDATE SET
          fcm_token = EXCLUDED.fcm_token,
          platform = EXCLUDED.platform,
          app_version = EXCLUDED.app_version,
          timezone = EXCLUDED.timezone,
          language = EXCLUDED.language,
          notifications_enabled = EXCLUDED.notifications_enabled,
          updated_at = now()
        """,
        (device_id, fcm_token, platform, app_version, tz, lang, notifications_enabled),
    )

    return jsonify({"ok": True, "device_id": device_id})


@nba_notifications_bp.get("/api/nba/subscriptions")
def nba_list_subscriptions():
    device_id = (request.args.get("device_id") or "").strip()
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400

    rows = nba_fetch_all(
        """
        SELECT device_id, game_id, notify_score, notify_game_start, notify_game_end, notify_periods, created_at, updated_at
        FROM nba_game_notification_subscriptions
        WHERE device_id = %s
        ORDER BY updated_at DESC
        """,
        (device_id,),
    )
    return jsonify({"ok": True, "rows": rows})


@nba_notifications_bp.post("/api/nba/subscribe")
def nba_subscribe_game():
    """
    body: { device_id, game_id, notify_score, notify_game_start, notify_game_end, notify_periods }
    """
    body = request.get_json(silent=True) or {}
    device_id = (body.get("device_id") or "").strip()
    game_id = body.get("game_id")

    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400
    try:
        game_id = int(game_id)
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be int"}), 400

    notify_score = bool(body.get("notify_score", True))
    notify_game_start = bool(body.get("notify_game_start", True))
    notify_game_end = bool(body.get("notify_game_end", True))
    notify_periods = bool(body.get("notify_periods", True))

    nba_execute(
        """
        INSERT INTO nba_game_notification_subscriptions
          (device_id, game_id, created_at, notify_score, notify_game_start, notify_game_end, updated_at, notify_periods)
        VALUES
          (%s, %s, now(), %s, %s, %s, now(), %s)
        ON CONFLICT (device_id, game_id)
        DO UPDATE SET
          notify_score = EXCLUDED.notify_score,
          notify_game_start = EXCLUDED.notify_game_start,
          notify_game_end = EXCLUDED.notify_game_end,
          notify_periods = EXCLUDED.notify_periods,
          updated_at = now()
        """,
        (device_id, game_id, notify_score, notify_game_start, notify_game_end, notify_periods),
    )

    # 상태 row도 미리 만들어두면 worker/조회가 편함(하키 라우터와 동일 패턴)
    nba_execute(
        """
        INSERT INTO nba_game_notification_states
          (device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, created_at, updated_at, sent_event_keys)
        VALUES
          (%s, %s, NULL, NULL, NULL, NULL, now(), now(), '{}'::text[])
        ON CONFLICT (device_id, game_id)
        DO UPDATE SET updated_at = now()
        """,
        (device_id, game_id),
    )

    return jsonify({"ok": True, "device_id": device_id, "game_id": game_id})


@nba_notifications_bp.post("/api/nba/unsubscribe")
def nba_unsubscribe_game():
    """
    body: { device_id, game_id }
    """
    body = request.get_json(silent=True) or {}
    device_id = (body.get("device_id") or "").strip()
    game_id = body.get("game_id")

    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400
    try:
        game_id = int(game_id)
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be int"}), 400

    nba_execute(
        "DELETE FROM nba_game_notification_subscriptions WHERE device_id=%s AND game_id=%s",
        (device_id, game_id),
    )
    nba_execute(
        "DELETE FROM nba_game_notification_states WHERE device_id=%s AND game_id=%s",
        (device_id, game_id),
    )

    return jsonify({"ok": True, "device_id": device_id, "game_id": game_id})


@nba_notifications_bp.get("/api/nba/subscribers")
def nba_list_subscribers_for_game():
    """
    GET /api/nba/subscribers?game_id=123
    """
    game_id = request.args.get("game_id")
    try:
        game_id = int(game_id)
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be int"}), 400

    rows = nba_fetch_all(
        """
        SELECT s.device_id,
               d.fcm_token,
               d.platform,
               d.timezone,
               d.language,
               d.notifications_enabled,
               s.notify_score,
               s.notify_game_start,
               s.notify_game_end,
               s.notify_periods,
               s.updated_at
        FROM nba_game_notification_subscriptions s
        JOIN nba_user_devices d ON d.device_id = s.device_id
        WHERE s.game_id = %s
          AND COALESCE(d.notifications_enabled, TRUE) = TRUE
          AND d.fcm_token IS NOT NULL
          AND d.fcm_token <> ''
        ORDER BY s.updated_at DESC
        """,
        (game_id,),
    )
    return jsonify({"ok": True, "rows": rows})
