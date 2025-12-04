# notifications/routes.py

from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, request, jsonify

from db import execute

notifications_bp = Blueprint("notifications", __name__)


@notifications_bp.route("/api/notifications/register_device", methods=["POST"])
def register_device() -> Any:
    """
    앱에서 FCM 토큰/디바이스 정보를 보내면
    user_devices 테이블에 upsert 하는 엔드포인트.

    요청 JSON 예:
    {
        "device_id": "abc-uuid",
        "fcm_token": "xxx",
        "platform": "android",
        "app_version": "1.6.0",
        "timezone": "Asia/Seoul",
        "language": "ko",
        "notifications_enabled": true
    }
    """

    data: Dict[str, Any] = request.get_json(silent=True) or {}

    device_id = str(data.get("device_id", "")).strip()
    fcm_token = str(data.get("fcm_token", "")).strip()
    platform = str(data.get("platform", "")).strip() or "android"

    app_version = str(data.get("app_version", "")).strip() or None
    timezone_str = str(data.get("timezone", "")).strip() or None
    language = str(data.get("language", "")).strip() or None

    notifications_enabled_raw = data.get("notifications_enabled")
    notifications_enabled = (
        bool(notifications_enabled_raw)
        if notifications_enabled_raw is not None
        else True
    )

    # 필수값 체크
    if not device_id or not fcm_token:
        return jsonify(
            {"ok": False, "error": "device_id and fcm_token are required"}
        ), 400

    # ⚠️ 여기서는 항상 user_devices 테이블만 사용
    sql = """
        INSERT INTO user_devices (
            device_id,
            fcm_token,
            platform,
            app_version,
            timezone,
            language,
            notifications_enabled,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (device_id)
        DO UPDATE SET
            fcm_token = EXCLUDED.fcm_token,
            platform = EXCLUDED.platform,
            app_version = EXCLUDED.app_version,
            timezone = EXCLUDED.timezone,
            language = EXCLUDED.language,
            notifications_enabled = EXCLUDED.notifications_enabled,
            updated_at = NOW();
    """

    execute(
        sql,
        (
            device_id,
            fcm_token,
            platform,
            app_version,
            timezone_str,
            language,
            notifications_enabled,
        ),
    )

    return jsonify({"ok": True})
