# routers/notifications_router.py

from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, request, jsonify

from db import execute  # INSERT / UPDATE 용 헬퍼

notifications_bp = Blueprint("notifications", __name__)


@notifications_bp.route("/api/notifications/register_device", methods=["POST"])
def register_device() -> Any:
    """
    앱에서 FCM 토큰을 받아온 뒤,
    디바이스 정보를 서버에 등록/업데이트하는 엔드포인트.

    요청 JSON 예시:
    {
        "device_id": "uuid-1234",
        "fcm_token": "dC1k2B1fRPiz....",
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
    notifications_enabled = bool(notifications_enabled_raw) if notifications_enabled_raw is not None else True

    # 최소 필수값 체크
    if not device_id or not fcm_token:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "device_id and fcm_token are required",
                }
            ),
            400,
        )

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
