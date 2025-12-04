# notifications/routes.py

from datetime import datetime, timezone
from typing import Any, Dict

from flask import Blueprint, request, jsonify

from db import fetch_one, execute

notifications_bp = Blueprint("notifications", __name__)


@notifications_bp.route("/api/notifications/register_device", methods=["POST"])
def register_device() -> Any:
    """
    앱에서 FCM 토큰을 보내오면
    notification_devices 테이블에 upsert(있으면 UPDATE, 없으면 INSERT) 하는 엔드포인트.
    """

    payload: Dict[str, Any] = request.get_json(silent=True) or {}

    device_id = payload.get("device_id")
    fcm_token = payload.get("fcm_token")
    platform = payload.get("platform") or "android"
    app_version = payload.get("app_version")
    tz = payload.get("timezone")

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

    now_utc = datetime.now(timezone.utc)

    # 이미 등록된 device 인지 확인
    existing = fetch_one(
        """
        SELECT id, fcm_token, is_active
        FROM notification_devices
        WHERE device_id = %s
        """,
        (device_id,),
    )

    if existing:
        # 기존 row 업데이트
        execute(
            """
            UPDATE notification_devices
            SET
                fcm_token    = %s,
                platform     = %s,
                app_version  = %s,
                timezone     = %s,
                is_active    = TRUE,
                updated_utc  = %s,
                last_seen_utc = %s
            WHERE device_id = %s
            """,
            (
                fcm_token,
                platform,
                app_version,
                tz,
                now_utc,
                now_utc,
                device_id,
            ),
        )
        device_pk = existing["id"]
        created = False
    else:
        # 새 row 생성
        row = fetch_one(
            """
            INSERT INTO notification_devices
                (device_id, fcm_token, platform, app_version,
                 timezone, is_active, created_utc, updated_utc, last_seen_utc)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s, %s)
            RETURNING id
            """,
            (
                device_id,
                fcm_token,
                platform,
                app_version,
                tz,
                now_utc,
                now_utc,
                now_utc,
            ),
        )
        device_pk = row["id"]
        created = True

    return jsonify(
        {
            "ok": True,
            "device_db_id": device_pk,
            "created": created,
        }
    )
