# routers/vip_routes.py
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from vip_db import vip_fetch_one, vip_execute

vip_bp = Blueprint("vip", __name__)


def _now_utc():
    return datetime.now(timezone.utc)


@vip_bp.route("/api/vip/status", methods=["POST"])
def vip_status():
    """
    body: { "device_id": "xxxx" }

    응답:
    {
      "is_vip": true/false,
      "expires_at": "2025-01-01T00:00:00Z" or null
    }
    """
    data = request.get_json(silent=True) or {}
    device_id = data.get("device_id")

    if not device_id:
        return jsonify({"error": "device_id is required"}), 400

    # 1) 해당 device_id 가 vip_users 에 이미 있는지 조회
    row = vip_fetch_one(
        """
        SELECT is_vip, expires_at
        FROM vip_users
        WHERE device_id = %s
        """,
        (device_id,),
    )

    # 2) 없으면 -> 베타 기간 동안은 그냥 VIP 로 하나 만들어주고 반환
    if row is None:
        vip_execute(
            """
            INSERT INTO vip_users (device_id, is_vip, expires_at)
            VALUES (%s, TRUE, NULL)
            """,
            (device_id,),
        )

        return jsonify(
            {
                "is_vip": True,
                "expires_at": None,
            }
        )

    # 3) 있으면 -> 만료 여부만 간단히 체크해서 돌려줌
    is_vip = bool(row["is_vip"])
    expires_at = row["expires_at"]

    # 만료일이 있고, 이미 지났으면 VIP 아님
    if expires_at is not None and expires_at < _now_utc():
        is_vip = False

    return jsonify(
        {
            "is_vip": is_vip,
            "expires_at": expires_at.isoformat() if expires_at else None,
        }
    )
