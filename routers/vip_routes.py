# vip_routes.py
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from vip_db import vip_fetch_one

vip_bp = Blueprint("vip", __name__)


def _now_utc():
    return datetime.now(timezone.utc)


@vip_bp.route("/api/vip/status", methods=["POST"])
def vip_status():
    """
    body: { "device_id": "..." }

    응답: { "is_vip": bool, "expires_at": "2025-01-01T00:00:00Z" or null }
    """
    data = request.get_json(silent=True) or {}
    device_id = data.get("device_id")

    if not device_id:
        return jsonify({"error": "device_id is required"}), 400

    # VIP DB에서 해당 디바이스 조회
    row = vip_fetch_one(
        """
        SELECT is_vip, expires_at
        FROM vip_users
        WHERE device_id = %s
        """,
        (device_id,),
    )

    # 기본값: VIP 아님
    is_vip = False
    expires_at = None

    if row:
        # 만료일 있는 경우: 만료가 안 됐을 때만 True
        if row["is_vip"]:
            if row["expires_at"] is None:
                is_vip = True
            else:
                if row["expires_at"] > _now_utc():
                    is_vip = True
                    expires_at = row["expires_at"].isoformat()

    return jsonify(
        {
            "is_vip": is_vip,
            "expires_at": expires_at,
        }
    )
