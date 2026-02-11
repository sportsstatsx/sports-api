# basketball/nba/routers/nba_notifications_router.py
from __future__ import annotations

from flask import Blueprint, jsonify


nba_notifications_bp = Blueprint("nba_notifications_bp", __name__)


@nba_notifications_bp.get("/api/nba/notifications/status")
def nba_notifications_status():
    """
    현재 NBA DB에는 하키처럼 notifications/device/subscription/state 테이블이 없음.
    (네가 psql로 확인한 결과: nba_fetch_state만 존재)
    그래서 이 라우터는 "의도적으로 비활성" 상태로 둔다.
    """
    return jsonify({"ok": False, "error": "nba_notifications_not_configured"}), 404
