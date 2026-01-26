# routers/version_router.py

import os
from flask import Blueprint, jsonify, request

version_bp = Blueprint("version_bp", __name__)

def _as_int(v: str, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default

@version_bp.get("/api/app/version_policy")
def version_policy():
    """
    Public endpoint:
      GET /api/app/version_policy?platform=android

    기본은 android만 쓰면 충분.
    """
    platform = (request.args.get("platform") or "android").strip().lower()

    # ✅ env로 관리 (서버 배포 없이 Render env만 바꿔도 정책 변경 가능)
    latest_code = _as_int(os.getenv("ANDROID_LATEST_VERSION_CODE", "1"), 1)
    min_supported_code = _as_int(os.getenv("ANDROID_MIN_SUPPORTED_VERSION_CODE", "1"), 1)

    title = (os.getenv("ANDROID_UPDATE_TITLE", "") or "").strip() or "Update Available"
    message = (os.getenv("ANDROID_UPDATE_MESSAGE", "") or "").strip() or "Please update to the latest version."
    store_url = (os.getenv("PLAY_STORE_URL", "") or "").strip()

    # 혹시 min > latest 같은 실수 방어
    if min_supported_code > latest_code:
        min_supported_code = latest_code

    return jsonify({
        "ok": True,
        "platform": platform,
        "latest_version_code": latest_code,
        "min_supported_version_code": min_supported_code,
        "title": title,
        "message": message,
        "store_url": store_url,
    })
