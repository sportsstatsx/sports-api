# hockey/routers/hockey_matchdetail_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request
from hockey.services.hockey_matchdetail_service import hockey_get_game_detail
from hockey.hockey_db import hockey_fetch_one

hockey_matchdetail_bp = Blueprint("hockey_matchdetail", __name__, url_prefix="/api/hockey")

@hockey_matchdetail_bp.route("/games/<int:game_id>")
@hockey_matchdetail_bp.route("/matchdetail/<int:game_id>")  # ✅ 구버전/앱 호환 alias
def hockey_game_detail(game_id: int):
    """
    하키 상세 (+ override 적용)
    - apply_override=0 이면 원본 그대로(어드민 raw 비교용)
    - hockey_match_overrides 테이블 PK는 fixture_id (game_id 컬럼 없음)
    """
    apply_override = str(request.args.get("apply_override", "1")).strip() != "0"

    # deep merge(간단)
    def _dm(a, b):
        if isinstance(a, dict) and isinstance(b, dict):
            out = dict(a)
            for k, v in b.items():
                out[k] = _dm(out.get(k), v)
            return out
        return b

    try:
        base = hockey_get_game_detail(game_id)

        # base가 {ok:true, data:{...}}면 data에 병합, 아니면 전체에 병합
        data = base.get("data") if isinstance(base, dict) and "data" in base else base

        if apply_override and isinstance(data, dict):
            # ✅ FIX: game_id -> fixture_id (DB 스키마와 일치)
            row = hockey_fetch_one(
                "SELECT patch FROM hockey_match_overrides WHERE fixture_id=%s",
                (game_id,),
            )
            p = (row or {}).get("patch")

            if isinstance(p, dict):
                if isinstance(p.get("header"), dict):
                    # header 우선 병합
                    if isinstance(data.get("header"), dict):
                        data["header"] = _dm(data["header"], p["header"])
                    else:
                        data["header"] = p["header"]
                    if "hidden" in p:
                        data["hidden"] = p.get("hidden")
                else:
                    data = _dm(data, p)

                # base 형태 유지
                if isinstance(base, dict) and "data" in base:
                    base["data"] = data
                else:
                    base = data

        return jsonify(base)

    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

