# ==============================================================
# matchdetail_router.py  (A방식 + comp/last_n 지원 완전체)
# ==============================================================

from fastapi import APIRouter, Request
from services.bundle_service import build_match_detail_bundle

router = APIRouter()


@router.get("/match_detail_bundle")
async def match_detail_bundle(request: Request):
    """
    API:
      /api/match_detail_bundle?fixture_id=xxx&league_id=xxx&season=2025
                              &comp=League&last_n=Last%205

    A방식: 여기서는 DB만 보고, 모든 insights 계산을 서버에서 수행.
    """

    q = request.query_params

    fixture_id = q.get("fixture_id")
    league_id = q.get("league_id")
    season = q.get("season")

    # 🔥 새 필터
    comp = q.get("comp", "All")
    last_n = q.get("last_n", "Last 10")

    if not fixture_id or not league_id or not season:
        return {"ok": False, "error": "fixture_id / league_id / season required"}

    try:
        fixture_id_int = int(fixture_id)
        league_id_int = int(league_id)
        season_int = int(season)
    except:
        return {"ok": False, "error": "Invalid fixture_id/league_id/season"}

    data = build_match_detail_bundle(
        fixture_id=fixture_id_int,
        league_id=league_id_int,
        season_int=season_int,
        comp=comp,
        last_n=last_n
    )

    return {"ok": True, "data": data}
