# routers/matchdetail_router.py

from fastapi import APIRouter, HTTPException, Query
from service.matchdetail.bundle_service import get_match_detail_bundle

router = APIRouter(prefix="/api", tags=["match_detail"])


@router.get("/match_detail_bundle")
async def match_detail_bundle(
    fixture_id: int = Query(...),
    league_id: int = Query(...),
    season: int = Query(...),
):
    """
    매치디테일 화면에서 한 번만 호출하는 번들 엔드포인트.
    - header / form / timeline / lineups / stats / h2h / standings / insights / ai_predictions
      전부 한 번에 내려준다.
    """
    bundle = await get_match_detail_bundle(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if bundle is None:
        raise HTTPException(status_code=404, detail="Match not found")

    return bundle
