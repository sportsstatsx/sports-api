from fastapi import APIRouter, HTTPException, Query
from matchdetail.bundle_service import get_match_detail_bundle

router = APIRouter(prefix="/api", tags=["match_detail"])

@router.get("/match_detail_bundle")
async def match_detail_bundle(
    fixture_id: int = Query(...),
    league_id: int = Query(...),
    season: int = Query(...),
):
    bundle = await get_match_detail_bundle(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if bundle is None:
        raise HTTPException(status_code=404, detail="Match not found")

    return bundle
