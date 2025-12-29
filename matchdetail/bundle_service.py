# matchdetail/bundle_service.py

from typing import Any, Dict, Optional

from .header_block import build_header_block
from .form_block import build_form_block
from .timeline_block import build_timeline_block
from .lineups_block import build_lineups_block
from .stats_block import build_stats_block
from .h2h_block import build_h2h_block
from .standings_block import build_standings_block
from .insights_block import build_team_insights_overall_block
from .ai_predictions_block import build_ai_predictions_block


def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
    *,
    comp: Optional[str] = None,
    last_n: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    매치디테일 번들의 진입점 (sync 버전).
    comp / last_n 필터를 라우터에서 받아 header.filters 에 반영한다.
    """

    # 1) header 블록 생성
    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if header is None:
        return None

    # 2) 🔥 comp / last_n 필터 덮어쓰기 (앱 → 서버)
    header_filters = header.get("filters", {})  # header_block 기본값 있음

    if comp is not None:
        header_filters["comp"] = comp

    if last_n is not None:
        header_filters["last_n"] = last_n

    header["filters"] = header_filters  # 다시 덮어쓰기

    # 3) 나머지 블록
    form = build_form_block(header)
    timeline = build_timeline_block(header)
    lineups = build_lineups_block(header)
    stats = build_stats_block(header)
    h2h = build_h2h_block(header)
    standings = build_standings_block(header)

    # 🔥 여기서부터 comp + last_n 필터를 사용하는 insights 계산
    filters_for_client = header.get("filters") or {}
    if not isinstance(filters_for_client, dict):
        filters_for_client = {}

    # header.filters 우선, 없으면 함수 인자 comp/last_n 사용
    comp_val = filters_for_client.get("comp", comp)
    last_n_val = filters_for_client.get("last_n", last_n)

    def _pick_team_id(h: Dict[str, Any], side: str) -> Optional[int]:
        """
        header 구조가 버전마다 달라도 team_id를 최대한 뽑는다.
        """
        candidates = [
            h.get(f"{side}_team_id"),
            (h.get(side) or {}).get("team_id") if isinstance(h.get(side), dict) else None,
            (h.get(side) or {}).get("id") if isinstance(h.get(side), dict) else None,
            ((h.get("teams") or {}).get(side) or {}).get("id")
            if isinstance(h.get("teams"), dict) and isinstance((h.get("teams") or {}).get(side), dict)
            else None,
            ((h.get("teams") or {}).get(side) or {}).get("team_id")
            if isinstance(h.get("teams"), dict) and isinstance((h.get("teams") or {}).get(side), dict)
            else None,
        ]
        for v in candidates:
            if v is None:
                continue
            try:
                return int(v)
            except (TypeError, ValueError):
                continue
        return None

    home_team_id = _pick_team_id(header, "home")
    away_team_id = _pick_team_id(header, "away")

    home_ins: Dict[str, Any] = {}
    away_ins: Dict[str, Any] = {}

    if home_team_id is not None:
        home_ins = build_team_insights_overall_block(
            league_id=league_id,
            season=season,
            team_id=home_team_id,
            comp=comp_val or "All",
            last_n=last_n_val or 0,
        )

    if away_team_id is not None:
        away_ins = build_team_insights_overall_block(
            league_id=league_id,
            season=season,
            team_id=away_team_id,
            comp=comp_val or "All",
            last_n=last_n_val or 0,
        )

    # ✅ (중요) 예전 build_insights_overall_block(header)와 동일한 스키마로 맞춤
    insights_overall = {
        "league_id": league_id,
        "season": season,
        "last_n": last_n_val,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "filters": filters_for_client,
        "home": home_ins,
        "away": away_ins,
    }




    # 🔥 insights_overall 를 이용한 AI Predictions 블록
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    return {
        "header": header,
        "form": form,
        "timeline": timeline,
        "lineups": lineups,
        "stats": stats,
        "h2h": h2h,
        "standings": standings,
        "insights_overall": insights_overall,
        "ai_predictions": ai_predictions,
    }
