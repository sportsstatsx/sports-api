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
    filters = header.get("filters") or {}
    comp_val = filters.get("comp", comp) if isinstance(filters, dict) else comp
    last_n_val = filters.get("last_n", last_n) if isinstance(filters, dict) else last_n

    def _pick_team_id(h: Dict[str, Any], side: str) -> Optional[int]:
        """
        header 구조가 버전마다 달라도 최대한 팀 id를 뽑아낸다.
        가능한 키들을 순서대로 탐색.
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

    insights_overall: Dict[str, Any] = {
        "filters": {
            "comp": comp_val,
            "last_n": last_n_val,
        }
    }

    # 홈팀/원정팀 각각 계산해서 기존처럼 home/away로 묶음
    if home_team_id is not None:
        insights_overall["home"] = build_team_insights_overall_block(
            league_id=league_id,
            season=season,
            team_id=home_team_id,
            comp=comp_val or "All",
            last_n=last_n_val or 0,
        )
    else:
        insights_overall["home"] = {}

    if away_team_id is not None:
        insights_overall["away"] = build_team_insights_overall_block(
            league_id=league_id,
            season=season,
            team_id=away_team_id,
            comp=comp_val or "All",
            last_n=last_n_val or 0,
        )
    else:
        insights_overall["away"] = {}


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
