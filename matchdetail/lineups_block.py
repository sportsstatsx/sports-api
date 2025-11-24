# services/matchdetail/lineups_block.py

from typing import Any, Dict, Optional


def build_lineups_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    match_lineups 기반 선발/교체/포메이션 구현.

    1단계: header 기반 스켈레톤만 내려주고,
           실제 선발/교체/포메이션/코치는 2단계에서 DB(match_lineups) 붙인다.

    기대 JSON 구조 (match_detail_bundle.lineups):

    "lineups": {
      "home": {
        "team_id": 40,
        "team_name": "Liverpool",
        "team_logo": "https://...",
        "formation": null,
        "coach": {
          "id": null,
          "name": null
        },
        "starting_xi": [],
        "substitutes": []
      },
      "away": { ... 동일 구조 ... }
    }
    """

    # header_block 이 이미 이런 구조로 내려주고 있음:
    # {
    #   "fixture_id": ...,
    #   "league_id": ...,
    #   "season": ...,
    #   "home": { "id": ..., "name": ..., "logo": ... },
    #   "away": { "id": ..., "name": ..., "logo": ... },
    #   ...
    # }
    home = header.get("home") or {}
    away = header.get("away") or {}

    def build_side(side: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "team_id": side.get("id"),
            "team_name": side.get("name"),
            "team_logo": side.get("logo"),
            "formation": None,  # 2단계에서 match_lineups 기반으로 채움
            "coach": {
                "id": None,
                "name": None,
            },
            "starting_xi": [],
            "substitutes": [],
        }

    return {
        "home": build_side(home),
        "away": build_side(away),
    }
