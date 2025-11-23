# services/matchdetail/form_block.py

from typing import Any, Dict


def build_form_block(header: Dict[str, Any]) -> Dict[str, Any]:
    """
    TODO: FormRepository 가 하던 최근 경기 폼 계산을
    DB + Python 로직으로 옮겨서 구현.

    1단계: 빈 구조만 내려보낸다.
    """
    return {
        "home_last5": [],
        "away_last5": [],
        "home_goals_for": None,
        "home_goals_against": None,
        "away_goals_for": None,
        "away_goals_against": None,
    }
