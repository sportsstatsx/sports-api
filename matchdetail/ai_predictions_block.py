from typing import Any, Dict, Optional

from .ai_predictions_engine import compute_ai_predictions_from_overall


def build_ai_predictions_block(
    header: Dict[str, Any],
    insights_overall: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    matchdetail/insights_block 에서 만든 insights_overall 블록을 기반으로
    AI Predictions 블록(dict)을 생성한다.

    header 안의 comp / last_n 필터는 이미 insights_overall 에 반영되어 있다고
    가정하고, 여기서는 insights_overall 만 사용한다.
    """
    if not insights_overall:
        return None

    try:
        return compute_ai_predictions_from_overall(insights_overall)
    except Exception as e:
        # 문제가 생겨도 번들 전체가 죽지 않도록 방어
        print(f"[AI_PREDICTIONS] error while computing predictions: {e}")
        return None
