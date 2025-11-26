# matchdetail/ai_predictions_block.py

from typing import Any, Dict, Optional
from .ai_predictions_engine import compute_ai_predictions_from_overall


def build_ai_predictions_block(
    header: Dict[str, Any],
    insights_overall: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    insights_overall 기반으로 AI Predictions 전체 블록 생성.
    """

    if insights_overall is None:
        return None

    try:
        predictions = compute_ai_predictions_from_overall(insights_overall)
        return predictions
    except Exception as e:
        print(f"[AI_PRED] error: {e}")
        return None
