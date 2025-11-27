# matchdetail/ai_predictions_block.py

from typing import Any, Dict, Optional, List

from db import fetch_all
from .ai_predictions_engine import compute_ai_predictions_from_overall


def _is_continental_league(league_id: Any) -> bool:
    """
    현재 fixture 의 league_id 가 UEFA / ACL 같은 대륙컵 계열인지 간단히 판별.
    - leagues.name 을 한 번 조회해서 문자열로 체크한다.
    """
    try:
        lid = int(league_id)
    except (TypeError, ValueError):
        return False

    try:
        rows = fetch_all(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            LIMIT 1
            """,
            (lid,),
        )
    except Exception:
        return False

    if not rows:
        return False

    name = (rows[0].get("name") or "").strip().lower()
    if not name:
        return False

    # UEFA / 유럽 대륙컵 계열
    if (
        "uefa" in name
        or "champions league" in name
        or "europa league" in name
        or "conference league" in name
    ):
        return True

    # 아시아 ACL 계열
    if "afc" in name or "acl" in name or "afc champions league" in name:
        return True

    return False


def _build_ai_comp_block(
    *,
    header: Dict[str, Any],
    insights_overall: Dict[str, Any],
) -> Dict[str, Any]:
    """
    insights_overall.filters.comp 를 기반으로
    AI Predictions 전용 comp 블록을 만든다.

    - 기본: insights_overall 과 동일한 options/selected
    - 대륙컵 경기(UEFA / ACL 등)일 때:
        → All + (컵/대륙컵 계열 이름)만 남기고, 각 리그 이름은 제거
    """
    filters_overall = insights_overall.get("filters") or {}
    comp_block = filters_overall.get("comp") or {}

    raw_options = list(comp_block.get("options") or [])
    raw_selected = comp_block.get("selected") or "All"

    league_id = header.get("league_id")
    is_continental = _is_continental_league(league_id)

    # 기본값: 그대로 복사
    ai_options: List[str] = raw_options[:]
    ai_selected: str = str(raw_selected) if raw_selected is not None else "All"

    if is_continental and raw_options:
        kept: List[str] = []

        for opt in raw_options:
            s = str(opt).strip()
            if not s:
                continue

            # All 은 항상 유지
            if s == "All":
                kept.append(s)
                continue

            lower = s.lower()

            # 컵 / 대륙컵 계열만 남긴다
            is_cup = (
                "cup" in lower
                or "copa" in lower
                or "컵" in lower
                or "taça" in lower
                or "杯" in lower
            )
            is_uefa = (
                "uefa" in lower
                or "champions league" in lower
                or "europa league" in lower
                or "conference league" in lower
            )
            is_acl = (
                "afc" in lower
                or "acl" in lower
                or "afc champions league" in lower
            )

            if is_cup or is_uefa or is_acl:
                if s not in kept:
                    kept.append(s)

        # 최소 한 개는 보장
        ai_options = kept or ["All"]

        # 선택 값이 빠졌으면 All 로 폴백
        if ai_selected not in ai_options:
            ai_selected = "All"

    return {
        "options": ai_options,
        "selected": ai_selected,
    }


def _build_ai_last_n_block(insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    """
    last_n 은 그냥 insights_overall 쪽 값을 그대로 복사해서 내려준다.
    (나중에 필요하면 여기서만 별도로 커스터마이징 가능)
    """
    filters_overall = insights_overall.get("filters") or {}
    last_n_block = filters_overall.get("last_n") or {}

    options = list(last_n_block.get("options") or [])
    selected = last_n_block.get("selected") or "Last 10"

    return {
        "options": options,
        "selected": selected,
    }


def build_ai_predictions_block(
    header: Dict[str, Any],
    insights_overall: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    matchdetail/insights_block 에서 만든 insights_overall 블록을 기반으로
    AI Predictions 블록(dict)을 생성한다.

    - 예측 계산 자체는 기존과 동일하게
      compute_ai_predictions_from_overall(insights_overall) 만 사용.
    - 추가로, AI Predictions 전용 filters(comp/last_n) 블록을 함께 내려준다.
      → 나중에 앱에서 AI 탭은 이 filters 를 사용하면
        다른 섹션과 독립적으로 comp 옵션을 제어할 수 있다.
    """
    if not insights_overall:
        return None

    try:
        predictions = compute_ai_predictions_from_overall(insights_overall)
        if not isinstance(predictions, dict):
            return None

        # 🔥 새로 추가: AI 전용 필터 블록
        filters_block = {
            "comp": _build_ai_comp_block(
                header=header,
                insights_overall=insights_overall,
            ),
            "last_n": _build_ai_last_n_block(insights_overall),
        }

        # 기존 필드는 그대로 두고, filters 만 추가
        predictions["filters"] = filters_block
        return predictions

    except Exception as e:
        # 문제가 생겨도 번들 전체가 죽지 않도록 방어
        print(f"[AI_PREDICTIONS] error while computing predictions: {e}")
        return None
