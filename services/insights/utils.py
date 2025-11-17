# services/insights/utils.py
from __future__ import annotations

from typing import Any


def safe_div(num: Any, den: Any) -> float:
    """
    0 나누기, 타입 오류 등을 모두 0.0 으로 처리하는 안전한 나눗셈.
    """
    try:
        num_f = float(num)
    except (TypeError, ValueError):
        return 0.0

    try:
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0

    if den_f == 0:
        return 0.0

    return num_f / den_f


def fmt_pct(n: Any, d: Any) -> int:
    """
    (n / d) * 100 을 정수 퍼센트로.
    분모가 0이거나 계산이 불가능하면 0.
    """
    v = safe_div(n, d)
    return int(round(v * 100)) if v > 0 else 0


def fmt_avg(n: Any, d: Any) -> float:
    """
    (n / d) 를 소수점 둘째 자리까지 반올림.
    분모가 0이거나 계산이 불가능하면 0.0.
    """
    v = safe_div(n, d)
    return round(v, 2) if v > 0 else 0.0
