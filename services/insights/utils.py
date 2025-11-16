# services/insights/utils.py
from __future__ import annotations

from typing import Any


def safe_div(num: Any, den: Any) -> float:
    """
    안전한 나눗셈 유틸리티.
    0 나누기, None, 문자열 등 들어와도 0.0 반환.
    """
    try:
        num_f = float(num)
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0
    if den_f == 0:
        return 0.0
    return num_f / den_f


def fmt_pct(n: Any, d: Any) -> int:
    """
    비율을 % 정수로 반환. (0~100)
    """
    v = safe_div(n, d)
    return int(round(v * 100)) if v > 0 else 0


def fmt_avg(n: Any, d: Any) -> float:
    """
    평균 값을 소수점 2자리까지 반환.
    """
    v = safe_div(n, d)
    return round(v, 2) if v > 0 else 0.0
