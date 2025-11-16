# services/insights/utils.py
from __future__ import annotations

from typing import Any


def safe_div(num: Any, den: Any) -> float:
    """0 나눗셈, 타입 에러 방지용 안전 나눗셈."""
    try:
        n = float(num)
        d = float(den)
    except (TypeError, ValueError):
        return 0.0
    if d == 0:
        return 0.0
    return n / d


def fmt_pct(n: Any, d: Any) -> int:
    """(n / d) * 100 을 정수 퍼센트로. d=0 이면 0."""
    v = safe_div(n, d)
    return int(round(v * 100)) if v > 0 else 0


def fmt_avg(sum_val: Any, cnt: Any) -> float:
    """sum / cnt 를 소수 2자리 평균으로. cnt=0 이면 0.0."""
    v = safe_div(sum_val, cnt)
    return round(v, 2) if v > 0 else 0.0
