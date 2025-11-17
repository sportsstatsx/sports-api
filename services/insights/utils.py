# services/insights/utils.py
from __future__ import annotations

from typing import Any, Optional


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


# ─────────────────────────────────────
#  필터용 공통 유틸 (Competition / Last N)
# ─────────────────────────────────────

def normalize_comp(raw: Any) -> str:
    """
    클라이언트에서 넘어오는 competition 필터 문자열을
    서버 내부에서 쓸 표준 형태로 정규화한다.

    예)
      "league" / "League"      → "League"
      "cup" / "Cup"            → "Cup"
      "europe" / "UCL" 등      → "Europe (UEFA)"
      "continental" 등         → "Continental"
      그 외 / 빈값 / None      → "All"

    ⚠️ 지금 단계에서는 아직 각 섹션 쿼리에서 이 값을 사용하지 않고,
       이후 단계에서 공통 match 샘플을 만들 때 사용할 예정.
    """
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    lower = s.lower()

    # 이미 표준 키워드로 온 경우
    if s in ("All", "League", "Cup", "Europe (UEFA)", "Continental"):
        return s

    if "league" in lower:
        return "League"
    if "cup" in lower:
        return "Cup"
    if any(k in lower for k in ("europe", "uefa", "ucl", "uel", "conference")):
        return "Europe (UEFA)"
    if any(k in lower for k in ("continental", "international", "afc", "conmebol", "concacaf")):
        return "Continental"

    # 그 외는 모두 All 로 통일
    return "All"


def parse_last_n(raw: Any) -> int:
    """
    클라이언트에서 넘어오는 lastN 값을 안전하게 정수 N 으로 변환.

    규칙:
      - None / 빈 문자열         → 0  (0 = 시즌 전체 사용)
      - "Season" / "All"         → 0
      - "Last 5", "last 10" 등   → 5, 10 추출
      - "7" 처럼 숫자 문자열     → 7
      - 잘못된 형식              → 0

    이 값은 나중에
      ORDER BY date DESC LIMIT N
    형태로 사용할 수 있다.
    """
    if raw is None:
        return 0

    if isinstance(raw, int):
        return raw if raw > 0 else 0

    s = str(raw).strip()
    if not s:
        return 0

    lower = s.lower()
    if lower in ("season", "all"):
        return 0

    # "Last 5", "last 10" 같은 형태
    if lower.startswith("last"):
        parts = s.split()
        for p in parts:
            if p.isdigit():
                n = int(p)
                return n if n > 0 else 0
        return 0

    # 그냥 숫자 문자열
    if s.isdigit():
        n = int(s)
        return n if n > 0 else 0

    # 그 외는 모두 0
    return 0
