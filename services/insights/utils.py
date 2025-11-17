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


# ─────────────────────────────────────
#  Insights Overall 필터 파싱 헬퍼
#   - Competition: All / League / Cup / Europe (UEFA) / Continental
#   - Last N: "Season", "Last 5", "Last 10" 등
# ─────────────────────────────────────

def normalize_comp(raw: Any) -> str:
    """
    UI에서 내려오는 competition 필터 값을
    서버 내부에서 사용하는 표준 문자열로 정규화.

    반환값은 아래 중 하나:
      - "All"
      - "League"
      - "Cup"
      - "Europe (UEFA)"
      - "Continental"
    """
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    # 이미 우리가 쓰는 표준 값이면 그대로 돌려준다.
    if s in ("All", "League", "Cup", "Europe (UEFA)", "Continental"):
        return s

    lower = s.lower()

    if "league" in lower:
        return "League"
    if "cup" in lower:
        return "Cup"
    if any(k in lower for k in ("europe", "uefa", "ucl", "uel", "conference")):
        return "Europe (UEFA)"
    if any(k in lower for k in ("continental", "international", "afc", "conmebol", "concacaf")):
        return "Continental"

    # 인식 못 하면 안전하게 전체
    return "All"


def parse_last_n(raw: Any) -> int:
    """
    UI에서 내려오는 lastN 값을 안전하게 정수 N 으로 변환.

    규칙:
      - None / "", "Season", "All"         → 0   (0 = 시즌 전체 사용)
      - "Last 5", "last 10"                → 5, 10
      - "5", "10" 같은 숫자 문자열        → 그대로 정수
      - 그 외 잘못된 형식                 → 0
    """
    if raw is None:
        return 0

    # 이미 숫자면 그대로
    if isinstance(raw, int):
        return raw if raw > 0 else 0

    s = str(raw).strip()
    if not s:
        return 0

    lower = s.lower()
    if lower in ("season", "all", "full season"):
        return 0

    # "Last 5", "Last 10" 등에서 숫자만 추출
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        try:
            n = int(digits)
            return n if n > 0 else 0
        except ValueError:
            return 0

    # 마지막 fallback: 전체 문자열이 숫자일 때
    if s.isdigit():
        n = int(s)
        return n if n > 0 else 0

    return 0
