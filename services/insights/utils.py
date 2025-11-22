# services/insights/utils.py
from __future__ import annotations

from typing import Any


# ─────────────────────────────────────
#  공통 유틸
# ─────────────────────────────────────

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

    if den_f == 0.0:
        return 0.0

    return num_f / den_f


def fmt_pct(num: Any, den: Any) -> int:
    """
    분자/분모에서 퍼센트(int, 0~100) 를 만들어 준다.
    분모가 0 이면 0 리턴.
    """
    v = safe_div(num, den) * 100.0
    return int(round(v)) if v > 0.0 else 0


def fmt_avg(total: Any, matches: Any, decimals: int = 1) -> float:
    """
    total / matches 의 평균을 소수점 n자리까지 반올림해서 리턴.
    matches <= 0 이면 0.0
    """
    try:
        total_f = float(total)
        matches_i = int(matches)
    except (TypeError, ValueError):
        return 0.0

    if matches_i <= 0:
        return 0.0

    v = total_f / matches_i
    factor = 10 ** decimals
    return round(v * factor) / factor


# ─────────────────────────────────────
#  Insights Overall 필터 파싱 헬퍼
#   - Competition: All / League / Cup / UEFA / ACL / 개별 대회 이름
#   - Last N: "Season", "Last 5", "Last 10" 등
# ─────────────────────────────────────

def normalize_comp(raw: Any) -> str:
    """
    UI에서 내려오는 competition 필터 값을
    서버 내부에서 사용하는 표준 문자열로 정규화.

    새 규칙:
      - None, ""          → "All"
      - "All", "전체"     → "All"
      - "League", "리그"  → "League"
      - "UEFA", "Europe (UEFA)" 등 → "UEFA"
      - "ACL", "AFC Champions League" 등 → "ACL"
      - "Cup", "Domestic Cup", "국내컵" → "Cup"
      - 그 외 문자열(예: "UEFA Champions League", "FA Cup") → 그대로 반환
        → 나중에 competition_detail.competitions 의 name 과 1:1 매칭해서
          특정 대회만 필터링할 때 사용
    """
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    # 이미 우리가 쓰는 표준 값이면 그대로
    if s in ("All", "League", "Cup", "UEFA", "ACL"):
        return s

    lower = s.lower()

    # All
    if lower in ("all", "전체"):
        return "All"

    # League
    if lower in ("league", "리그"):
        return "League"

    # UEFA 계열 (그룹 전체)
    if lower in ("uefa", "europe (uefa)", "europe"):
        return "UEFA"

    # ACL 계열 (아시아 대륙컵 그룹)
    if lower in ("acl", "afc champions league", "asia (acl)", "afc"):
        return "ACL"

    # Domestic Cup 그룹
    if lower in ("cup", "domestic cup", "국내컵"):
        return "Cup"

    # 그 외에는 개별 대회 이름으로 그대로 사용
    #   예: "UEFA Champions League", "FA Cup"
    return s


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
    if isinstance(raw, float):
        try:
            n = int(raw)
            return n if n > 0 else 0
        except (TypeError, ValueError):
            return 0

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
