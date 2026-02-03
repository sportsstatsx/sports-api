# services/home_config.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------
# ✅ 0) 네가 사용하는 지원 리그 (고정)
# ------------------------------------------------------------
SUPPORTED_LEAGUE_IDS: List[int] = [
    39, 40, 140, 141, 78, 79, 135, 136, 61, 62, 88, 89, 94, 95,
    203, 204, 144, 145, 207, 208, 119, 98, 99, 292, 293, 307,
    305, 290, 188, 189, 71, 72, 253, 2, 3, 848, 17, 16, 218,
    219, 179, 180, 345, 346, 106, 107, 169
]

# ------------------------------------------------------------
# ✅ 1) 리그 메타 (id -> name/country) : 네가 준 DB 출력 그대로
# ------------------------------------------------------------
LEAGUE_META: Dict[int, Dict[str, str]] = {
    188: {"name": "A-League", "country": "Australia"},
    189: {"name": "Capital Territory NPL", "country": "Australia"},
    219: {"name": "2. Liga", "country": "Austria"},
    218: {"name": "Bundesliga", "country": "Austria"},
    145: {"name": "Challenger Pro League", "country": "Belgium"},
    144: {"name": "Jupiler Pro League", "country": "Belgium"},
    71:  {"name": "Serie A", "country": "Brazil"},
    72:  {"name": "Serie B", "country": "Brazil"},
    169: {"name": "Chinese Super League", "country": "China"},
    345: {"name": "Czech First League", "country": "Czech Republic"},
    346: {"name": "Czech National Football League", "country": "Czech Republic"},
    119: {"name": "Superliga", "country": "Denmark"},
    40:  {"name": "Championship", "country": "England"},
    39:  {"name": "Premier League", "country": "England"},
    61:  {"name": "Ligue 1", "country": "France"},
    62:  {"name": "Ligue 2", "country": "France"},
    79:  {"name": "2. Bundesliga", "country": "Germany"},
    78:  {"name": "Bundesliga", "country": "Germany"},
    290: {"name": "Persian Gulf Pro League", "country": "Iran"},
    135: {"name": "Serie A", "country": "Italy"},
    136: {"name": "Serie B", "country": "Italy"},
    98:  {"name": "J1 League", "country": "Japan"},
    99:  {"name": "J2 League", "country": "Japan"},
    89:  {"name": "Eerste Divisie", "country": "Netherlands"},
    88:  {"name": "Eredivisie", "country": "Netherlands"},
    106: {"name": "Ekstraklasa", "country": "Poland"},
    107: {"name": "I Liga", "country": "Poland"},
    94:  {"name": "Primeira Liga", "country": "Portugal"},
    95:  {"name": "Segunda Liga", "country": "Portugal"},
    305: {"name": "Stars League", "country": "Qatar"},
    307: {"name": "Pro League", "country": "Saudi-Arabia"},
    180: {"name": "Championship", "country": "Scotland"},
    179: {"name": "Premiership", "country": "Scotland"},
    292: {"name": "K League 1", "country": "South-Korea"},
    293: {"name": "K League 2", "country": "South-Korea"},
    140: {"name": "La Liga", "country": "Spain"},
    141: {"name": "Segunda División", "country": "Spain"},
    208: {"name": "Challenge League", "country": "Switzerland"},
    207: {"name": "Super League", "country": "Switzerland"},
    204: {"name": "1. Lig", "country": "Turkey"},
    203: {"name": "Süper Lig", "country": "Turkey"},
    253: {"name": "Major League Soccer", "country": "USA"},
    17:  {"name": "AFC Champions League", "country": "World"},
    16:  {"name": "CONCACAF Champions League", "country": "World"},
    2:   {"name": "UEFA Champions League", "country": "World"},
    848: {"name": "UEFA Europa Conference League", "country": "World"},
    3:   {"name": "UEFA Europa League", "country": "World"},
}

# ------------------------------------------------------------
# ✅ 2) 대륙 분류 (네 규칙 고정)
# ------------------------------------------------------------
CONTINENT_ORDER: List[str] = ["Europe", "Asia", "Americas"]

EUROPE_COUNTRIES = {
    "England", "Spain", "Germany", "Italy", "France",
    "Netherlands", "Portugal", "Belgium", "Austria",
    "Denmark", "Scotland", "Switzerland", "Turkey",
    "Poland", "Czech Republic",
}

ASIA_COUNTRIES = {
    "South-Korea", "Japan", "China", "Australia",
    "Iran", "Qatar", "Saudi-Arabia",
}

AMERICAS_COUNTRIES = {"USA", "Brazil"}

# ------------------------------------------------------------
# ✅ 3) 티어(1부/2부) + 대륙컵(대륙 필터 안으로)
# ------------------------------------------------------------
# 유럽 5대리그 순서 고정: EPL > LaLiga > Bundesliga > Serie A > Ligue 1
EUROPE_TOP5_ORDER: List[int] = [39, 140, 78, 135, 61]

# 대륙컵들은 각 대륙 필터 안에 포함
EUROPE_CONTINENTAL_CUPS: List[int] = [2, 3, 848]   # UEFA CL/EL/UECL
ASIA_CONTINENTAL_CUPS: List[int] = [17]           # AFC CL
AMERICAS_CONTINENTAL_CUPS: List[int] = [16]       # CONCACAF CL


# (country, tier) 매핑: 네가 준 리그셋 기준으로 확정
# tier: 1 or 2, cup은 tier=None + is_cup=True로 다룸
TIER_MAP: Dict[int, int] = {
    # Europe
    39: 1, 40: 2,          # England
    140: 1, 141: 2,        # Spain
    78: 1, 79: 2,          # Germany
    135: 1, 136: 2,        # Italy
    61: 1, 62: 2,          # France
    88: 1, 89: 2,          # Netherlands
    94: 1, 95: 2,          # Portugal
    144: 1, 145: 2,        # Belgium
    218: 1, 219: 2,        # Austria
    119: 1,                # Denmark (2부 없음)
    179: 1, 180: 2,        # Scotland
    207: 1, 208: 2,        # Switzerland
    203: 1, 204: 2,        # Turkey
    106: 1, 107: 2,        # Poland
    345: 1, 346: 2,        # Czech Republic

    # Asia
    292: 1, 293: 2,        # South-Korea
    98: 1, 99: 2,          # Japan
    169: 1,                # China (2부 없음)
    188: 1, 189: 2,        # Australia (너가 넣은 2개 기준으로 2부 취급)
    290: 1,                # Iran
    305: 1,                # Qatar
    307: 1,                # Saudi

    # Americas
    71: 1, 72: 2,          # Brazil
    253: 1,                # USA (2부 없음)
}


def _continent_for_league(league_id: int) -> str:
    meta = LEAGUE_META.get(league_id) or {}
    country = (meta.get("country") or "").strip()

    if league_id in EUROPE_CONTINENTAL_CUPS:
        return "Europe"
    if league_id in ASIA_CONTINENTAL_CUPS:
        return "Asia"
    if league_id in AMERICAS_CONTINENTAL_CUPS:
        return "Americas"

    if country in EUROPE_COUNTRIES:
        return "Europe"
    if country in ASIA_COUNTRIES:
        return "Asia"
    if country in AMERICAS_COUNTRIES:
        return "Americas"

    # fallback: 정의 밖이면 마지막으로
    return "Americas"


def _is_cup(league_id: int) -> bool:
    return league_id in (set(EUROPE_CONTINENTAL_CUPS) | set(ASIA_CONTINENTAL_CUPS) | set(AMERICAS_CONTINENTAL_CUPS))


def _tier_for_league(league_id: int) -> Optional[int]:
    if _is_cup(league_id):
        return None
    return TIER_MAP.get(league_id)


def _name_for_league(league_id: int) -> str:
    meta = LEAGUE_META.get(league_id) or {}
    return (meta.get("name") or f"League {league_id}").strip()


# ------------------------------------------------------------
# ✅ 4) "필터" 순서 키 생성 (네 규칙 그대로)
# ------------------------------------------------------------
def filter_sort_key(league_id: int) -> Tuple[int, int, int, str]:
    """
    필터 정렬 규칙:

    - 대륙: Europe(0) > Asia(1) > Americas(2)
    - 대륙 안:
      - (컵은 해당 대륙 안에 포함)
      - Europe:
         * 5대리그(1부) 고정: EPL > LaLiga > Bundesliga > SerieA > Ligue1
         * 그 외 1부: A~Z
         * 2부: A~Z (1부와 동일 방식)
         * 대륙컵: Europe 안에 포함 (UI상 별도 그룹 가능)
      - Asia/Americas:
         * 1부: A~Z
         * 2부: A~Z
         * 대륙컵: 해당 대륙 안에 포함
    """
    continent = _continent_for_league(league_id)
    c_idx = CONTINENT_ORDER.index(continent) if continent in CONTINENT_ORDER else 99

    iscup = 1 if _is_cup(league_id) else 0
    tier = _tier_for_league(league_id)  # 1/2/None
    tier_idx = 3
    if tier == 1:
        tier_idx = 0
    elif tier == 2:
        tier_idx = 1
    elif iscup:
        # 컵을 어디에 둘지: "대륙 필터 안"에서 별도 그룹으로 보이게 하려면
        # tier_idx=2 로 두면 1부/2부 사이에 낄 수 있음.
        # 나는 일단: 1부 -> 컵 -> 2부 로 배치 (네 정책에 가장 자연스러움)
        tier_idx = 2

    # Europe 5대리그 우선순위
    if continent == "Europe" and (league_id in EUROPE_TOP5_ORDER) and tier == 1:
        top5_rank = EUROPE_TOP5_ORDER.index(league_id)  # 0..4
        return (c_idx, tier_idx, top5_rank, _name_for_league(league_id).lower())

    # 나머지는 A~Z (name 기준)
    # (동명이면 league_id로 안정화)
    return (c_idx, tier_idx, 50, _name_for_league(league_id).lower() + f"#{league_id}")


# ------------------------------------------------------------
# ✅ 5) "홈 매치리스트 섹션" 순서 키 (네 규칙 그대로)
# ------------------------------------------------------------
def home_section_sort_key(league_id: int) -> Tuple[int, int, int, str]:
    """
    홈 매치리스트 섹션 순서:

    - Europe 5대리그 고정 유지
    - Europe 1부 A~Z > Asia 1부 A~Z > Americas 1부 A~Z
    - (그리고) 2부도 동일한 대륙 순서 + A~Z
    - 컵은 해당 대륙 안에 포함 (난: 1부 다음, 2부 전으로 배치)
    """
    continent = _continent_for_league(league_id)
    c_idx = CONTINENT_ORDER.index(continent) if continent in CONTINENT_ORDER else 99

    iscup = 1 if _is_cup(league_id) else 0
    tier = _tier_for_league(league_id)

    # 홈 섹션은 "유럽5대"를 맨 앞에 박고,
    # 그 다음은 (Europe1 -> Asia1 -> Americas1 -> EuropeCup -> AsiaCup -> AmericasCup -> Europe2 -> Asia2 -> Americas2)
    if continent == "Europe" and (league_id in EUROPE_TOP5_ORDER) and tier == 1:
        return (0, 0, EUROPE_TOP5_ORDER.index(league_id), _name_for_league(league_id).lower())

    # 그룹 인덱스 구성
    # 1부: group=1
    # 컵: group=2
    # 2부: group=3
    group = 9
    if tier == 1:
        group = 1
    elif iscup:
        group = 2
    elif tier == 2:
        group = 3

    return (1, group, c_idx, _name_for_league(league_id).lower() + f"#{league_id}")


# ------------------------------------------------------------
# ✅ 6) 서버가 내려주는 "기준 config" 응답 생성
# ------------------------------------------------------------
def build_home_master_config() -> Dict[str, Any]:
    """
    앱이 그대로 렌더링할 수 있는 단일 config.
    """
    leagues: List[Dict[str, Any]] = []
    for lid in SUPPORTED_LEAGUE_IDS:
        meta = LEAGUE_META.get(lid) or {}
        leagues.append(
            {
                "league_id": lid,
                "name": meta.get("name"),
                "country": meta.get("country"),
                "continent": _continent_for_league(lid),
                "tier": _tier_for_league(lid),      # 1/2/None(cup)
                "is_cup": _is_cup(lid),
            }
        )

    # 1) 필터용 정렬
    filter_order = sorted(SUPPORTED_LEAGUE_IDS, key=filter_sort_key)

    # 2) 홈 섹션용 정렬
    home_section_order = sorted(SUPPORTED_LEAGUE_IDS, key=home_section_sort_key)

    return {
        "supported_leagues": leagues,
        "filter_order": filter_order,
        "home_section_order": home_section_order,
        "meta": {
            "continent_order": CONTINENT_ORDER,
            "europe_top5_order": EUROPE_TOP5_ORDER,
            "europe_cups": EUROPE_CONTINENTAL_CUPS,
            "asia_cups": ASIA_CONTINENTAL_CUPS,
            "americas_cups": AMERICAS_CONTINENTAL_CUPS,
        },
    }

def get_home_config() -> Dict[str, Any]:
    """
    /api/home/config 라우터에서 호출하는 진입점.
    (이름만 맞춰주는 얇은 래퍼)
    """
    return build_home_master_config()

def sort_leagues_for_home(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    get_home_leagues() 결과 rows를 '홈 매치리스트 섹션 순서(home_section_order)'로 정렬한다.

    rows item 예시:
      {"league_id": 39, "name": "...", "country": "...", "logo": "...", "season": 2025}
    """
    if not rows:
        return []

    cfg = build_home_master_config()
    order: List[int] = cfg.get("home_section_order") or []

    idx: Dict[int, int] = {}
    for i, lid in enumerate(order):
        try:
            idx[int(lid)] = i
        except (TypeError, ValueError):
            continue

    def _key(r: Dict[str, Any]) -> Tuple[int, str, int]:
        lid = r.get("league_id")
        try:
            lid_int = int(lid)
        except (TypeError, ValueError):
            lid_int = -1

        # home_section_order에 없으면 맨 뒤로
        order_idx = idx.get(lid_int, 10_000)

        name = (r.get("name") or "").strip().lower()
        return (order_idx, name, lid_int)

    return sorted(rows, key=_key)

def build_league_directory_from_config(
    *,
    date_str: Optional[str],
    timezone_str: str,
) -> List[Dict[str, Any]]:
    """
    홈 '리그 선택 바텀시트' 디렉터리:
    - 순서는 home_config의 filter_order를 그대로 사용 (대륙/티어/5대리그/ABC 규칙 반영)
    - 반환은 UI에서 쓰기 쉬운 '섹션' 리스트 형태

    반환 예시:
    [
      {
        "section": "Europe",
        "items": [
          {"league_id": 39, "name": "...", "country": "...", "continent": "...", "tier": 1, "is_cup": False},
          ...
        ]
      },
      ...
    ]

    ⚠️ date_str/timezone_str은 현재 단계에서는 '정렬'에 영향을 주지 않음.
       (오늘 경기 있는 리그만 보여주고 싶으면 이 함수에서 matches로 필터링하는 버전도 가능)
    """
    cfg = build_home_master_config()

    leagues_meta: Dict[int, Dict[str, Any]] = {}
    for x in (cfg.get("supported_leagues") or []):
        try:
            leagues_meta[int(x.get("league_id"))] = x
        except Exception:
            continue

    ordered_ids: List[int] = []
    for lid in (cfg.get("filter_order") or []):
        try:
            ordered_ids.append(int(lid))
        except (TypeError, ValueError):
            continue

    # continent 별로 묶기
    sections: Dict[str, List[Dict[str, Any]]] = {c: [] for c in CONTINENT_ORDER}
    sections.setdefault("Other", [])

    for lid in ordered_ids:
        meta = leagues_meta.get(lid) or {}
        continent = (meta.get("continent") or "").strip() or "Other"
        if continent not in sections:
            continent = "Other"
        sections[continent].append(
            {
                "league_id": lid,
                "name": meta.get("name"),
                "country": meta.get("country"),
                "continent": meta.get("continent"),
                "tier": meta.get("tier"),
                "is_cup": meta.get("is_cup"),
            }
        )

    # 결과는 CONTINENT_ORDER 순서 고정
    out: List[Dict[str, Any]] = []
    for c in CONTINENT_ORDER:
        items = sections.get(c) or []
        if items:
            out.append({"section": c, "items": items})

    # Other가 있으면 마지막에
    other_items = sections.get("Other") or []
    if other_items:
        out.append({"section": "Other", "items": other_items})

    return out
