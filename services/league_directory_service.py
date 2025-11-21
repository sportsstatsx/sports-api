from __future__ import annotations

from datetime import datetime, date as date_cls, time as time_cls
from typing import Any, Dict, List, Optional, Tuple

import pytz

from db import fetch_all

# ─────────────────────────────────────
#  Country → Continent / Region (소문자 키)
# ─────────────────────────────────────

_COUNTRY_TO_CONTINENT: Dict[str, str] = {
    # Europe
    "england": "europe",
    "spain": "europe",
    "germany": "europe",
    "italy": "europe",
    "france": "europe",
    "netherlands": "europe",
    "portugal": "europe",
    "scotland": "europe",
    "belgium": "europe",
    "turkey": "europe",
    "greece": "europe",
    "sweden": "europe",
    "norway": "europe",
    "denmark": "europe",
    "switzerland": "europe",
    "austria": "europe",
    "czech republic": "europe",
    "czechia": "europe",
    "poland": "europe",
    "croatia": "europe",
    "serbia": "europe",
    "russia": "europe",
    "ukraine": "europe",
    "romania": "europe",
    "bulgaria": "europe",
    "hungary": "europe",

    # Asia
    "south korea": "asia",
    "korea republic": "asia",
    "republic of korea": "asia",
    "south-korea": "asia",          # 하이픈 포함 케이스
    "japan": "asia",
    "saudi arabia": "asia",
    "saudi-arabia": "asia",         # 하이픈 포함 케이스
    "qatar": "asia",
    "united arab emirates": "asia",
    "uae": "asia",
    "china": "asia",
    "iran": "asia",
    "iraq": "asia",
    "uzbekistan": "asia",
    "thailand": "asia",
    "vietnam": "asia",

    # North America
    "usa": "north america",
    "united states": "north america",
    "united states of america": "north america",
    "mexico": "north america",
    "canada": "north america",
    "costa rica": "north america",
    "honduras": "north america",
    "guatemala": "north america",
    "panama": "north america",

    # South America
    "argentina": "south america",
    "brazil": "south america",
    "colombia": "south america",
    "chile": "south america",
    "uruguay": "south america",
    "paraguay": "south america",
    "peru": "south america",
    "ecuador": "south america",
    "bolivia": "south america",
    "venezuela": "south america",

    # Oceania / 기타
    "australia": "asia",            # 🔥 호주는 Asia 그룹으로 묶기
    "new zealand": "other",
    "world": "other",
}

# 대륙 그룹 순서: Europe → Asia → Americas → Other
_CONTINENT_GROUP_ORDER: Dict[str, int] = {
    "Europe": 1,
    "Asia": 2,
    "Americas": 3,
    "Other": 4,
}

# Kotlin MatchRepository.leaguePriority 와 동일한 맵 (fallback 용)
_LEAGUE_PRIORITY: Dict[str, int] = {
    "Premier League": 1,
    "La Liga": 2,
    "LaLiga": 2,
    "Bundesliga": 3,
    "Serie A": 4,
    "Ligue 1": 5,
    "Eredivisie": 6,
    "Primeira Liga": 7,
    "Championship": 20,
    "La Liga 2": 21,
    "2. Bundesliga": 22,
    "K League 1": 100,
    "K League 2": 101,
    "J1 League": 102,
    "J2 League": 103,
    "Saudi Pro League": 110,
    "MLS": 200,
    "CONMEBOL Libertadores": 300,
    "CONMEBOL Sudamericana": 301,
}

# 유럽 빅5 1부리그 우선순위 (대륙 내부 최상단)
_TOP_FIRST_PRIORITY: Dict[str, int] = {
    "Premier League": 1,   # EPL
    "La Liga": 2,          # Spain 1st
    "Bundesliga": 3,       # Germany 1st
    "Ligue 1": 4,          # France 1st
    "Serie A": 5,          # Italy 1st
}

# 유럽 2부리그 우선순위 (1부 순서와 매칭)
_SECOND_DIV_PRIORITY_EUROPE: Dict[str, int] = {
    "Championship": 1,      # England 2부
    "La Liga 2": 2,         # Spain 2부
    "2. Bundesliga": 3,     # Germany 2부
    "Ligue 2": 4,           # France 2부
    "Serie B": 5,           # Italy 2부
}

# 아시아 2부리그 우선순위 (K1/J1 순서와 매칭)
_SECOND_DIV_PRIORITY_ASIA: Dict[str, int] = {
    "K League 2": 1,
    "J2 League": 2,
}

# 2부리그/하위리그 키워드
_SECOND_DIV_KEYWORDS = [
    "2. bundesliga",
    "liga 2",
    "segunda división",
    "segunda division",
    "segunda liga",
    "ligue 2",
    "serie b",
    "primera nacional",
    "primera b",
    "championship",
    "eerste divisie",
    "j2 league",
    "k league 2",
    "liga de expansión mx",
    "expansion mx",
    "b nacional",
    "b serie",
]


# ─────────────────────────────────────
#  날짜 → UTC 하루 범위
# ─────────────────────────────────────

def _get_utc_range_for_local_date(
    date_str: Optional[str],
    timezone_str: str,
) -> Tuple[datetime, datetime]:
    try:
        tz = pytz.timezone(timezone_str)
    except Exception:
        tz = pytz.UTC

    if not date_str:
        local_now = datetime.now(tz)
        local_date = local_now.date()
    else:
        try:
            local_date = datetime.fromisoformat(str(date_str)).date()
        except Exception:
            local_date = datetime.now(tz).date()

    local_start = tz.localize(datetime.combine(local_date, time_cls(0, 0, 0)))
    local_end = tz.localize(datetime.combine(local_date, time_cls(23, 59, 59)))

    utc_start = local_start.astimezone(pytz.UTC)
    utc_end = local_end.astimezone(pytz.UTC)
    return utc_start, utc_end


# ─────────────────────────────────────
#  리그 분류 유틸
# ─────────────────────────────────────

def _normalize_name(name: str) -> str:
    s = name.lower().strip()
    # 라리가 철자 보정: laliga → la liga
    s = s.replace("laliga", "la liga")
    return s


def _country_to_continent(country: Optional[str]) -> Optional[str]:
    if not country:
        return None
    key = country.strip().lower()
    return _COUNTRY_TO_CONTINENT.get(key)


def _is_continental_cup(league_name: str) -> bool:
    n = _normalize_name(league_name)
    if "uefa champions league" in n:
        return True
    if "uefa europa league" in n:
        return True
    if "conference league" in n and "uefa" in n:
        return True
    if "afc champions league" in n:
        return True
    if "concacaf champions league" in n or "concacaf champions cup" in n:
        return True
    if "libertadores" in n or "sudamericana" in n:
        return True
    return False


def _is_domestic_cup(league_name: str) -> bool:
    n = _normalize_name(league_name)
    if _is_continental_cup(league_name):
        return False
    keywords = [
        "fa cup",
        "coppa",
        "copa",
        "taça",
        "taca",
        "pokal",
        "cup",
    ]
    return any(k in n for k in keywords)


def _is_second_division(league_name: str) -> bool:
    n = _normalize_name(league_name)
    return any(k in n for k in _SECOND_DIV_KEYWORDS)


def _detect_continent(country: Optional[str], league_name: str) -> str:
    """
    country + league_name 을 보고 최종 대륙 그룹을 결정한다.

    - South America / North America → Americas 로 통합
    - UEFA / AFC / CONCACAF 등 대륙컵은 이름만 보고도 우선 결정
    - country 값이 애매해도, league_name 안에 들어있는 국가/리그 키워드로 보정
    """
    n = _normalize_name(league_name)

    # 0) 대륙 컵: 이름만 보고 우선 대륙 결정
    if "uefa" in n:
        return "Europe"
    if "afc champions league" in n:
        return "Asia"
    if "concacaf" in n:
        return "Americas"
    if "libertadores" in n or "sudamericana" in n:
        return "Americas"

    # 1) 이름만 보고 강제 매핑 (country 엉망이어도 잡기)
    # Americas 계열
    if any(k in n for k in [
        "brazil", "argentina", "colombia", "uruguay", "paraguay",
        "chile", "peru", "ecuador", "bolivia", "venezuela",
        "mls", "major league soccer", "liga mx", "ligamx",
        "expansion mx", "liga de expansión mx",
    ]):
        return "Americas"

    # Asia 계열
    if any(k in n for k in [
        "k league", "k-league",
        "j1 league", "j2 league", "j-league", "j league",
        "qatar", "saudi",
        "japan", "korea",
        "a-league", "a league",
    ]):
        return "Asia"

    # 2) country 기반 기본 매핑
    base = _country_to_continent(country)

    if base == "north america" or base == "south america":
        return "Americas"
    if base == "europe":
        return "Europe"
    if base == "asia":
        return "Asia"
    if base == "other":
        return "Other"

    # 3) 그래도 못 잡으면 이름 기반 대략 추측
    if any(k in n for k in ["k league", "j1 league", "j2 league", "j-league", "j league"]):
        return "Asia"
    if any(k in n for k in ["mls", "liga mx"]):
        return "Americas"
    if any(k in n for k in ["brasileirao", "serie a (brazil)", "argentina", "brazil"]):
        return "Americas"

    # 4) 마지막 fallback
    return "Other"


def _calc_inner_sort(league_name: str, continent: str) -> int:
    """
    한 대륙 내부에서의 정렬 우선순위 숫자.

    Europe:
      - EPL > La Liga > Bundesliga > Ligue 1 > Serie A > 기타 1부
      - Championship > La Liga 2 > 2. Bundesliga > Ligue 2 > Serie B > 기타 2부
      - 국내컵 > 대륙컵

    Asia:
      - K League 1 > J1 League > A-League > 기타 1부
      - K League 2 > J2 League > 기타 2부
      - 국내컵 > 대륙컵(AFC CL)

    Americas:
      - MLS (Major League Soccer) 최상단
      - 나머지 1부리그 > 2부리그 > 국내컵 > 대륙컵(CONCACAF, Libertadores 등)
    """
    n = _normalize_name(league_name)

    # 6) 대륙 컵은 항상 맨 아래
    if _is_continental_cup(league_name):
        tier = 6
        sub = 0
        return tier * 100 + sub

    # 5) 국내 컵
    if _is_domestic_cup(league_name):
        tier = 5
        sub = 0
        return tier * 100 + sub

    # 4) 2부리그 / 하위리그
    if _is_second_division(league_name):
        tier = 4

        # 유럽: Championship > La Liga 2 > 2. Bundesliga > Ligue 2 > Serie B > 기타 2부
        if continent == "Europe":
            sub = _SECOND_DIV_PRIORITY_EUROPE.get(league_name, 50)
            return tier * 100 + sub

        # 아시아: K League 2 > J2 League > 기타 2부
        if continent == "Asia":
            sub = _SECOND_DIV_PRIORITY_ASIA.get(league_name, 50)
            return tier * 100 + sub

        # 그 외 대륙 2부는 그냥 같은 tier 안에서 이름순
        sub = 0
        return tier * 100 + sub

    # 1~3) 대륙별 1부리그 우선규칙
    # ───────────────── Europe ─────────────────
    if continent == "Europe":
        # 유럽 빅5 1부리그: EPL > La Liga > Bundesliga > Ligue 1 > Serie A
        if league_name in _TOP_FIRST_PRIORITY:
            tier = 1
            sub = _TOP_FIRST_PRIORITY[league_name]  # 1~5
            return tier * 100 + sub

        # 그 외 1부리그
        tier = 2
        sub = 0
        return tier * 100 + sub

    # ───────────────── Asia ─────────────────
    if continent == "Asia":
        # K League 1 > J1 League > A-League
        if "k league 1" in n or "k-league 1" in n:
            tier = 1
            sub = 1
            return tier * 100 + sub
        if "j1 league" in n:
            tier = 1
            sub = 2
            return tier * 100 + sub
        if "a-league" in n or "a league" in n:
            tier = 1
            sub = 3
            return tier * 100 + sub

        # 나머지 1부리그
        tier = 2
        sub = 0
        return tier * 100 + sub

    # ───────────────── Americas ─────────────────
    if continent == "Americas":
        # MLS 최상단 (Major League Soccer / MLS 모두 인식)
        if "major league soccer" in n or "mls" in n:
            tier = 1
            sub = 1
            return tier * 100 + sub

        # 나머지 1부리그
        tier = 2
        sub = 0
        return tier * 100 + sub

    # ───────────────── Other (기타 대륙) ─────────────────
    # 기본: 1부리그 = tier 2
    tier = 2
    sub = 0
    return tier * 100 + sub


def _calc_sort_order(league_name: str, country: Optional[str]) -> Tuple[str, int, int]:
    """
    리그 한 줄에 대해:
      - continent_group (Europe / Asia / Americas / Other)
      - continent_order (1~4)
      - sort_order (대륙 내부 정렬용 숫자)
    를 계산해서 돌려준다.
    """
    continent = _detect_continent(country, league_name)
    # 대문자 첫 글자로 정규화
    if continent == "europe":
        continent = "Europe"
    elif continent == "asia":
        continent = "Asia"
    elif continent == "americas":
        continent = "Americas"
    elif continent == "other":
        continent = "Other"

    continent_order = _CONTINENT_GROUP_ORDER.get(continent, 99)
    inner = _calc_inner_sort(league_name, continent)
    sort_order = continent_order * 1000 + inner
    return continent, continent_order, sort_order


def _calc_display_country(
    league_name: str,
    country: Optional[str],
    continent: str,
) -> Optional[str]:
    """
    앱에서 'Country - League Name' 앞부분에 어떤 텍스트를 보여줄지 결정.

    - 대륙 컵(UCL, AFC CL, CONCACAF, Libertadores 등)은
      World 대신 'Europe' / 'Asia' / 'Americas' 로 노출되게 조정.
    - 나머지는 DB에서 온 country 그대로 사용.
    """
    if _is_continental_cup(league_name):
        # 대륙컵은 World 대신 대륙 이름으로
        if continent in ("Europe", "Asia", "Americas"):
            return continent
    return country


# ─────────────────────────────────────
#  메인: 리그 디렉터리 빌더
# ─────────────────────────────────────

def build_league_directory(
    date_str: Optional[str],
    timezone_str: str,
) -> List[Dict[str, Any]]:
    """
    홈 화면 리그 선택 바텀시트에서 사용하는 "리그 디렉터리"를 만든다.

    - leagues 테이블 전체를 기준으로
    - matches.date_utc 가 해당 로컬 날짜(00:00~23:59, timezone_str 기준)에
      포함되는 경기 수(today_count)를 세고,
    - 각 리그를 Europe / Asia / Americas / Other 중 하나로 분류한 뒤,
    - 대륙 순서(Europe → Asia → Americas → Other) + 내부 우선순위로 정렬한다.
    """
    utc_start, utc_end = _get_utc_range_for_local_date(date_str, timezone_str)

    rows = fetch_all(
        """
        SELECT
            l.id      AS league_id,
            l.name    AS league_name,
            l.country AS country,
            l.logo    AS logo,
            COALESCE(
                SUM(
                    CASE
                        WHEN m.date_utc::timestamptz BETWEEN %s AND %s THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS today_count
        FROM leagues l
        LEFT JOIN matches m
          ON m.league_id = l.id
        GROUP BY
            l.id,
            l.name,
            l.country,
            l.logo
        """,
        (utc_start, utc_end),
    )

    enriched: List[Dict[str, Any]] = []

    for r in rows:
        league_id = r["league_id"]
        league_name = r["league_name"]
        raw_country = r.get("country")
        logo = r.get("logo")
        today_count = r.get("today_count", 0)

        continent, continent_order, sort_order = _calc_sort_order(
            league_name=league_name,
            country=raw_country,
        )

        display_country = _calc_display_country(
            league_name=league_name,
            country=raw_country,
            continent=continent,
        )

        enriched.append(
            {
                "league_id": league_id,
                "league_name": league_name,
                "country": display_country,   # 앱에 노출될 country
                "logo": logo,
                "today_count": today_count,
                "continent": continent,
                "continent_order": continent_order,
                "sort_order": sort_order,
            }
        )

    # 대륙 순서 → 내부 sort_order → league_name 으로 정렬
    enriched.sort(
        key=lambda x: (
            x["continent_order"],
            x["sort_order"],
            x["league_name"],
        )
    )

    return enriched
