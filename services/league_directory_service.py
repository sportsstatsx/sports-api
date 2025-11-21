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
    "south-korea": "asia",
    "japan": "asia",
    "saudi arabia": "asia",
    "saudi-arabia": "asia",
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

# 유럽 빅5 1부리그 우선순위
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

# 2부리그/하위리그 키워드  🔥 (여기 추가해서 2부 정확히 잡기)
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
    "challenger pro league",   # Belgium 2부
    "challenge league",       # Switzerland 2부
    "1. lig",                 # Turkey 2부
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
    if any(k in n for k in [
        "brazil", "argentina", "colombia", "uruguay", "paraguay",
        "chile", "peru", "ecuador", "bolivia", "venezuela",
        "mls", "major league soccer", "liga mx", "ligamx",
        "expansion mx", "liga de expansión mx",
    ]):
        return "Americas"

    if any(k in n for k in [
        "k league", "k-league",
        "j1 league", "j2 league", "j-league", "j league",
        "qatar", "saudi",
        "japan", "korea",
        "a-league", "a league",
    ]):
        return "Asia"

    base = _country_to_continent(country)

    if base == "north america" or base == "south america":
        return "Americas"
    if base == "europe":
        return "Europe"
    if base == "asia":
        return "Asia"
    if base == "other":
        return "Other"

    if any(k in n for k in ["k league", "j1 league", "j2 league", "j-league", "j league"]):
        return "Asia"
    if any(k in n for k in ["mls", "liga mx"]):
        return "Americas"
    if any(k in n for k in ["brasileirao", "serie a (brazil)", "argentina", "brazil"]):
        return "Americas"

    return "Other"


def _calc_inner_sort(league_name: str, continent: str) -> int:
    """
    Europe:
      EPL > La Liga > Bundesliga > Ligue 1 > Serie A > 기타 1부
      → 그 다음 2부 (Championship, La Liga 2, 2. Bundesliga, Ligue 2, Serie B, 기타 2부)
      → 국내컵 → 대륙컵

    Asia:
      K League 1 > J1 League > A-League > 기타 1부
      → K League 2 > J2 League > 기타 2부
      → 국내컵 → AFC CL

    Americas:
      MLS 최상단 → 기타 1부 → 2부 → 국내컵 → 대륙컵
    """
    n = _normalize_name(league_name)

    # 6) 대륙 컵
    if _is_continental_cup(league_name):
        tier = 6
        sub = 0
        return tier * 100 + sub

    # 5) 국내 컵
    if _is_domestic_cup(league_name):
        tier = 5
        sub = 0
        return tier * 100 + sub

    # 4) 2부리그
    if _is_second_division(league_name):
        tier = 4

        if continent == "Europe":
            sub = _SECOND_DIV_PRIORITY_EUROPE.get(league_name, 50)
            return tier * 100 + sub

        if continent == "Asia":
            sub = _SECOND_DIV_PRIORITY_ASIA.get(league_name, 50)
            return tier * 100 + sub

        sub = 0
        return tier * 100 + sub

    # 1~3) 1부리그

    # Europe
    if continent == "Europe":
        if league_name in _TOP_FIRST_PRIORITY:
            tier = 1
            sub = _TOP_FIRST_PRIORITY[league_name]
            return tier * 100 + sub
        tier = 2
        sub = 0
        return tier * 100 + sub

    # Asia
    if continent == "Asia":
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
        tier = 2
        sub = 0
        return tier * 100 + sub

    # Americas
    if continent == "Americas":
        if "major league soccer" in n or "mls" in n:
            tier = 1
            sub = 1
            return tier * 100 + sub
        tier = 2
        sub = 0
        return tier * 100 + sub

    # Other
    tier = 2
    sub = 0
    return tier * 100 + sub


def _calc_sort_order(league_name: str, country: Optional[str]) -> Tuple[str, int, int]:
    continent = _detect_continent(country, league_name)
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
    if _is_continental_cup(league_name):
        if continent in ("Europe", "Asia", "Americas"):
            return continent
    return country


def build_league_directory(
    date_str: Optional[str],
    timezone_str: str,
) -> List[Dict[str, Any]]:
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
                "country": display_country,
                "logo": logo,
                "today_count": today_count,
                "continent": continent,
                "continent_order": continent_order,
                "sort_order": sort_order,
            }
        )

    enriched.sort(
        key=lambda x: (
            x["continent_order"],
            x["sort_order"],
            x["league_name"],
        )
    )

    return enriched
