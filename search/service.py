from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from db import fetch_all, fetch_one
from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


# ─────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────

def _norm_query(q: str) -> str:
    return (q or "").strip()


def _norm_lower(q: str) -> str:
    return _norm_query(q).lower()


def _like_prefix(q: str) -> str:
    return f"{_norm_lower(q)}%"


def _like_contains(q: str) -> str:
    return f"%{_norm_lower(q)}%"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[Any, ...]] = set()

    for x in items:
        key = (
            x.get("kind"),
            x.get("sport"),
            x.get("league_id"),
            x.get("team_id"),
            x.get("season"),
            (x.get("label") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def _dedupe_cards(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[Any, ...]] = set()

    for x in cards:
        key = (
            x.get("type"),
            x.get("sport"),
            x.get("league_id"),
            x.get("team_id"),
            x.get("season"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


# ─────────────────────────────────────────
# 축구: 시즌 / 대표 진입점
# ─────────────────────────────────────────

def _football_latest_league_season(league_id: int) -> Optional[int]:
    row = fetch_one(
        """
        SELECT MAX(season) AS season
        FROM matches
        WHERE league_id = %s
        """,
        (league_id,),
    )
    season = _safe_int((row or {}).get("season"), 0)
    return season if season > 0 else None


def _football_team_latest_season(team_id: int) -> Optional[int]:
    row = fetch_one(
        """
        SELECT MAX(season) AS season
        FROM team_season_stats
        WHERE team_id = %s
          AND name = 'full_json'
        """,
        (team_id,),
    )
    season = _safe_int((row or {}).get("season"), 0)
    if season > 0:
        return season

    row2 = fetch_one(
        """
        SELECT MAX(season) AS season
        FROM matches
        WHERE home_id = %s OR away_id = %s
        """,
        (team_id, team_id),
    )
    season2 = _safe_int((row2 or {}).get("season"), 0)
    return season2 if season2 > 0 else None


def _football_resolve_team_entry(team_id: int) -> Optional[Dict[str, Any]]:
    """
    축구 팀카드에서 상세 진입용 대표 league_id / season 결정:
    1) team_season_stats 최신 시즌
    2) 그 시즌의 domestic(country 일치) 리그 중 played 최대
    3) domestic이 없으면 played 최대
    4) 그래도 없으면 matches 최신 시즌에서 경기수 최대 league
    """
    season = _football_team_latest_season(team_id)
    if not season:
        return None

    rows = fetch_all(
        """
        SELECT
          tss.league_id,
          COALESCE(l.name, '') AS league_name,
          COALESCE(l.country, '') AS league_country,
          COALESCE(
            (tss.value::jsonb #>> '{fixtures,played,total}')::int,
            0
          ) AS played,
          COALESCE(t.country, '') AS team_country
        FROM team_season_stats tss
        JOIN leagues l
          ON l.id = tss.league_id
        JOIN teams t
          ON t.id = tss.team_id
        WHERE tss.team_id = %s
          AND tss.season = %s
          AND tss.name = 'full_json'
        ORDER BY played DESC, tss.league_id ASC
        """,
        (team_id, season),
    )

    if rows:
        domestic_rows: List[Dict[str, Any]] = []
        other_rows: List[Dict[str, Any]] = []

        for r in rows:
            league_id = _safe_int(r.get("league_id"), 0)
            if league_id <= 0:
                continue

            team_country = (r.get("team_country") or "").strip()
            league_country = (r.get("league_country") or "").strip()

            if team_country and league_country and team_country == league_country:
                domestic_rows.append(r)
            else:
                other_rows.append(r)

        picked = None
        if domestic_rows:
            picked = sorted(
                domestic_rows,
                key=lambda x: (-_safe_int(x.get("played"), 0), _safe_int(x.get("league_id"), 0)),
            )[0]
        elif other_rows:
            picked = sorted(
                other_rows,
                key=lambda x: (-_safe_int(x.get("played"), 0), _safe_int(x.get("league_id"), 0)),
            )[0]

        if picked:
            return {
                "league_id": _safe_int(picked.get("league_id"), 0),
                "league_name": (picked.get("league_name") or "").strip(),
                "season": season,
            }

    row2 = fetch_one(
        """
        SELECT
          league_id,
          COUNT(*) AS played
        FROM matches
        WHERE season = %s
          AND (home_id = %s OR away_id = %s)
        GROUP BY league_id
        ORDER BY played DESC, league_id ASC
        LIMIT 1
        """,
        (season, team_id, team_id),
    )
    if row2:
        league_id = _safe_int(row2.get("league_id"), 0)
        if league_id > 0:
            league_row = fetch_one(
                """
                SELECT name
                FROM leagues
                WHERE id = %s
                LIMIT 1
                """,
                (league_id,),
            )
            return {
                "league_id": league_id,
                "league_name": (league_row or {}).get("name") or "",
                "season": season,
            }

    return None


# ─────────────────────────────────────────
# 하키: 시즌 / 대표 진입점
# ─────────────────────────────────────────

def _hockey_latest_league_season(league_id: int) -> Optional[int]:
    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_games
        WHERE league_id = %s
        """,
        (league_id,),
    )
    season = _safe_int((row or {}).get("season"), 0)
    return season if season > 0 else None


def _hockey_resolve_team_entry(team_id: int) -> Optional[Dict[str, Any]]:
    """
    하키 팀카드 대표 league_id / season:
    - hockey_games 기준 최신 시즌
    - 그 시즌에서 경기 수가 가장 많은 league_id
    """
    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_games
        WHERE home_team_id = %s OR away_team_id = %s
        """,
        (team_id, team_id),
    )
    season = _safe_int((row or {}).get("season"), 0)
    if season <= 0:
        return None

    row2 = hockey_fetch_one(
        """
        SELECT
          league_id,
          COUNT(*) AS played
        FROM hockey_games
        WHERE season = %s
          AND (home_team_id = %s OR away_team_id = %s)
        GROUP BY league_id
        ORDER BY played DESC, league_id ASC
        LIMIT 1
        """,
        (season, team_id, team_id),
    )
    if not row2:
        return None

    league_id = _safe_int((row2 or {}).get("league_id"), 0)
    if league_id <= 0:
        return None

    league_row = hockey_fetch_one(
        """
        SELECT name
        FROM hockey_leagues
        WHERE id = %s
        LIMIT 1
        """,
        (league_id,),
    )

    return {
        "league_id": league_id,
        "league_name": (league_row or {}).get("name") or "",
        "season": season,
    }


# ─────────────────────────────────────────
# 카드 빌더
# ─────────────────────────────────────────

def _build_football_league_card(league_id: int, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
    row = fetch_one(
        """
        SELECT id, name, country, logo
        FROM leagues
        WHERE id = %s
        LIMIT 1
        """,
        (league_id,),
    )
    if not row:
        return None

    resolved_season = season or _football_latest_league_season(league_id)

    return {
        "type": "league",
        "sport": "football",
        "league_id": _safe_int(row.get("id"), 0),
        "season": resolved_season,
        "name": (row.get("name") or "").strip(),
        "logo": row.get("logo"),
        "country": (row.get("country") or "").strip(),
        "subtitle": (row.get("country") or "").strip(),
    }


def _build_football_team_card(team_id: int, league_id: Optional[int] = None, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
    team_row = fetch_one(
        """
        SELECT id, name, country, logo
        FROM teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not team_row:
        return None

    entry = None
    if league_id and season:
        league_row = fetch_one(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            LIMIT 1
            """,
            (league_id,),
        )
        entry = {
            "league_id": league_id,
            "league_name": (league_row or {}).get("name") or "",
            "season": season,
        }
    else:
        entry = _football_resolve_team_entry(team_id)

    if not entry:
        return None

    return {
        "type": "team",
        "sport": "football",
        "team_id": _safe_int(team_row.get("id"), 0),
        "league_id": _safe_int(entry.get("league_id"), 0),
        "season": _safe_int(entry.get("season"), 0),
        "name": (team_row.get("name") or "").strip(),
        "logo": team_row.get("logo"),
        "country": (team_row.get("country") or "").strip(),
        "subtitle": (entry.get("league_name") or "").strip(),
    }


def _build_hockey_league_card(league_id: int, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
    row = hockey_fetch_one(
        """
        SELECT id, name, logo
        FROM hockey_leagues
        WHERE id = %s
        LIMIT 1
        """,
        (league_id,),
    )
    if not row:
        return None

    resolved_season = season or _hockey_latest_league_season(league_id)

    return {
        "type": "league",
        "sport": "hockey",
        "league_id": _safe_int(row.get("id"), 0),
        "season": resolved_season,
        "name": (row.get("name") or "").strip(),
        "logo": row.get("logo"),
        "country": "",
        "subtitle": "Hockey",
    }


def _build_hockey_team_card(team_id: int, league_id: Optional[int] = None, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
    team_row = hockey_fetch_one(
        """
        SELECT id, name, logo
        FROM hockey_teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not team_row:
        return None

    entry = None
    if league_id and season:
        league_row = hockey_fetch_one(
            """
            SELECT name
            FROM hockey_leagues
            WHERE id = %s
            LIMIT 1
            """,
            (league_id,),
        )
        entry = {
            "league_id": league_id,
            "league_name": (league_row or {}).get("name") or "",
            "season": season,
        }
    else:
        entry = _hockey_resolve_team_entry(team_id)

    if not entry:
        return None

    return {
        "type": "team",
        "sport": "hockey",
        "team_id": _safe_int(team_row.get("id"), 0),
        "league_id": _safe_int(entry.get("league_id"), 0),
        "season": _safe_int(entry.get("season"), 0),
        "name": (team_row.get("name") or "").strip(),
        "logo": team_row.get("logo"),
        "country": "",
        "subtitle": (entry.get("league_name") or "").strip(),
    }


# ─────────────────────────────────────────
# 후보(suggest) 생성
# ─────────────────────────────────────────

def _football_suggest_leagues(q: str) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
          id,
          name,
          country,
          logo
        FROM leagues
        WHERE LOWER(name) LIKE %s
        ORDER BY
          CASE
            WHEN LOWER(name) = %s THEN 0
            ELSE 1
          END,
          LENGTH(name) ASC,
          name ASC
        """,
        (_like_prefix(q), _norm_lower(q)),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        league_id = _safe_int(r.get("id"), 0)
        if league_id <= 0:
            continue

        league_name = (r.get("name") or "").strip()
        country = (r.get("country") or "").strip()
        season = _football_latest_league_season(league_id)

        out.append(
            {
                "kind": "league",
                "sport": "football",
                "league_id": league_id,
                "season": season,
                "label": league_name,
                "subLabel": country,
                "logo": r.get("logo"),
                "country": country,
                "league_name": league_name,
                "display_text": league_name,
                "display_subtext": f"{country} : {league_name}" if country else league_name,
            }
        )
    return out


def _football_suggest_direct_teams(q: str) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
          id,
          name,
          country,
          logo
        FROM teams
        WHERE LOWER(name) LIKE %s
        ORDER BY
          CASE
            WHEN LOWER(name) = %s THEN 0
            ELSE 1
          END,
          LENGTH(name) ASC,
          name ASC
        """,
        (_like_prefix(q), _norm_lower(q)),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _safe_int(r.get("id"), 0)
        if team_id <= 0:
            continue

        entry = _football_resolve_team_entry(team_id)
        if not entry:
            continue

        team_name = (r.get("name") or "").strip()
        country = (r.get("country") or "").strip()
        league_name = (entry.get("league_name") or "").strip()

        out.append(
            {
                "kind": "team",
                "sport": "football",
                "team_id": team_id,
                "league_id": _safe_int(entry.get("league_id"), 0),
                "season": _safe_int(entry.get("season"), 0),
                "label": team_name,
                "subLabel": league_name,
                "logo": r.get("logo"),
                "country": country,
                "league_name": league_name,
                "display_text": team_name,
                "display_subtext": f"{country} : {league_name}" if country and league_name else (league_name or country),
            }
        )
    return out


def _football_suggest_teams_by_leagues(leagues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for lg in leagues:
        league_id = _safe_int(lg.get("league_id"), 0)
        season = _safe_int(lg.get("season"), 0)
        league_name = (lg.get("league_name") or lg.get("label") or "").strip()
        country = (lg.get("country") or "").strip()

        if league_id <= 0 or season <= 0:
            continue

        rows = fetch_all(
            """
            SELECT DISTINCT
              t.id AS team_id,
              t.name AS team_name,
              t.country AS team_country,
              t.logo AS team_logo
            FROM matches m
            JOIN teams t
              ON t.id = m.home_id OR t.id = m.away_id
            WHERE m.league_id = %s
              AND m.season = %s
            ORDER BY t.name ASC
            """,
            (league_id, season),
        )

        for r in rows:
            team_id = _safe_int(r.get("team_id"), 0)
            if team_id <= 0:
                continue

            team_name = (r.get("team_name") or "").strip()
            team_country = (r.get("team_country") or "").strip() or country

            out.append(
                {
                    "kind": "team",
                    "sport": "football",
                    "team_id": team_id,
                    "league_id": league_id,
                    "season": season,
                    "label": team_name,
                    "subLabel": league_name,
                    "logo": r.get("team_logo"),
                    "country": team_country,
                    "league_name": league_name,
                    "display_text": team_name,
                    "display_subtext": f"{team_country} : {league_name}" if team_country and league_name else (league_name or team_country),
                }
            )

    return out


def _hockey_suggest_leagues(q: str) -> List[Dict[str, Any]]:
    rows = hockey_fetch_all(
        """
        SELECT
          id,
          name,
          logo
        FROM hockey_leagues
        WHERE LOWER(name) LIKE %s
        ORDER BY
          CASE
            WHEN LOWER(name) = %s THEN 0
            ELSE 1
          END,
          LENGTH(name) ASC,
          name ASC
        """,
        (_like_prefix(q), _norm_lower(q)),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        league_id = _safe_int(r.get("id"), 0)
        if league_id <= 0:
            continue

        league_name = (r.get("name") or "").strip()
        season = _hockey_latest_league_season(league_id)

        out.append(
            {
                "kind": "league",
                "sport": "hockey",
                "league_id": league_id,
                "season": season,
                "label": league_name,
                "subLabel": "Hockey",
                "logo": r.get("logo"),
                "country": "",
                "league_name": league_name,
                "display_text": league_name,
                "display_subtext": league_name,
            }
        )
    return out


def _hockey_suggest_direct_teams(q: str) -> List[Dict[str, Any]]:
    rows = hockey_fetch_all(
        """
        SELECT
          id,
          name,
          logo
        FROM hockey_teams
        WHERE LOWER(name) LIKE %s
        ORDER BY
          CASE
            WHEN LOWER(name) = %s THEN 0
            ELSE 1
          END,
          LENGTH(name) ASC,
          name ASC
        """,
        (_like_prefix(q), _norm_lower(q)),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _safe_int(r.get("id"), 0)
        if team_id <= 0:
            continue

        entry = _hockey_resolve_team_entry(team_id)
        if not entry:
            continue

        team_name = (r.get("name") or "").strip()
        league_name = (entry.get("league_name") or "").strip()

        out.append(
            {
                "kind": "team",
                "sport": "hockey",
                "team_id": team_id,
                "league_id": _safe_int(entry.get("league_id"), 0),
                "season": _safe_int(entry.get("season"), 0),
                "label": team_name,
                "subLabel": league_name,
                "logo": r.get("logo"),
                "country": "",
                "league_name": league_name,
                "display_text": team_name,
                "display_subtext": league_name,
            }
        )
    return out


def _hockey_suggest_teams_by_leagues(leagues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for lg in leagues:
        league_id = _safe_int(lg.get("league_id"), 0)
        season = _safe_int(lg.get("season"), 0)
        league_name = (lg.get("league_name") or lg.get("label") or "").strip()

        if league_id <= 0 or season <= 0:
            continue

        rows = hockey_fetch_all(
            """
            SELECT DISTINCT
              t.id AS team_id,
              t.name AS team_name,
              t.logo AS team_logo
            FROM hockey_games g
            JOIN hockey_teams t
              ON t.id = g.home_team_id OR t.id = g.away_team_id
            WHERE g.league_id = %s
              AND g.season = %s
            ORDER BY t.name ASC
            """,
            (league_id, season),
        )

        for r in rows:
            team_id = _safe_int(r.get("team_id"), 0)
            if team_id <= 0:
                continue

            team_name = (r.get("team_name") or "").strip()

            out.append(
                {
                    "kind": "team",
                    "sport": "hockey",
                    "team_id": team_id,
                    "league_id": league_id,
                    "season": season,
                    "label": team_name,
                    "subLabel": league_name,
                    "logo": r.get("team_logo"),
                    "country": "",
                    "league_name": league_name,
                    "display_text": team_name,
                    "display_subtext": league_name,
                }
            )

    return out


def search_suggest(q: str, sport: str = "all") -> Dict[str, Any]:
    query = _norm_query(q)
    sport_norm = (sport or "all").strip().lower()

    if not query:
        return {
            "query": query,
            "sport": sport_norm,
            "items": [],
        }

    items: List[Dict[str, Any]] = []

    if sport_norm in ("all", "football"):
        football_leagues = _football_suggest_leagues(query)
        football_direct_teams = _football_suggest_direct_teams(query)
        football_teams_by_league = _football_suggest_teams_by_leagues(football_leagues)

        items.extend(football_leagues)
        items.extend(football_direct_teams)
        items.extend(football_teams_by_league)

    if sport_norm in ("all", "hockey"):
        hockey_leagues = _hockey_suggest_leagues(query)
        hockey_direct_teams = _hockey_suggest_direct_teams(query)
        hockey_teams_by_league = _hockey_suggest_teams_by_leagues(hockey_leagues)

        items.extend(hockey_leagues)
        items.extend(hockey_direct_teams)
        items.extend(hockey_teams_by_league)

    items = _dedupe_items(items)

    def _sort_key(x: Dict[str, Any]) -> Tuple[int, int, str, str]:
        kind = (x.get("kind") or "").strip().lower()
        label = (x.get("label") or "").strip().lower()
        ql = _norm_lower(query)

        if label == ql:
            score = 0
        elif label.startswith(ql):
            score = 1
        else:
            score = 2

        kind_score = 0 if kind == "league" else 1
        sub = (x.get("subLabel") or "").strip().lower()
        return (score, kind_score, label, sub)

    items = sorted(items, key=_sort_key)

    return {
        "query": query,
        "sport": sport_norm,
        "items": items,
    }


# ─────────────────────────────────────────
# 후보 선택(resolve) → 카드 생성
# ─────────────────────────────────────────

def _football_league_team_cards(league_id: int, season: int) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT DISTINCT
          t.id AS team_id
        FROM matches m
        JOIN teams t
          ON t.id = m.home_id OR t.id = m.away_id
        WHERE m.league_id = %s
          AND m.season = %s
        ORDER BY t.id ASC
        """,
        (league_id, season),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _safe_int(r.get("team_id"), 0)
        if team_id <= 0:
            continue

        card = _build_football_team_card(team_id=team_id, league_id=league_id, season=season)
        if card:
            out.append(card)

    out = sorted(out, key=lambda x: (x.get("name") or "").lower())
    return out


def _hockey_league_team_cards(league_id: int, season: int) -> List[Dict[str, Any]]:
    rows = hockey_fetch_all(
        """
        SELECT DISTINCT
          t.id AS team_id
        FROM hockey_games g
        JOIN hockey_teams t
          ON t.id = g.home_team_id OR t.id = g.away_team_id
        WHERE g.league_id = %s
          AND g.season = %s
        ORDER BY t.id ASC
        """,
        (league_id, season),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _safe_int(r.get("team_id"), 0)
        if team_id <= 0:
            continue

        card = _build_hockey_team_card(team_id=team_id, league_id=league_id, season=season)
        if card:
            out.append(card)

    out = sorted(out, key=lambda x: (x.get("name") or "").lower())
    return out


def search_resolve(
    *,
    kind: str,
    sport: str,
    league_id: Optional[int] = None,
    team_id: Optional[int] = None,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    kind_norm = (kind or "").strip().lower()
    sport_norm = (sport or "").strip().lower()

    selected: Dict[str, Any] = {
        "kind": kind_norm,
        "sport": sport_norm,
        "league_id": league_id,
        "team_id": team_id,
        "season": season,
    }

    cards: List[Dict[str, Any]] = []

    if sport_norm == "football":
        if kind_norm == "league":
            if not league_id:
                return {"selected": selected, "cards": []}

            resolved_season = season or _football_latest_league_season(league_id)
            if not resolved_season:
                return {"selected": selected, "cards": []}

            selected["season"] = resolved_season

            league_card = _build_football_league_card(league_id=league_id, season=resolved_season)
            if league_card:
                cards.append(league_card)

            cards.extend(_football_league_team_cards(league_id=league_id, season=resolved_season))

        elif kind_norm == "team":
            if not team_id:
                return {"selected": selected, "cards": []}

            team_card = _build_football_team_card(team_id=team_id, league_id=league_id, season=season)
            if team_card:
                selected["league_id"] = team_card.get("league_id")
                selected["season"] = team_card.get("season")
                cards.append(team_card)

    elif sport_norm == "hockey":
        if kind_norm == "league":
            if not league_id:
                return {"selected": selected, "cards": []}

            resolved_season = season or _hockey_latest_league_season(league_id)
            if not resolved_season:
                return {"selected": selected, "cards": []}

            selected["season"] = resolved_season

            league_card = _build_hockey_league_card(league_id=league_id, season=resolved_season)
            if league_card:
                cards.append(league_card)

            cards.extend(_hockey_league_team_cards(league_id=league_id, season=resolved_season))

        elif kind_norm == "team":
            if not team_id:
                return {"selected": selected, "cards": []}

            team_card = _build_hockey_team_card(team_id=team_id, league_id=league_id, season=season)
            if team_card:
                selected["league_id"] = team_card.get("league_id")
                selected["season"] = team_card.get("season")
                cards.append(team_card)

    cards = _dedupe_cards(cards)

    return {
        "selected": selected,
        "cards": cards,
    }
