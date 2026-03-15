from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from db import fetch_all, fetch_one
from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one

from leaguedetail.seasons_block import resolve_season_for_league
from hockey.leaguedetail.hockey_seasons_block import resolve_season_for_league as hockey_resolve_season_for_league


MAX_SUGGESTIONS = 100
MAX_EXPANDED_LEAGUES = 8


def _norm_sport(sport: str) -> str:
    s = (sport or "").strip().lower()
    return s if s in ("all", "football", "hockey") else "all"


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Tuple[str, str, int]] = set()
    out: List[Dict[str, Any]] = []

    for item in items:
        kind = str(item.get("kind") or "").strip().lower()
        sport = str(item.get("sport") or "").strip().lower()

        raw_id = (
            item.get("team_id")
            if kind == "team"
            else item.get("league_id")
        )
        try:
            obj_id = int(raw_id)
        except Exception:
            continue

        key = (kind, sport, obj_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def _prefix_contains_order_sql(field_name: str = "name") -> str:
    return f"""
        CASE
            WHEN LOWER({field_name}) = LOWER(%s) THEN 300
            WHEN LOWER({field_name}) LIKE LOWER(%s) THEN 200
            WHEN LOWER({field_name}) LIKE LOWER(%s) THEN 100
            ELSE 0
        END
    """


# ─────────────────────────────────────────
# Football: season / cards / suggestions
# ─────────────────────────────────────────

def _resolve_football_league_season(league_id: int) -> Optional[int]:
    return resolve_season_for_league(league_id=league_id, season=None)


def _build_football_league_card(league_id: int) -> Optional[Dict[str, Any]]:
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

    season = _resolve_football_league_season(league_id)
    return {
        "type": "league",
        "sport": "football",
        "league_id": int(row["id"]),
        "season": season,
        "name": row.get("name") or "",
        "logo": row.get("logo"),
        "country": row.get("country") or "",
        "subtitle": row.get("country") or "",
    }


def _resolve_football_team_entry(team_id: int) -> Optional[Dict[str, Any]]:
    team = fetch_one(
        """
        SELECT id, name, country, logo
        FROM teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not team:
        return None

    team_country = (team.get("country") or "").strip()

    rows = fetch_all(
        """
        SELECT
            m.season,
            m.league_id,
            l.name AS league_name,
            l.country AS league_country,
            l.logo AS league_logo,
            COUNT(*) AS played
        FROM matches m
        JOIN leagues l
          ON l.id = m.league_id
        WHERE (m.home_id = %s OR m.away_id = %s)
        GROUP BY
            m.season,
            m.league_id,
            l.name,
            l.country,
            l.logo
        ORDER BY
            m.season DESC,
            COUNT(*) DESC,
            m.league_id ASC
        """,
        (team_id, team_id),
    )

    if not rows:
        return None

    best_row = None
    best_key = None

    for r in rows:
        season = _coalesce_int(r.get("season"), 0)
        league_id = _coalesce_int(r.get("league_id"), 0)
        played = _coalesce_int(r.get("played"), 0)
        league_country = (r.get("league_country") or "").strip()

        domestic = 1 if team_country and league_country and (team_country == league_country) else 0

        sort_key = (
            season,
            domestic,
            played,
            -league_id,
        )
        if best_row is None or sort_key > best_key:
            best_row = r
            best_key = sort_key

    if not best_row:
        return None

    season = _coalesce_int(best_row.get("season"), 0) or None
    league_id = _coalesce_int(best_row.get("league_id"), 0) or None
    league_name = best_row.get("league_name") or ""
    league_country = best_row.get("league_country") or ""
    league_logo = best_row.get("league_logo")

    return {
        "team_id": int(team["id"]),
        "team_name": team.get("name") or "",
        "team_logo": team.get("logo"),
        "team_country": team.get("country") or "",
        "league_id": league_id,
        "league_name": league_name,
        "league_logo": league_logo,
        "league_country": league_country,
        "season": season,
    }


def _build_football_team_card(
    team_id: int,
    *,
    forced_league_id: Optional[int] = None,
    forced_season: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    entry = _resolve_football_team_entry(team_id)
    if not entry:
        return None

    league_id = forced_league_id if forced_league_id else entry.get("league_id")
    season = forced_season if forced_season else entry.get("season")

    if not league_id:
        return None

    if season is None:
        season = _resolve_football_league_season(int(league_id))

    subtitle_parts: List[str] = []
    if entry.get("team_country"):
        subtitle_parts.append(str(entry.get("team_country")))
    if entry.get("league_name"):
        subtitle_parts.append(str(entry.get("league_name")))

    return {
        "type": "team",
        "sport": "football",
        "team_id": int(entry["team_id"]),
        "league_id": int(league_id),
        "season": season,
        "name": entry.get("team_name") or "",
        "logo": entry.get("team_logo"),
        "country": entry.get("team_country") or "",
        "subtitle": " • ".join([x for x in subtitle_parts if x]),
    }


def _search_football_league_suggestions(q: str) -> List[Dict[str, Any]]:
    qq = (q or "").strip()
    if not qq:
        return []

    like_prefix = f"{qq}%"
    like_contains = f"%{qq}%"

    score_sql = _prefix_contains_order_sql("name")

    rows = fetch_all(
        f"""
        SELECT
            id,
            name,
            country,
            logo,
            {score_sql} AS score
        FROM leagues
        WHERE LOWER(name) LIKE LOWER(%s)
           OR LOWER(name) LIKE LOWER(%s)
        ORDER BY
            score DESC,
            LENGTH(name) ASC,
            name ASC
        LIMIT %s
        """,
        (qq, like_prefix, like_contains, like_prefix, like_contains, MAX_SUGGESTIONS),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "kind": "league",
                "sport": "football",
                "league_id": int(r["id"]),
                "label": r.get("name") or "",
                "sublabel": r.get("country") or "",
            }
        )
    return out


def _search_football_team_suggestions(q: str) -> List[Dict[str, Any]]:
    qq = (q or "").strip()
    if not qq:
        return []

    like_prefix = f"{qq}%"
    like_contains = f"%{qq}%"

    score_sql = _prefix_contains_order_sql("name")

    rows = fetch_all(
        f"""
        SELECT
            id,
            name,
            country,
            logo,
            {score_sql} AS score
        FROM teams
        WHERE LOWER(name) LIKE LOWER(%s)
           OR LOWER(name) LIKE LOWER(%s)
        ORDER BY
            score DESC,
            LENGTH(name) ASC,
            name ASC
        LIMIT %s
        """,
        (qq, like_prefix, like_contains, like_prefix, like_contains, MAX_SUGGESTIONS),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "kind": "team",
                "sport": "football",
                "team_id": int(r["id"]),
                "label": r.get("name") or "",
                "sublabel": r.get("country") or "",
            }
        )
    return out


def _football_expand_team_suggestions_from_leagues(league_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for item in league_items[:MAX_EXPANDED_LEAGUES]:
        league_id = _coalesce_int(item.get("league_id"), 0)
        if league_id <= 0:
            continue

        season = _resolve_football_league_season(league_id)
        if season is None:
            continue

        rows = fetch_all(
            """
            SELECT DISTINCT
                t.id,
                t.name,
                t.country
            FROM (
                SELECT home_id AS team_id
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                UNION
                SELECT away_id AS team_id
                FROM matches
                WHERE league_id = %s
                  AND season = %s
            ) x
            JOIN teams t
              ON t.id = x.team_id
            ORDER BY t.name ASC
            """,
            (league_id, season, league_id, season),
        )

        league_label = item.get("label") or ""

        for r in rows:
            out.append(
                {
                    "kind": "team",
                    "sport": "football",
                    "team_id": int(r["id"]),
                    "label": r.get("name") or "",
                    "sublabel": league_label,
                }
            )

    return out


def _football_league_team_cards(league_id: int) -> List[Dict[str, Any]]:
    season = _resolve_football_league_season(league_id)
    if season is None:
        return []

    rows = fetch_all(
        """
        SELECT DISTINCT
            t.id
        FROM (
            SELECT home_id AS team_id
            FROM matches
            WHERE league_id = %s
              AND season = %s
            UNION
            SELECT away_id AS team_id
            FROM matches
            WHERE league_id = %s
              AND season = %s
        ) x
        JOIN teams t
          ON t.id = x.team_id
        ORDER BY t.id ASC
        """,
        (league_id, season, league_id, season),
    )

    cards: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _coalesce_int(r.get("id"), 0)
        if team_id <= 0:
            continue

        card = _build_football_team_card(
            team_id,
            forced_league_id=league_id,
            forced_season=season,
        )
        if card:
            cards.append(card)

    cards.sort(key=lambda x: str(x.get("name") or "").lower())
    return cards


# ─────────────────────────────────────────
# Hockey: season / cards / suggestions
# ─────────────────────────────────────────

def _resolve_hockey_league_season(league_id: int) -> Optional[int]:
    return hockey_resolve_season_for_league(league_id=league_id, season=None)


def _build_hockey_league_card(league_id: int) -> Optional[Dict[str, Any]]:
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

    season = _resolve_hockey_league_season(league_id)
    return {
        "type": "league",
        "sport": "hockey",
        "league_id": int(row["id"]),
        "season": season,
        "name": row.get("name") or "",
        "logo": row.get("logo"),
        "country": "",
        "subtitle": "",
    }


def _resolve_hockey_team_entry(team_id: int) -> Optional[Dict[str, Any]]:
    team = hockey_fetch_one(
        """
        SELECT id, name, logo
        FROM hockey_teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not team:
        return None

    rows = hockey_fetch_all(
        """
        SELECT
            g.season,
            g.league_id,
            l.name AS league_name,
            l.logo AS league_logo,
            COUNT(*) AS played
        FROM hockey_games g
        JOIN hockey_leagues l
          ON l.id = g.league_id
        WHERE (g.home_team_id = %s OR g.away_team_id = %s)
        GROUP BY
            g.season,
            g.league_id,
            l.name,
            l.logo
        ORDER BY
            g.season DESC,
            COUNT(*) DESC,
            g.league_id ASC
        """,
        (team_id, team_id),
    )

    if not rows:
        return None

    best_row = None
    best_key = None

    for r in rows:
        season = _coalesce_int(r.get("season"), 0)
        league_id = _coalesce_int(r.get("league_id"), 0)
        played = _coalesce_int(r.get("played"), 0)

        sort_key = (
            season,
            played,
            -league_id,
        )
        if best_row is None or sort_key > best_key:
            best_row = r
            best_key = sort_key

    if not best_row:
        return None

    return {
        "team_id": int(team["id"]),
        "team_name": team.get("name") or "",
        "team_logo": team.get("logo"),
        "league_id": _coalesce_int(best_row.get("league_id"), 0) or None,
        "league_name": best_row.get("league_name") or "",
        "league_logo": best_row.get("league_logo"),
        "season": _coalesce_int(best_row.get("season"), 0) or None,
    }


def _build_hockey_team_card(
    team_id: int,
    *,
    forced_league_id: Optional[int] = None,
    forced_season: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    entry = _resolve_hockey_team_entry(team_id)
    if not entry:
        return None

    league_id = forced_league_id if forced_league_id else entry.get("league_id")
    season = forced_season if forced_season else entry.get("season")

    if not league_id:
        return None

    if season is None:
        season = _resolve_hockey_league_season(int(league_id))

    subtitle_parts: List[str] = []
    if entry.get("league_name"):
        subtitle_parts.append(str(entry.get("league_name")))
    if season is not None:
        subtitle_parts.append(str(season))

    return {
        "type": "team",
        "sport": "hockey",
        "team_id": int(entry["team_id"]),
        "league_id": int(league_id),
        "season": season,
        "name": entry.get("team_name") or "",
        "logo": entry.get("team_logo"),
        "country": "",
        "subtitle": " • ".join([x for x in subtitle_parts if x]),
    }


def _search_hockey_league_suggestions(q: str) -> List[Dict[str, Any]]:
    qq = (q or "").strip()
    if not qq:
        return []

    like_prefix = f"{qq}%"
    like_contains = f"%{qq}%"

    score_sql = _prefix_contains_order_sql("name")

    rows = hockey_fetch_all(
        f"""
        SELECT
            id,
            name,
            logo,
            {score_sql} AS score
        FROM hockey_leagues
        WHERE LOWER(name) LIKE LOWER(%s)
           OR LOWER(name) LIKE LOWER(%s)
        ORDER BY
            score DESC,
            LENGTH(name) ASC,
            name ASC
        LIMIT %s
        """,
        (qq, like_prefix, like_contains, like_prefix, like_contains, MAX_SUGGESTIONS),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "kind": "league",
                "sport": "hockey",
                "league_id": int(r["id"]),
                "label": r.get("name") or "",
                "sublabel": "",
            }
        )
    return out


def _search_hockey_team_suggestions(q: str) -> List[Dict[str, Any]]:
    qq = (q or "").strip()
    if not qq:
        return []

    like_prefix = f"{qq}%"
    like_contains = f"%{qq}%"

    score_sql = _prefix_contains_order_sql("name")

    rows = hockey_fetch_all(
        f"""
        SELECT
            id,
            name,
            logo,
            {score_sql} AS score
        FROM hockey_teams
        WHERE LOWER(name) LIKE LOWER(%s)
           OR LOWER(name) LIKE LOWER(%s)
        ORDER BY
            score DESC,
            LENGTH(name) ASC,
            name ASC
        LIMIT %s
        """,
        (qq, like_prefix, like_contains, like_prefix, like_contains, MAX_SUGGESTIONS),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "kind": "team",
                "sport": "hockey",
                "team_id": int(r["id"]),
                "label": r.get("name") or "",
                "sublabel": "",
            }
        )
    return out


def _hockey_expand_team_suggestions_from_leagues(league_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for item in league_items[:MAX_EXPANDED_LEAGUES]:
        league_id = _coalesce_int(item.get("league_id"), 0)
        if league_id <= 0:
            continue

        season = _resolve_hockey_league_season(league_id)
        if season is None:
            continue

        rows = hockey_fetch_all(
            """
            SELECT DISTINCT
                t.id,
                t.name
            FROM (
                SELECT home_team_id AS team_id
                FROM hockey_games
                WHERE league_id = %s
                  AND season = %s
                UNION
                SELECT away_team_id AS team_id
                FROM hockey_games
                WHERE league_id = %s
                  AND season = %s
            ) x
            JOIN hockey_teams t
              ON t.id = x.team_id
            ORDER BY t.name ASC
            """,
            (league_id, season, league_id, season),
        )

        league_label = item.get("label") or ""

        for r in rows:
            out.append(
                {
                    "kind": "team",
                    "sport": "hockey",
                    "team_id": int(r["id"]),
                    "label": r.get("name") or "",
                    "sublabel": league_label,
                }
            )

    return out


def _hockey_league_team_cards(league_id: int) -> List[Dict[str, Any]]:
    season = _resolve_hockey_league_season(league_id)
    if season is None:
        return []

    rows = hockey_fetch_all(
        """
        SELECT DISTINCT
            t.id
        FROM (
            SELECT home_team_id AS team_id
            FROM hockey_games
            WHERE league_id = %s
              AND season = %s
            UNION
            SELECT away_team_id AS team_id
            FROM hockey_games
            WHERE league_id = %s
              AND season = %s
        ) x
        JOIN hockey_teams t
          ON t.id = x.team_id
        ORDER BY t.id ASC
        """,
        (league_id, season, league_id, season),
    )

    cards: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _coalesce_int(r.get("id"), 0)
        if team_id <= 0:
            continue

        card = _build_hockey_team_card(
            team_id,
            forced_league_id=league_id,
            forced_season=season,
        )
        if card:
            cards.append(card)

    cards.sort(key=lambda x: str(x.get("name") or "").lower())
    return cards


# ─────────────────────────────────────────
# Public services
# ─────────────────────────────────────────

def search_suggestions(q: str, sport: str) -> Dict[str, Any]:
    qq = (q or "").strip()
    ss = _norm_sport(sport)

    if not qq:
        return {
            "query": "",
            "sport": ss,
            "items": [],
        }

    items: List[Dict[str, Any]] = []

    if ss in ("all", "football"):
        football_leagues = _search_football_league_suggestions(qq)
        football_teams = _search_football_team_suggestions(qq)
        football_expanded_teams = _football_expand_team_suggestions_from_leagues(football_leagues)

        items.extend(football_leagues)
        items.extend(football_teams)
        items.extend(football_expanded_teams)

    if ss in ("all", "hockey"):
        hockey_leagues = _search_hockey_league_suggestions(qq)
        hockey_teams = _search_hockey_team_suggestions(qq)
        hockey_expanded_teams = _hockey_expand_team_suggestions_from_leagues(hockey_leagues)

        items.extend(hockey_leagues)
        items.extend(hockey_teams)
        items.extend(hockey_expanded_teams)

    items = _dedupe_items(items)

    return {
        "query": qq,
        "sport": ss,
        "items": items,
    }


def search_selection_result(
    *,
    kind: str,
    sport: str,
    league_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    kk = (kind or "").strip().lower()
    ss = (sport or "").strip().lower()

    if ss == "football":
        if kk == "league":
            if not league_id:
                raise ValueError("league_id is required")

            league_card = _build_football_league_card(int(league_id))
            if not league_card:
                return {
                    "selected": {
                        "kind": "league",
                        "sport": "football",
                        "league_id": league_id,
                    },
                    "league_card": None,
                    "team_cards": [],
                }

            team_cards = _football_league_team_cards(int(league_id))
            return {
                "selected": {
                    "kind": "league",
                    "sport": "football",
                    "league_id": int(league_id),
                },
                "league_card": league_card,
                "team_cards": team_cards,
            }

        if kk == "team":
            if not team_id:
                raise ValueError("team_id is required")

            team_card = _build_football_team_card(int(team_id))
            return {
                "selected": {
                    "kind": "team",
                    "sport": "football",
                    "team_id": int(team_id),
                },
                "team_cards": [team_card] if team_card else [],
            }

    if ss == "hockey":
        if kk == "league":
            if not league_id:
                raise ValueError("league_id is required")

            league_card = _build_hockey_league_card(int(league_id))
            if not league_card:
                return {
                    "selected": {
                        "kind": "league",
                        "sport": "hockey",
                        "league_id": league_id,
                    },
                    "league_card": None,
                    "team_cards": [],
                }

            team_cards = _hockey_league_team_cards(int(league_id))
            return {
                "selected": {
                    "kind": "league",
                    "sport": "hockey",
                    "league_id": int(league_id),
                },
                "league_card": league_card,
                "team_cards": team_cards,
            }

        if kk == "team":
            if not team_id:
                raise ValueError("team_id is required")

            team_card = _build_hockey_team_card(int(team_id))
            return {
                "selected": {
                    "kind": "team",
                    "sport": "hockey",
                    "team_id": int(team_id),
                },
                "team_cards": [team_card] if team_card else [],
            }

    raise ValueError("unsupported selection")
