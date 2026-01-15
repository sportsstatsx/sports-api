"""
reconcile_fixtures_season.py (단독 실행 버전)

Api-Football /fixtures (league + season 전체)와
우리 DB(matches, fixtures)를 비교해서:

  - API에는 있는데 DB에 없는 fixture_id  → INSERT/UPSERT
  - 둘 다 있는데 date/status 등이 달라진 fixture_id → UPSERT(갱신)
  - DB에는 있는데 API에 없는 fixture_id → 유령 경기로 보고 삭제

사용 예시:
  python reconcile_fixtures_season.py
  python reconcile_fixtures_season.py 2025
  python reconcile_fixtures_season.py 2024 2025
  python reconcile_fixtures_season.py 2024,2025

환경변수:
  - APIFOOTBALL_KEY (또는 API_FOOTBALL_KEY / API_KEY)
  - LIVE_LEAGUES="39,140,135"
"""

import os
import sys
import datetime as dt
from typing import Any, Dict, List, Optional

import requests

from db import fetch_all, execute


BASE_URL = "https://v3.football.api-sports.io/fixtures"


def _api_key() -> str:
    return (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )


def _get_headers() -> Dict[str, str]:
    key = _api_key()
    if not key:
        raise RuntimeError("APIFOOTBALL_KEY(또는 API_FOOTBALL_KEY/API_KEY) 환경변수가 비어있습니다.")
    return {"x-apisports-key": key}


def parse_live_leagues(env_value: str) -> List[int]:
    tokens: List[str] = []
    for part in (env_value or "").replace("\n", ",").replace(" ", ",").split(","):
        p = part.strip()
        if p:
            tokens.append(p)

    out: List[int] = []
    for t in tokens:
        try:
            out.append(int(t))
        except ValueError:
            continue
    return sorted(set(out))


def _status_group(status_short: str, status_long: str = "") -> str:
    s = (status_short or "").upper()
    l = (status_long or "").lower()

    if s in ("FT", "AET", "PEN"):
        return "FINISHED"
    if s in ("NS", "TBD"):
        return "UPCOMING"
    if s in ("1H", "HT", "2H", "ET", "BT", "P", "LIVE"):
        return "INPLAY"
    if "finished" in l:
        return "FINISHED"
    if "not started" in l:
        return "UPCOMING"
    if "in play" in l or "live" in l:
        return "INPLAY"
    return "UNKNOWN"


def _extract_basic_for_upsert(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}
    status_block = fixture_block.get("status") or {}

    fid = fixture_block.get("id")
    if fid is None:
        return None

    league_id = league_block.get("id")
    season = league_block.get("season")
    date_utc = fixture_block.get("date")

    status_short = status_block.get("short") or ""
    status_long = status_block.get("long") or ""
    elapsed = status_block.get("elapsed")
    extra = status_block.get("extra")

    return {
        "fixture_id": int(fid),
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "date_utc": date_utc,
        "status_short": status_short,
        "status_long": status_long,
        "status_group": _status_group(status_short, status_long),
        "elapsed": elapsed,
        "status_elapsed": elapsed,
        "status_extra": extra,
    }


def upsert_fixture_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    b = _extract_basic_for_upsert(fx)
    if b is None:
        return
    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            date_utc     = EXCLUDED.date_utc,
            status       = EXCLUDED.status,
            status_group = EXCLUDED.status_group
        """,
        (
            b["fixture_id"],
            league_id,
            season,
            b.get("date_utc"),
            b.get("status_short") or "",
            b.get("status_group") or "UNKNOWN",
        ),
    )


def upsert_match_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    b = _extract_basic_for_upsert(fx)
    if b is None:
        return

    fixture_id = b["fixture_id"]
    date_utc = b.get("date_utc")
    status_short = b.get("status_short") or ""
    status_group = b.get("status_group") or "UNKNOWN"
    elapsed = b.get("elapsed")

    teams_block = fx.get("teams") or {}
    home_team = teams_block.get("home") or {}
    away_team = teams_block.get("away") or {}
    home_id = home_team.get("id")
    away_id = away_team.get("id")
    if home_id is None or away_id is None:
        return

    goals_block = fx.get("goals") or {}
    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}
    status_block = fixture_block.get("status") or {}
    venue_block = fixture_block.get("venue") or {}
    score_block = fx.get("score") or {}
    ht_block = score_block.get("halftime") or {}

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")

    status_long = status_block.get("long")
    status_elapsed = status_block.get("elapsed")
    status_extra = status_block.get("extra")

    home_ht = ht_block.get("home")
    away_ht = ht_block.get("away")

    venue_id = venue_block.get("id")
    venue_name = venue_block.get("name")
    venue_city = venue_block.get("city")

    league_round = league_block.get("round")

    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short,
            status_long,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season = EXCLUDED.season,
            date_utc = EXCLUDED.date_utc,
            status = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id = EXCLUDED.home_id,
            away_id = EXCLUDED.away_id,
            home_ft = EXCLUDED.home_ft,
            away_ft = EXCLUDED.away_ft,
            elapsed = EXCLUDED.elapsed,
            home_ht = EXCLUDED.home_ht,
            away_ht = EXCLUDED.away_ht,
            referee = EXCLUDED.referee,
            fixture_timezone = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,
            status_short = EXCLUDED.status_short,
            status_long = EXCLUDED.status_long,
            status_elapsed = EXCLUDED.status_elapsed,
            status_extra = EXCLUDED.status_extra,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_city = EXCLUDED.venue_city,
            league_round = EXCLUDED.league_round
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            int(home_id),
            int(away_id),
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short,
            status_long,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round,
        ),
    )


# ─────────────────────────────────────
#  CLI 유틸
# ─────────────────────────────────────

def parse_seasons_from_argv(argv: List[str]) -> List[int]:
    season_tokens: List[str] = []
    for arg in argv:
        for token in arg.split(","):
            token = token.strip()
            if token:
                season_tokens.append(token)

    seasons: List[int] = []
    for t in season_tokens:
        try:
            seasons.append(int(t))
        except ValueError:
            print(f"[WARN] 시즌 값으로 해석 불가: {t!r} → 무시", file=sys.stderr)

    return sorted(set(seasons))


def load_latest_season_from_db() -> Optional[int]:
    rows = fetch_all(
        """
        SELECT MAX(season) AS max_season
        FROM matches
        WHERE season IS NOT NULL
        """,
        (),
    )
    if not rows:
        return None

    max_s = rows[0].get("max_season")
    if max_s is None:
        return None

    try:
        return int(max_s)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────
#  삭제 유틸 (유령 경기 정리)
# ─────────────────────────────────────

def delete_fixture_everywhere(fixture_id: int) -> None:
    # 기존 파일도 동일하게 여기 테이블들을 삭제함 :contentReference[oaicite:4]{index=4}
    execute("DELETE FROM match_events       WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_events_raw   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_lineups      WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_team_stats   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_player_stats WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM fixtures           WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM matches            WHERE fixture_id = %s", (fixture_id,))


# ─────────────────────────────────────
#  Api-Football /fixtures (league+season 전체)
# ─────────────────────────────────────

def fetch_league_season_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    params = {"league": league_id, "season": season}
    resp = requests.get(BASE_URL, headers=_get_headers(), params=params, timeout=25)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", 0) or 0
    if results == 0:
        errors = data.get("errors")
        print(f"[WARN] league={league_id}, season={season} → results=0, errors={errors}")
        return []

    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def load_db_fixtures(league_id: int, season: int) -> Dict[int, Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT fixture_id, date_utc, status, status_group
        FROM matches
        WHERE league_id = %s
          AND season     = %s
        """,
        (league_id, season),
    )

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        fid = r.get("fixture_id")
        if fid is None:
            continue
        out[int(fid)] = {
            "fixture_id": int(fid),
            "date_utc": r.get("date_utc"),
            "status": r.get("status"),
            "status_group": r.get("status_group"),
        }
    return out


def reconcile_league_season(league_id: int, season: int) -> None:
    print(f"[RUN] league_id={league_id}, season={season}")

    api_fixtures = fetch_league_season_from_api(league_id, season)
    if not api_fixtures:
        print(f"[INFO] league={league_id}, season={season} → API 경기 없음 (건너뜀)")
        return

    db_fixtures = load_db_fixtures(league_id, season)

    api_by_id: Dict[int, Dict[str, Any]] = {}
    for f in api_fixtures:
        fid = (f.get("fixture") or {}).get("id")
        if fid is None:
            continue
        api_by_id[int(fid)] = f

    api_ids = set(api_by_id.keys())
    db_ids = set(db_fixtures.keys())

    only_api = api_ids - db_ids
    common = api_ids & db_ids
    only_db = db_ids - api_ids

    print(f"    API={len(api_ids)}, DB={len(db_ids)}, only_api={len(only_api)}, common={len(common)}, only_db={len(only_db)}")

    for fid in sorted(only_api):
        fx = api_by_id[fid]
        upsert_match_row(fx, league_id=league_id, season=season)
        upsert_fixture_row(fx, league_id=league_id, season=season)

    for fid in sorted(common):
        fx = api_by_id[fid]
        upsert_match_row(fx, league_id=league_id, season=season)
        upsert_fixture_row(fx, league_id=league_id, season=season)

    for fid in sorted(only_db):
        delete_fixture_everywhere(fid)

    print(f"[DONE] league_id={league_id}, season={season}")


def main() -> None:
    seasons = parse_seasons_from_argv(sys.argv[1:])
    if not seasons:
        latest = load_latest_season_from_db()
        if latest is None:
            print("[ERROR] matches에서 season MAX를 찾지 못함. 인자로 시즌을 넘겨줘.", file=sys.stderr)
            sys.exit(1)
        seasons = [latest]
        print(f"[INFO] 인자 없음 → 최신 시즌({latest})만 수행")

    league_ids = parse_live_leagues(os.environ.get("LIVE_LEAGUES", ""))
    if not league_ids:
        print("[ERROR] LIVE_LEAGUES가 비어있어서 리컨실 대상 리그를 알 수 없음.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] leagues={league_ids}, seasons={seasons}")

    for season in seasons:
        for lid in league_ids:
            reconcile_league_season(league_id=lid, season=season)


if __name__ == "__main__":
    main()
