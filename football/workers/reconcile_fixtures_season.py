# reconcile_fixtures_season.py
#
# 목적:
# - LIVE_LEAGUES에 포함된 리그들에 대해 league+season 전체 fixtures를 API에서 다시 조회하고
#   DB(matches/fixtures + A그룹 테이블)의 "유령 fixture"를 정리하는 리컨실 워커.
#
# 안전장치:
# - /fixtures (league+season)는 페이지네이션이 있을 수 있으므로 paging.total 만큼 전부 수집.
# - API 수집 결과가 비정상적으로 작으면(페이징/요청 실패/차단 등) 삭제 단계는 스킵(대량삭제 방지).
#
# 주의:
# - 이 스크립트는 "DB 스키마 변경" 없이 동작하도록 작성됨.

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import fetch_all, execute

BASE_URL = "https://v3.football.api-sports.io/fixtures"


# ─────────────────────────────────────
#  ENV / 유틸
# ─────────────────────────────────────


def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_KEY)")
    return key


def _get_headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def parse_live_leagues(s: str) -> List[int]:
    out: List[int] = []
    for tok in (s or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            print(f"[WARN] LIVE_LEAGUES token invalid: {tok!r}", file=sys.stderr)
    return sorted(set(out))


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
#  DB 조회/삭제
# ─────────────────────────────────────

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


def delete_fixture_everywhere(fixture_id: int) -> None:
    print(f"    [DEL] fixture_id={fixture_id} → 관련 테이블에서 삭제")

    # A그룹 상세 테이블
    execute("DELETE FROM match_events       WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_events_raw   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_lineups      WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_team_stats   WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM match_player_stats WHERE fixture_id = %s", (fixture_id,))

    # raw(/fixtures 원본)도 같이 제거 (스키마 존재 확인됨)
    execute("DELETE FROM match_fixtures_raw WHERE fixture_id = %s", (fixture_id,))

    # 메인 테이블
    execute("DELETE FROM fixtures           WHERE fixture_id = %s", (fixture_id,))
    execute("DELETE FROM matches            WHERE fixture_id = %s", (fixture_id,))


# ─────────────────────────────────────
#  Api-Football /fixtures (league+season 전체, paging 지원)
# ─────────────────────────────────────

def _safe_get(url: str, *, params: Dict[str, Any], timeout: int = 25, max_retry: int = 4) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = requests.get(url, headers=_get_headers(), params=params, timeout=timeout)
            # 429/5xx는 재시도
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.7 * (i + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")
            return data
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
            continue
    raise RuntimeError(f"API request failed after retries: {last_err}")


def fetch_league_season_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    /fixtures?league=XXX&season=YYYY 는 페이지네이션이 있을 수 있음.
    paging.total만큼 전부 가져온다.
    """
    fixtures: List[Dict[str, Any]] = []

    # 1) 첫 페이지로 total 페이지 수를 확인
    params0: Dict[str, Any] = {"league": league_id, "season": season, "page": 1}
    data0 = _safe_get(BASE_URL, params=params0, timeout=25)

    results0 = int(data0.get("results", 0) or 0)
    if results0 == 0:
        errors = data0.get("errors")
        print(f"[WARN] league={league_id}, season={season} → results=0, errors={errors}")
        return []

    paging = data0.get("paging") or {}
    total_pages = int(paging.get("total", 1) or 1)

    rows0 = data0.get("response", []) or []
    for item in rows0:
        if isinstance(item, dict):
            fixtures.append(item)

    # 2) 나머지 페이지 수집
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            params = {"league": league_id, "season": season, "page": page}
            data = _safe_get(BASE_URL, params=params, timeout=25)
            rows = data.get("response", []) or []
            for item in rows:
                if isinstance(item, dict):
                    fixtures.append(item)

    return fixtures


# ─────────────────────────────────────
#  UPSERT (matches / fixtures) - 스키마 그대로
# ─────────────────────────────────────

def _status_group_from_short(short: Optional[str]) -> str:
    s = (short or "").upper()
    if s in ("FT", "AET", "PEN"):
        return "FINISHED"
    if s in ("NS", "TBD"):
        return "SCHEDULED"
    if s in ("PST", "CANC", "ABD", "AWD", "WO"):
        return "CANCELLED"
    # LIVE/HT/ET/BT 등
    return "LIVE"


def upsert_fixture_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    status_short = (fixture_block.get("status") or {}).get("short")
    status_group = _status_group_from_short(status_short)

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        """,
        (int(fid), int(league_id), int(season), date_utc, status_short, status_group),
    )


def upsert_match_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    teams_block = fx.get("teams") or {}
    goals_block = fx.get("goals") or {}
    score_block = fx.get("score") or {}

    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    st = fixture_block.get("status") or {}
    status_short = st.get("short")
    status_long = st.get("long")
    status_elapsed = st.get("elapsed")
    status_extra = st.get("extra")
    status_group = _status_group_from_short(status_short)

    home_id = (teams_block.get("home") or {}).get("id")
    away_id = (teams_block.get("away") or {}).get("id")
    if home_id is None or away_id is None:
        return

    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    ht = score_block.get("halftime") or {}
    home_ht = ht.get("home")
    away_ht = ht.get("away")

    elapsed = status_elapsed

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")
    venue = fixture_block.get("venue") or {}
    venue_id = venue.get("id")
    venue_name = venue.get("name")
    venue_city = venue.get("city")

    league_round = (fx.get("league") or {}).get("round")

    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc, status, status_group,
            home_id, away_id,
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round
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
            int(fid), int(league_id), int(season), date_utc, status_short, status_group,
            int(home_id), int(away_id),
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round,
        ),
    )


# ─────────────────────────────────────
#  리컨실 메인
# ─────────────────────────────────────

def reconcile_league_season(league_id: int, season: int) -> None:
    print(f"[RUN] league_id={league_id}, season={season} 리컨실 시작")

    api_fixtures = fetch_league_season_from_api(league_id, season)
    if not api_fixtures:
        print(f"[INFO] league={league_id}, season={season} → API 쪽 경기 없음 (건너뜀)")
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

    # ✅ 대량 삭제 방지 가드:
    # DB가 어느 정도 큰데 API가 너무 작으면(페이징/차단/오류 의심) 삭제를 하지 않는다.
    if len(db_ids) >= 50:
        ratio = (len(api_ids) / max(1, len(db_ids)))
        if ratio < 0.60:
            print(
                f"[SAFEGUARD] league={league_id}, season={season} "
                f"API/DB ratio too low: {len(api_ids)}/{len(db_ids)}={ratio:.2f} → 삭제 단계 스킵",
                file=sys.stderr,
            )
            only_db = set()  # 삭제 금지

    # 1) API에만 있는 fixture → 신규/복구 → UPSERT
    for fid in sorted(only_api):
        fx = api_by_id[fid]
        print(f"    [UPSERT new] fixture_id={fid}")
        upsert_match_row(fx, league_id=league_id, season=season)
        upsert_fixture_row(fx, league_id=league_id, season=season)

    # 2) 공통 fixture → UPSERT
    for fid in sorted(common):
        fx = api_by_id[fid]
        # 너무 시끄러우면 로그 줄이고 싶으면 여기 print 지워도 됨
        # print(f"    [UPSERT sync] fixture_id={fid}")
        upsert_match_row(fx, league_id=league_id, season=season)
        upsert_fixture_row(fx, league_id=league_id, season=season)

    # 3) DB에만 있는 fixture → 유령 경기 → 삭제
    for fid in sorted(only_db):
        delete_fixture_everywhere(fid)

    print(f"[DONE] league_id={league_id}, season={season} 리컨실 완료")


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
        print("[ERROR] LIVE_LEAGUES가 비어있음", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] leagues={league_ids}, seasons={seasons}")
    for season in seasons:
        for lid in league_ids:
            reconcile_league_season(league_id=lid, season=season)


if __name__ == "__main__":
    main()
