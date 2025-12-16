# hockey/workers/hockey_match_event_worker.py
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from db import fetch_all  # 축구 DB (DATABASE_URL)
from hockey.hockey_db import hockey_fetch_one, hockey_fetch_all  # 하키 DB (HOCKEY_DATABASE_URL)
from notifications.fcm_client import FCMClient
from hockey.workers.hockey_live_common import notify_interval_sec, hockey_live_leagues

log = logging.getLogger("hockey_match_event_worker")
logging.basicConfig(level=logging.INFO)


@dataclass
class GameState:
    game_id: int
    status: str
    home_total: int
    away_total: int


def _safe_int(v: Any) -> int:
    try:
        if v is None or v == "":
            return 0
        return int(v)
    except Exception:
        return 0


def _extract_totals(score_json: Any) -> Tuple[int, int]:
    """
    hockey_games.score_json에서 total 스코어 추출 (방어적으로)
    기대 예: {"home":{"total":2}, "away":{"total":1}, ...}
    """
    if not isinstance(score_json, dict):
        return 0, 0

    home = score_json.get("home")
    away = score_json.get("away")

    home_total = 0
    away_total = 0

    if isinstance(home, dict):
        home_total = _safe_int(home.get("total") or home.get("goals") or home.get("score"))
    if isinstance(away, dict):
        away_total = _safe_int(away.get("total") or away.get("goals") or away.get("score"))

    return home_total, away_total


def _is_started(status: str) -> bool:
    s = (status or "").upper()
    return s not in ("NS", "TBD", "")


def _is_finished(status: str) -> bool:
    s = (status or "").upper()
    return s in ("FT", "AET", "PEN", "FIN", "ENDED", "END")


def get_subscribed_tokens(game_id: int) -> List[str]:
    """
    축구 DB: match_notification_subscriptions + user_devices
    match_id 컬럼에 hockey game_id를 그대로 넣어 사용(테스트용)
    """
    rows = fetch_all(
        """
        SELECT ud.fcm_token
        FROM match_notification_subscriptions s
        JOIN user_devices ud ON ud.device_id = s.device_id
        WHERE s.match_id = %s
          AND ud.notifications_enabled = TRUE
          AND ud.fcm_token IS NOT NULL
          AND ud.fcm_token != ''
        """,
        (game_id,),
    )
    return [str(r["fcm_token"]) for r in rows if r.get("fcm_token")]


def send_push(tokens: List[str], title: str, body: str, data: Optional[Dict[str, Any]] = None) -> None:
    if not tokens:
        return
    fcm = FCMClient()
    batch = 500
    for i in range(0, len(tokens), batch):
        fcm.send_to_tokens(tokens[i : i + batch], title, body, data=data or {})


def read_game_state(game_id: int) -> Optional[GameState]:
    row = hockey_fetch_one(
        """
        SELECT id, status, score_json
        FROM hockey_games
        WHERE id = %s
        """,
        (game_id,),
    )
    if not row:
        return None

    status = str(row.get("status") or "").strip()
    score_json = row.get("score_json")
    home_total, away_total = _extract_totals(score_json)

    return GameState(game_id=game_id, status=status, home_total=home_total, away_total=away_total)


def candidate_game_ids() -> List[int]:
    """
    하키 DB에서 최근 +-36시간 게임을 뽑아서 알림 대상으로 스캔
    (구독이 없는 게임은 토큰 조회에서 자연스럽게 걸러짐)
    """
    leagues = hockey_live_leagues()
    if not leagues:
        return []

    rows = hockey_fetch_all(
        """
        SELECT id
        FROM hockey_games
        WHERE league_id = ANY(%s)
          AND game_date >= NOW() - INTERVAL '36 hours'
          AND game_date <= NOW() + INTERVAL '36 hours'
        ORDER BY game_date ASC
        """,
        (leagues,),
    )
    return [int(r["id"]) for r in rows if r.get("id") is not None]


def main() -> None:
    interval = notify_interval_sec(6.0)
    log.info("🔔 hockey notify worker start: interval=%.1fs leagues=%s", interval, hockey_live_leagues())

    last: Dict[int, GameState] = {}

    while True:
        try:
            gids = candidate_game_ids()
            for gid in gids:
                cur = read_game_state(gid)
                if not cur:
                    continue

                prev = last.get(gid)

                if prev is None:
                    last[gid] = cur
                    continue

                # kickoff
                if (not _is_started(prev.status)) and _is_started(cur.status):
                    tokens = get_subscribed_tokens(gid)
                    send_push(
                        tokens,
                        title="Hockey 시작!",
                        body=f"경기가 시작했어요. (ID: {gid})",
                        data={"type": "hockey_kickoff", "game_id": str(gid)},
                    )

                # score change
                if (prev.home_total, prev.away_total) != (cur.home_total, cur.away_total):
                    tokens = get_subscribed_tokens(gid)
                    send_push(
                        tokens,
                        title="득점 업데이트",
                        body=f"스코어: {cur.home_total}-{cur.away_total} (ID: {gid})",
                        data={
                            "type": "hockey_score",
                            "game_id": str(gid),
                            "home_total": str(cur.home_total),
                            "away_total": str(cur.away_total),
                        },
                    )

                # finished
                if (not _is_finished(prev.status)) and _is_finished(cur.status):
                    tokens = get_subscribed_tokens(gid)
                    send_push(
                        tokens,
                        title="경기 종료",
                        body=f"최종 스코어: {cur.home_total}-{cur.away_total} (ID: {gid})",
                        data={
                            "type": "hockey_ft",
                            "game_id": str(gid),
                            "home_total": str(cur.home_total),
                            "away_total": str(cur.away_total),
                        },
                    )

                last[gid] = cur

        except Exception as e:
            log.exception("notify tick failed: %s", e)

        time.sleep(interval)


if __name__ == "__main__":
    main()
