# workers/football/postmatch_backfill.py

## 역할(한 줄)
**경기 종료(FT)된 경기**를 대상으로 `/events`, `/lineups`, `/statistics`, `/players` 등 **경기 상세 데이터를 DB에 채우는 postmatch 백필 워커**.

## 무엇을 “하는가”
- 대상 리그(설정된 LIVE_LEAGUES)에서 `target_date` 기준 **FINISHED(FT)** 경기만 가져옴
- 각 경기마다 아래 엔드포인트 데이터를 가져와 DB에 upsert
  - `/fixtures`(기본 fixture)
  - `/events`
  - `/lineups`
  - `/statistics`
  - `/players`

## 무엇을 “안 하는가”
- 특정 fixture_id만 찍어서 복구하는 용도 아님 → `tools/football_backfill/backfill_match_fixtures_raw_by_ids.py` 사용
- team_season_stats 재계산 용도 아님 → `tools/football_backfill/backfill_team_season_stats.py` 사용

## 중복 실행 방지(중요)
- 이미 DB에 `match_events`가 존재하면 **“이미 백필된 경기”로 보고 스킵**(중복 백필 방지)

## Render Cron(Command) 예시
이 파일을 `workers/football/`로 옮겼다면, Render Cron의 Command를 아래처럼 바꾸면 됨:

```bash
python workers/football/postmatch_backfill.py
```

> 참고: Render가 레포 루트에서 명령을 실행해야 `from db import ...` 같은 루트 모듈 import가 안전합니다.
