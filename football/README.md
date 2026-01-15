# football/

축구(football) 관련 코드를 한 곳에 모으기 위한 폴더입니다.  
(hockey/ 처럼 종목별로 구분)

## 포함
- `football/workers/`: Render Cron/Worker로 도는 실행 스크립트
- `football/tools/`: 필요할 때 수동으로 실행하는 유틸 스크립트

## 권장 실행 방식(패키지 모드)
폴더에 `__init__.py`가 포함되어 있으므로, 아래처럼 `-m` 실행을 권장합니다(경로/Import 안정적).

- Postmatch 백필(크론):
  - `python -m football.workers.postmatch_backfill`

- 특정 fixture_id만 fixtures/raw 복구(수동):
  - `python -m football.tools.backfill.backfill_match_fixtures_raw_by_ids ids.txt`

- team_season_stats 재계산(수동):
  - `python -m football.tools.backfill.backfill_team_season_stats`
