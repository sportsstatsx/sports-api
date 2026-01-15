# tools/football_backfill

축구(football) 데이터 **수동 백필/복구용 도구 모음**입니다.  
⚠️ Render Cron(주기 작업)에서 자동으로 돌지 않습니다. 필요할 때만 수동 실행하세요.

## 포함 도구 (역할 한 줄 요약)
- `backfill_match_fixtures_raw_by_ids.py`  
  → **fixture_id 목록(ids.txt)** 만 골라서 API에서 `/fixtures`를 재수집하고, DB의 `match_fixtures_raw` + `matches`를 **업서트(갱신)** 합니다.

- `backfill_team_season_stats.py`  
  → DB의 `(league_id, season)` 조합을 기준으로 `team_season_stats`를 **일괄 생성/재계산** 합니다.

## 실행 예시
```bash
# 1) 특정 경기들만 fixtures raw/meta 복구
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt 120   # 120번째부터 재개

# 2) 팀 시즌 통계 재계산(전체)
python tools/football_backfill/backfill_team_season_stats.py

# 2-1) 특정 시즌만
python tools/football_backfill/backfill_team_season_stats.py 2024
python tools/football_backfill/backfill_team_season_stats.py 2024,2025
```

## 공통 주의
- **DB에 즉시 반영**됩니다. 반드시 dev에서 먼저 검증 후 prod에 적용하세요.
- API 키/DB 연결 환경변수는 운영 워커와 동일하게 세팅되어 있어야 합니다.
