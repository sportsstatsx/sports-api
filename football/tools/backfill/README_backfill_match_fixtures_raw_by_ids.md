# backfill_match_fixtures_raw_by_ids.py

## 역할(한 줄)
**fixture_id 목록(ids.txt)에 있는 경기만** `/fixtures?id=...`를 다시 받아 `fixtures_raw` 저장 + `matches`(기본 fixture) 갱신.

## 언제 쓰나
- 특정 경기 몇 개만 fixtures/raw가 누락/깨짐
- API에서 다시 받아 “해당 경기만” 빠르게 복구하고 싶을 때

## 실행 방법
ids.txt에 fixture_id를 한 줄에 하나씩 넣고 실행.

```bash
# 전체 실행
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt

# 일부만 실행(예: 100번째 줄부터)
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt 100
```

## 주의
- 이 스크립트는 **선택한 fixture_id만** 처리합니다(전체 시즌/전체 리그 백필 아님).
- Render Cron에 연결되어 있지 않은 **수동 도구**입니다.
