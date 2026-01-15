# backfill_match_fixtures_raw_by_ids.py

## 이 파일이 하는 일 (정확히)
**fixture_id 목록(ids.txt)** 을 받아서, 각 id에 대해 API-Football의 **`/fixtures?id=...`** 를 다시 호출합니다.  
그리고 결과를 DB에 다음처럼 **업서트(갱신)** 합니다.

- `match_fixtures_raw` : fixtures 원본(raw) JSON 저장/갱신
- `matches` : 해당 fixture의 메타(상태/시간/스코어 등) 갱신

➡️ 한마디로: **“이 경기들만 fixtures raw/meta를 강제로 다시 맞추는 타겟 복구 도구”**

## 언제 쓰나
- 특정 몇 경기만 `fixtures_raw`/`matches`가 누락/오염/오래된 값으로 남아있는 경우
- API 쪽에서 경기 정보가 정정되어 “그 경기만” 다시 받아야 하는 경우
- postmatch 백필 전에 fixture 메타부터 먼저 정상화하고 싶은 경우

## 실행 명령어 예시
```bash
# ids.txt: 한 줄에 하나의 fixture_id
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt

# 중간부터 재개(옵션): start_idx부터 다시 시작
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt 120
```

## 입력 파일 형식 (ids.txt)
- 한 줄에 하나의 숫자 fixture_id
- 빈 줄은 자동 무시

예:
```txt
14918006
14918005
14917999
```

## 필요 환경
- `APIFOOTBALL_KEY` : API-Football 키 (없으면 즉시 에러)
- DB 연결 환경변수: 프로젝트에서 쓰는 `db.py` 설정(예: `DATABASE_URL` 등)

## 주의
- fixture 개수만큼 API 호출합니다. **대량 실행은 비추** (쿼터/속도/실행시간 이슈)
- 실행 결과는 DB에 바로 반영됩니다. dev에서 먼저 확인 후 prod 실행 권장
