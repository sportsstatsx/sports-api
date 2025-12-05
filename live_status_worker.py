# live_status_worker.py (루트 경로)

import time
import traceback

from update_live_fixtures import main as run_update_live


INTERVAL_SEC = 10  # 10초마다 라이브 상태 업데이트


def loop():
    print(f"[live_status_worker] 시작 (interval={INTERVAL_SEC} sec)")
    while True:
        try:
            run_update_live()   # update_live_fixtures.py 1회 실행
        except Exception:
            traceback.print_exc()

        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    loop()
