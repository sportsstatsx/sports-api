# notifications/fcm_client.py

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import firebase_admin
from firebase_admin import credentials, messaging

SERVICE_ENV_VAR = "FIREBASE_SERVICE_ACCOUNT_JSON"


def _init_firebase_app() -> firebase_admin.App:
    """
    FIREBASE_SERVICE_ACCOUNT_JSON 환경변수에 들어있는
    서비스 계정 JSON을 사용해서 Firebase Admin SDK를 초기화한다.
    """
    # 이미 초기화되어 있으면 그대로 사용
    if firebase_admin._apps:
        return list(firebase_admin._apps.values())[0]

    raw = os.environ.get(SERVICE_ENV_VAR)
    if not raw:
        raise RuntimeError(
            f"{SERVICE_ENV_VAR} 환경변수가 설정되어 있지 않습니다. "
            "Render 서비스 환경변수에 서비스 계정 JSON 전체를 넣어 주세요."
        )

    try:
        service_account_info = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"{SERVICE_ENV_VAR} 환경변수 내용이 올바른 JSON 형식이 아닙니다."
        ) from e

    cred = credentials.Certificate(service_account_info)
    return firebase_admin.initialize_app(cred)


class FCMClient:
    """
    Firebase Admin SDK를 이용해서 FCM 알림을 보내는 클라이언트.

    ※ 더 이상 FCM_SERVER_KEY는 사용하지 않는다.
    """

    def __init__(self) -> None:
        # 앱이 한 번만 초기화되도록 내부 함수 호출
        _init_firebase_app()

    def send_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        여러 FCM 토큰으로 멀티캐스트 알림 전송.
        """
        if not tokens:
            return {"success_count": 0, "failure_count": 0}

        # data 값은 모두 문자열이어야 해서 str()로 한 번 감싸준다.
        data_str: Dict[str, str] | None = None
        if data:
            data_str = {k: str(v) for k, v in data.items()}

        message = messaging.MulticastMessage(
            tokens=tokens,
            notification=messaging.Notification(title=title, body=body),
            data=data_str,
        )

        response = messaging.send_multicast(message)

        return {
            "success_count": response.success_count,
            "failure_count": response.failure_count,
        }


if __name__ == "__main__":
    # 간단한 수동 테스트용 (Render Shell에서 직접 돌릴 때 사용)
    import sys

    print("FCMClient self-test 시작")

    try:
        _init_firebase_app()
    except Exception as e:  # noqa: BLE001
        print("Firebase 초기화 실패:", e, file=sys.stderr)
        sys.exit(1)

    print("Firebase 초기화 성공 (실제 푸시는 여기서 보내지 않음)")
