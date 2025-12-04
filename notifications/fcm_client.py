# notifications/fcm_client.py

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import firebase_admin
from firebase_admin import credentials, messaging

SERVICE_ENV_VAR = "FIREBASE_SERVICE_ACCOUNT_JSON"


def _init_firebase_app() -> firebase_admin.App:
    """
    FIREBASE_SERVICE_ACCOUNT_JSON 환경변수에 들어있는
    서비스 계정 JSON으로 Firebase Admin SDK를 초기화한다.
    """
    # 이미 초기화되어 있으면 그 앱을 그대로 사용
    if firebase_admin._apps:
        # _apps 는 dict 이라 values()[0] 로 기본 앱 하나 꺼내면 됨
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
    Firebase Admin SDK 를 이용해서 FCM 알림을 보내는 클라이언트.

    - 더 이상 FCM_SERVER_KEY 는 사용하지 않는다.
    - FIREBASE_SERVICE_ACCOUNT_JSON 만 사용한다.
    """

    def __init__(self) -> None:
        _init_firebase_app()

    @staticmethod
    def _build_data(data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """
        data payload 는 모두 문자열이어야 해서 str() 로 캐스팅.
        """
        if not data:
            return {}
        return {str(k): str(v) for k, v in data.items()}

    def send_to_tokens(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        여러 FCM 토큰으로 알림 전송.
        firebase_admin 버전이 낮아서 send_multicast 를 쓰지 않고,
        messaging.send() 를 토큰마다 한 번씩 호출하는 방식으로 구현한다.
        """
        if not tokens:
            return {"success_count": 0, "failure_count": 0, "results": []}

        _init_firebase_app()
        payload_data = self._build_data(data)

        success_count = 0
        failure_count = 0
        results: List[Dict[str, Any]] = []

        for token in tokens:
            msg = messaging.Message(
                token=token,
                notification=messaging.Notification(title=title, body=body),
                data=payload_data,
            )

            try:
                message_id = messaging.send(msg)
                results.append(
                    {
                        "token": token,
                        "success": True,
                        "message_id": message_id,
                        "error": None,
                    }
                )
                success_count += 1
            except Exception as e:  # noqa: BLE001
                results.append(
                    {
                        "token": token,
                        "success": False,
                        "message_id": None,
                        "error": str(e),
                    }
                )
                failure_count += 1

        return {
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results,
        }

    def send_to_token(
        self,
        token: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        단일 토큰용 헬퍼. 기존 코드에서 필요하면 사용 가능.
        """
        return self.send_to_tokens([token], title, body, data)


if __name__ == "__main__":
    # Render Shell 에서 수동 테스트용
    import sys

    print("FCMClient self-test 시작")

    try:
        _init_firebase_app()
    except Exception as e:  # noqa: BLE001
        print("Firebase 초기화 실패:", e, file=sys.stderr)
        sys.exit(1)

    print("Firebase 초기화 성공 (실제 푸시는 여기서 보내지 않음)")
