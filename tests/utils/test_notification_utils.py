import unittest
from unittest.mock import patch

from data_juicer.utils.notification_utils import (
    TEST_CARD_NOTIFICATION_ENDPOINT,
    send_test_card_notification,
)


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


class TestCardNotificationTest(unittest.TestCase):

    @patch("data_juicer.utils.notification_utils.HttpClient")
    def test_sends_test_card_with_user_and_ppe_headers_from_ctx(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client

        result = send_test_card_notification(
            template_id="AAqt1lQ72dVxK",
            template_variable={
                "operator": "ad_ai_data_center_http_mapper",
                "stage": "开始",
                "content": "{}",
                "errMsg": "",
            },
            ctx={
                "userAccount": "wangjianda.667",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
        )

        mock_client_cls.assert_called_once_with(
            endpoint=TEST_CARD_NOTIFICATION_ENDPOINT,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
        )
        self.assertEqual(
            fake_client.requests,
            [{
                "json_body": {
                    "userEmailOrAccount": "wangjianda.667",
                    "templateId": "AAqt1lQ72dVxK",
                    "templateVariable": {
                        "operator": "ad_ai_data_center_http_mapper",
                        "stage": "开始",
                        "content": "{}",
                        "errMsg": "",
                        "user": "wangjianda.667",
                    },
                }
            }],
        )
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["status_code"], 200)

    def test_rejects_missing_user_account_in_ctx(self):
        with self.assertRaisesRegex(ValueError, "ctx.userAccount must be provided"):
            send_test_card_notification(
                template_id="AAqt1lQ72dVxK",
                template_variable={},
                ctx={},
            )


if __name__ == "__main__":
    unittest.main()
