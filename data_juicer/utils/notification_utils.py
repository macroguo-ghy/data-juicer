from __future__ import annotations

from typing import Any

from data_juicer.utils.http_utils import HttpClient

TEST_CARD_NOTIFICATION_ENDPOINT = (
    "https://ai-data-center.bytedance.net/api/openapi/lark/message/template-card/send-to-user"
)
TEST_CARD_NOTIFICATION_HEADERS = {
    "Content-Type": "application/json",
    "x-tt-env": "ppe_sirius2",
    "x-use-ppe": "1",
}


def send_test_card_notification(
    template_id: str,
    template_variable: dict[str, Any],
    user_email_or_account: str,
) -> dict[str, Any]:
    """Send a test template-card notification."""
    if not template_id:
        raise ValueError("template_id must be provided")
    if not user_email_or_account:
        raise ValueError("user_email_or_account must be provided")

    client = HttpClient(
        endpoint=TEST_CARD_NOTIFICATION_ENDPOINT,
        method="POST",
        headers=TEST_CARD_NOTIFICATION_HEADERS,
        timeout=30.0,
    )
    return client.request(
        json_body={
            "userEmailOrAccount": user_email_or_account,
            "templateId": template_id,
            "templateVariable": template_variable,
        }
    )
