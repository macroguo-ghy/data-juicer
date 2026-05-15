from __future__ import annotations

from typing import Any

from data_juicer.utils.http_utils import HttpClient

TEST_CARD_NOTIFICATION_ENDPOINT = (
    "https://ai-data-center.bytedance.net/api/openapi/lark/message/template-card/send-to-user"
)
TEST_CARD_NOTIFICATION_HEADERS = {
    "Content-Type": "application/json",
}


def send_test_card_notification(
    template_id: str,
    template_variable: dict[str, Any],
    ctx: dict[str, Any],
) -> dict[str, Any]:
    """Send a test template-card notification."""
    if not template_id:
        raise ValueError("template_id must be provided")
    user_email_or_account = _get_ctx_required_value(ctx, "userAccount")
    headers = _build_headers(ctx)

    client = HttpClient(
        endpoint=TEST_CARD_NOTIFICATION_ENDPOINT,
        method="POST",
        headers=headers,
        timeout=30.0,
    )
    template_variable = dict(template_variable or {})
    template_variable["user"] = user_email_or_account
    return client.request(
        json_body={
            "userEmailOrAccount": user_email_or_account,
            "templateId": template_id,
            "templateVariable": template_variable,
        }
    )


def _build_headers(ctx: dict[str, Any]) -> dict[str, str]:
    headers = dict(TEST_CARD_NOTIFICATION_HEADERS)
    for key in ("x-tt-env", "x-use-ppe"):
        value = ctx.get(key)
        if value:
            headers[key] = str(value)
    return headers


def _get_ctx_required_value(ctx: dict[str, Any], key: str) -> str:
    if not isinstance(ctx, dict) or not ctx.get(key):
        raise ValueError(f"ctx.{key} must be provided")
    return str(ctx[key])
