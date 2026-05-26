from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from data_juicer.utils.http_utils import HttpClient

EXPORT_TO_SHEET_PATH = "/openapi/dataset/export-to-sheet"
SYNC_EVAL_SET_FROM_LANCE_PATH = "/openapi/eval/evalsets/sync-from-lance"
RESULT_SYNC_NOTIFICATION_PATH = "/openapi/lark/message/template-card/send-to-user"
RESULT_SYNC_NOTIFICATION_TEMPLATE_ID = "AAqtBYKVfi75b"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_STATUS_CODES = (420, 429, 500, 502, 503, 504)


class AdcResultSyncHook:
    """Sync final Magnus Lance export results to ADC external targets."""

    def __init__(self, export_cfg: dict[str, Any], hook_cfg: dict[str, Any]):
        self.export_cfg = export_cfg
        self.hook_cfg = hook_cfg
        self.ctx = self._require_dict(hook_cfg.get("ctx"), "after_export_hook.ctx")
        self.table_name = self._require_value(export_cfg.get("table_name"), "export.table_name")
        self.catalog, self.namespace_name, self.short_table_name = self._parse_table_name(self.table_name)
        self.api_base = str(self._require_value(self.ctx.get("apiBase"), "ctx.apiBase")).rstrip("/")
        self.user_account = str(self._require_value(self.ctx.get("userAccount"), "ctx.userAccount"))
        self.timeout = float(hook_cfg.get("timeout", DEFAULT_TIMEOUT_SECONDS))
        self.retry_attempts = int(hook_cfg.get("retry_attempts", DEFAULT_RETRY_ATTEMPTS))
        if self.retry_attempts < 0:
            raise ValueError("after_export_hook.retry_attempts must be non-negative")

    def run(self) -> None:
        if self.export_cfg.get("target") != "magnus":
            raise ValueError("adc_result_sync after_export_hook only supports export.target=magnus")

        sync_cfg = self._require_dict(self.hook_cfg.get("sync"), "after_export_hook.sync")
        targets = sync_cfg.get("targets")
        if not isinstance(targets, list):
            raise ValueError("after_export_hook.sync.targets must be a list")

        sync_results = {}
        failed_target_types = []
        for target in targets:
            if not isinstance(target, dict):
                raise ValueError("after_export_hook.sync.targets item must be a dictionary")
            if target.get("enabled", True) is False:
                continue
            target_type = target.get("type")
            try:
                sync_results[target_type] = {
                    "status": STATUS_SUCCESS,
                    "data": self._sync_target(target),
                }
            except Exception as exc:
                logger.warning("After export hook target failed [{}]: {}", target_type, exc)
                failed_target_types.append(target_type)
                sync_results[target_type] = {
                    "status": STATUS_FAILED,
                    "data": {},
                }

        self._send_result_sync_notification(sync_results)
        if failed_target_types and self.hook_cfg.get("fail_on_error", False):
            raise ValueError(f"After export hook target failed: {failed_target_types[0]}")

    def _sync_target(self, target: dict[str, Any]) -> dict[str, Any]:
        target_type = target.get("type")
        if target_type == "sheet":
            return self._sync_sheet(target)
        if target_type == "eval_set":
            return self._sync_eval_set(target)
        raise ValueError(f"Unsupported after_export_hook sync target type: {target_type}")

    def _sync_sheet(self, target: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "datasourceType": "lance",
            "datasourceName": self.table_name,
        }
        if target.get("sheetTitle") is not None:
            payload["sheetTitle"] = target.get("sheetTitle")
        result = self._post(
            EXPORT_TO_SHEET_PATH,
            payload,
            headers=self._base_headers(include_space_id=True),
        )
        return self._openapi_data(result)

    def _sync_eval_set(self, target: dict[str, Any]) -> dict[str, Any]:
        target_cfg = self._require_dict(target.get("target"), "eval_set.target")
        payload = {
            "source": {
                "catalog": self.catalog,
                "namespaceName": self.namespace_name,
                "tableName": self.short_table_name,
            },
            "target": copy.deepcopy(target_cfg),
        }
        if target.get("selectedFields") is not None:
            payload["source"]["selectedFields"] = target.get("selectedFields")
        if target.get("fieldMapping") is not None:
            payload["fieldMapping"] = target.get("fieldMapping")

        space_id = target_cfg.get("spaceId", self.ctx.get("spaceId"))
        headers = self._base_headers(include_space_id=True)
        headers["space-id"] = str(self._require_value(space_id, "ctx.spaceId or target.spaceId"))
        result = self._post(SYNC_EVAL_SET_FROM_LANCE_PATH, payload, headers=headers)
        data = self._openapi_data(result)
        data.setdefault("spaceId", space_id)
        return data

    def _send_result_sync_notification(self, sync_results: dict[str, dict[str, Any]]) -> None:
        try:
            self._post(
                RESULT_SYNC_NOTIFICATION_PATH,
                {
                    "userEmailOrAccount": self.user_account,
                    "templateId": RESULT_SYNC_NOTIFICATION_TEMPLATE_ID,
                    "templateVariable": self._build_notification_variable(sync_results),
                },
                headers=self._base_headers(),
            )
        except Exception as exc:
            logger.warning("Failed to send after export result sync notification: {}", exc)

    def _build_notification_variable(self, sync_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
        sheet_result = sync_results.get("sheet", {})
        eval_set_result = sync_results.get("eval_set", {})
        sheet_data = sheet_result.get("data") or {}
        eval_set_data = eval_set_result.get("data") or {}
        sheet_status = sheet_result.get("status", STATUS_SKIPPED)
        eval_set_status = eval_set_result.get("status", STATUS_SKIPPED)
        return {
            "title": self._notification_title([result.get("status") for result in sync_results.values()]),
            "sheetStatus": sheet_status,
            "sheetUrl": sheet_data.get("sheetUrl") or sheet_data.get("url") or "",
            "evalSetStatus": eval_set_status,
            "evalSetId": eval_set_data.get("evalSetId") or eval_set_data.get("id") or "",
            "spaceId": eval_set_data.get("spaceId") or self.ctx.get("spaceId") or "",
        }

    @staticmethod
    def _notification_title(statuses: list[str]) -> str:
        statuses = set(statuses)
        if STATUS_FAILED not in statuses:
            return "数据合成结果同步完成"
        if STATUS_SUCCESS in statuses:
            return "数据合成结果同步部分失败"
        return "数据合成结果同步失败"

    def _post(self, path: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        client = HttpClient(
            endpoint=f"{self.api_base}/{path.lstrip('/')}",
            method="POST",
            headers=headers,
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            retry_status_codes=DEFAULT_RETRY_STATUS_CODES,
            retry_on_timeout=False,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"ADC result sync request failed: {result['error']}")
        self._validate_openapi_result(result.get("data"))
        return result

    @staticmethod
    def _openapi_data(result: dict[str, Any]) -> dict[str, Any]:
        envelope = result.get("data")
        if not isinstance(envelope, dict):
            return {}
        data = envelope.get("data")
        return data if isinstance(data, dict) else {}

    def _base_headers(self, include_space_id: bool = False) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Account": self.user_account,
        }
        if include_space_id:
            headers["space-id"] = str(self._require_value(self.ctx.get("spaceId"), "ctx.spaceId"))
        for key in ("x-tt-env", "x-use-ppe"):
            value = self.ctx.get(key)
            if value:
                headers[key] = str(value)
        return headers

    @staticmethod
    def _validate_openapi_result(data: Any) -> None:
        if not isinstance(data, dict) or "code" not in data:
            return
        if data.get("code") != 0:
            message = data.get("message") or data.get("msg") or ""
            raise ValueError(f"ADC result sync business failed: code={data.get('code')}, message={message}")

    @staticmethod
    def _parse_table_name(table_name: str) -> tuple[str, str, str]:
        parts = table_name.split(".")
        if len(parts) != 3 or not all(parts):
            raise ValueError("export.table_name must be in catalog.namespace.table format")
        return parts[0], parts[1], parts[2]

    @staticmethod
    def _require_dict(value: Any, name: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError(f"{name} must be a dictionary")
        return value

    @staticmethod
    def _require_value(value: Any, name: str) -> Any:
        if value in (None, ""):
            raise ValueError(f"{name} must be provided")
        return value
