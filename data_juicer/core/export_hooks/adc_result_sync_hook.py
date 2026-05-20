from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from data_juicer.utils.http_utils import HttpClient

EXPORT_TO_SHEET_PATH = "/openapi/dataset/export-to-sheet"
SYNC_EVAL_SET_FROM_LANCE_PATH = "/openapi/eval/evalsets/sync-from-lance"


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
        self.timeout = float(hook_cfg.get("timeout", 30.0))

    def run(self) -> None:
        if self.export_cfg.get("target") != "magnus":
            raise ValueError("adc_result_sync after_export_hook only supports export.target=magnus")

        sync_cfg = self._require_dict(self.hook_cfg.get("sync"), "after_export_hook.sync")
        targets = sync_cfg.get("targets")
        if not isinstance(targets, list):
            raise ValueError("after_export_hook.sync.targets must be a list")

        for target in targets:
            if not isinstance(target, dict):
                raise ValueError("after_export_hook.sync.targets item must be a dictionary")
            if target.get("enabled", True) is False:
                continue
            try:
                self._sync_target(target)
            except Exception as exc:
                logger.warning("After export hook target failed [{}]: {}", target.get("type"), exc)
                if self.hook_cfg.get("fail_on_error", False):
                    raise ValueError(f"After export hook target failed: {target.get('type')}") from exc

    def _sync_target(self, target: dict[str, Any]) -> None:
        target_type = target.get("type")
        if target_type == "sheet":
            self._sync_sheet(target)
            return
        if target_type == "eval_set":
            self._sync_eval_set(target)
            return
        raise ValueError(f"Unsupported after_export_hook sync target type: {target_type}")

    def _sync_sheet(self, target: dict[str, Any]) -> None:
        payload = {
            "datasourceType": "lance",
            "datasourceName": self.table_name,
        }
        if target.get("sheetTitle") is not None:
            payload["sheetTitle"] = target.get("sheetTitle")
        self._post(EXPORT_TO_SHEET_PATH, payload, headers=self._base_headers())

    def _sync_eval_set(self, target: dict[str, Any]) -> None:
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
        headers = self._base_headers()
        headers["space-id"] = str(self._require_value(space_id, "ctx.spaceId or target.spaceId"))
        self._post(SYNC_EVAL_SET_FROM_LANCE_PATH, payload, headers=headers)

    def _post(self, path: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        client = HttpClient(
            endpoint=f"{self.api_base}/{path.lstrip('/')}",
            method="POST",
            headers=headers,
            timeout=self.timeout,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"ADC result sync request failed: {result['error']}")
        self._validate_openapi_result(result.get("data"))
        return result

    def _base_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Account": self.user_account,
        }
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
