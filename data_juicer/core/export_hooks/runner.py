from __future__ import annotations

from typing import Any

from loguru import logger

from data_juicer.core.export_hooks.adc_result_sync_hook import AdcResultSyncHook
from data_juicer.core.io_utils import namespace_to_plain_dict


def run_after_export_hook(export_cfg: Any) -> None:
    export_cfg = namespace_to_plain_dict(export_cfg or {})
    hook_cfg = export_cfg.get("after_export_hook") or {}
    if not hook_cfg or hook_cfg.get("enabled") is not True:
        return

    hook_type = hook_cfg.get("type")
    fail_on_error = bool(hook_cfg.get("fail_on_error", False))
    try:
        if hook_type == "adc_result_sync":
            AdcResultSyncHook(export_cfg, hook_cfg).run()
            return
        raise ValueError(f"Unsupported after_export_hook type: {hook_type}")
    except Exception as exc:
        if fail_on_error:
            raise
        logger.warning("Failed to run after_export_hook [{}]: {}", hook_type, exc)
