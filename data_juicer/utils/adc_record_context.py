from __future__ import annotations

from typing import Any

ADC_LOG_ID_FIELD = "__adc_log_id"
TT_LOG_ID_HEADER = "x-tt-logid"


def add_record_log_id_header(headers: dict[str, str], sample: dict[str, Any] | None) -> dict[str, str]:
    """Add the per-record LogID header when the sample carries one."""
    if not isinstance(sample, dict):
        return headers
    log_id = sample.get(ADC_LOG_ID_FIELD)
    if log_id not in (None, ""):
        headers[TT_LOG_ID_HEADER] = str(log_id)
    return headers
