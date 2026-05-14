import json
import math
from typing import Any

import pyarrow as pa

_ARROW_TYPE_ALIASES = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "str": pa.string(),
    "string": pa.string(),
    "binary": pa.binary(),
}
_NULL_LIKE_STRINGS = {"", "none", "null", "nan"}
_MAX_NULL_LIKE_STRING_LEN = 16


def parse_arrow_type(value: str | pa.DataType) -> pa.DataType:
    if isinstance(value, pa.DataType):
        return value
    normalized = str(value).strip().lower()
    if normalized in _ARROW_TYPE_ALIASES:
        return _ARROW_TYPE_ALIASES[normalized]
    raise ValueError(f"Unsupported passthrough arrow type: {value}")


def coerce_value_for_arrow_type(value: Any, arrow_type: pa.DataType) -> Any:
    value = _unwrap_value(value)
    if _is_null_like(value):
        return None
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return _coerce_string(value)
    if pa.types.is_integer(arrow_type):
        return _coerce_int(value)
    if pa.types.is_floating(arrow_type):
        return _coerce_float(value)
    if pa.types.is_boolean(arrow_type):
        return _coerce_bool(value)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else _coerce_string(value).encode()
    return value


def _unwrap_value(value: Any) -> Any:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
        value = value.tolist()
    return value


def _is_null_like(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        if len(value) > _MAX_NULL_LIKE_STRING_LEN:
            return False
        return value.strip().lower() in _NULL_LIKE_STRINGS
    try:
        return bool(math.isnan(value))
    except (TypeError, ValueError):
        return False


def _coerce_string(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        try:
            return int(float(value))
        except (TypeError, ValueError, OverflowError):
            return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
        return None
    return bool(value)
