from __future__ import annotations

import importlib
import pkgutil
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from .base_op import (
    ATTRIBUTION_FILTERS,
    NON_STATS_FILTERS,
    OPERATORS,
    TAGGING_OPS,
    UNFORKABLE,
    Aggregator,
    Deduplicator,
    Filter,
    Grouper,
    Mapper,
    Pipeline,
    Selector,
)
from .load import load_ops

_OPS_ROOT = Path(__file__).resolve().parent
_OP_PACKAGES = (
    "aggregator",
    "deduplicator",
    "filter",
    "grouper",
    "mapper",
    "pipeline",
    "selector",
)
_LOADED_IMPORT_PATHS = set()
_LAZY_ATTRS = {
    "OPEnvManager": (".op_env", "OPEnvManager"),
    "OPEnvSpec": (".op_env", "OPEnvSpec"),
    "analyze_lazy_loaded_requirements": (
        ".op_env",
        "analyze_lazy_loaded_requirements",
    ),
    "analyze_lazy_loaded_requirements_for_code_file": (
        ".op_env",
        "analyze_lazy_loaded_requirements_for_code_file",
    ),
    "op_requirements_to_op_env_spec": (
        ".op_env",
        "op_requirements_to_op_env_spec",
    ),
}


def _import_module(import_path: str) -> None:
    if import_path in _LOADED_IMPORT_PATHS:
        return
    importlib.import_module(import_path)
    _LOADED_IMPORT_PATHS.add(import_path)


@lru_cache(maxsize=1)
def _op_module_index() -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    for package in _OP_PACKAGES:
        package_path = _OPS_ROOT / package
        prefix = f"{__name__}.{package}."
        for module_info in pkgutil.walk_packages([str(package_path)], prefix=prefix):
            if module_info.ispkg:
                continue
            module_name = module_info.name.rsplit(".", 1)[-1]
            if module_name.startswith("_"):
                continue
            index.setdefault(module_name, []).append(module_info.name)
    return index


def _candidate_module_names(op_name: str) -> Iterable[str]:
    yield op_name
    if op_name.endswith("_with_uid"):
        yield op_name[: -len("_with_uid")]


def load_builtin_ops(op_names: Iterable[str] | None = None) -> None:
    """
    Import builtin operator modules on demand.

    If ``op_names`` is omitted, all builtin operators are imported.
    Otherwise only modules whose filenames match the requested operator names
    are loaded. This keeps Ray-only demo setups from importing every optional
    operator dependency during startup.
    """

    if op_names is None:
        for import_paths in _op_module_index().values():
            for import_path in import_paths:
                _import_module(import_path)
        return

    for op_name in op_names:
        if op_name in OPERATORS.modules:
            continue
        for candidate in _candidate_module_names(op_name):
            for import_path in _op_module_index().get(candidate, []):
                _import_module(import_path)
            if op_name in OPERATORS.modules:
                break


def __getattr__(name: str):
    if name not in _LAZY_ATTRS:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    module_name, attr_name = _LAZY_ATTRS[name]
    value = getattr(importlib.import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


__all__ = [
    "load_builtin_ops",
    "load_ops",
    "Filter",
    "Mapper",
    "Deduplicator",
    "Selector",
    "Grouper",
    "Aggregator",
    "UNFORKABLE",
    "NON_STATS_FILTERS",
    "OPERATORS",
    "TAGGING_OPS",
    "ATTRIBUTION_FILTERS",
    "Pipeline",
    "OPEnvSpec",
    "op_requirements_to_op_env_spec",
    "OPEnvManager",
    "analyze_lazy_loaded_requirements",
    "analyze_lazy_loaded_requirements_for_code_file",
]
