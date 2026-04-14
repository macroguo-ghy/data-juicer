from __future__ import annotations

from importlib import import_module

_LAZY_ATTRS = {
    "Adapter": (".adapter", "Adapter"),
    "Analyzer": (".analyzer", "Analyzer"),
    "NestedDataset": (".data", "NestedDataset"),
    "DefaultExecutor": (".executor", "DefaultExecutor"),
    "ExecutorBase": (".executor", "ExecutorBase"),
    "ExecutorFactory": (".executor", "ExecutorFactory"),
    "PartitionedRayExecutor": (".executor", "PartitionedRayExecutor"),
    "RayExecutor": (".executor", "RayExecutor"),
    "Exporter": (".exporter", "Exporter"),
    "Monitor": (".monitor", "Monitor"),
    "RayExporter": (".ray_exporter", "RayExporter"),
    "Tracer": (".tracer", "Tracer"),
}


def __getattr__(name: str):
    if name not in _LAZY_ATTRS:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    module_name, attr_name = _LAZY_ATTRS[name]
    value = getattr(import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


__all__ = list(_LAZY_ATTRS)
