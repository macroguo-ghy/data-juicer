from __future__ import annotations

import importlib

_ANALYSIS_MODULES = {
    "ColumnWiseAnalysis": ".column_wise_analysis",
    "CorrelationAnalysis": ".correlation_analysis",
    "DiversityAnalysis": ".diversity_analysis",
    "OverallAnalysis": ".overall_analysis",
}

__all__ = [
    "ColumnWiseAnalysis",
    "CorrelationAnalysis",
    "DiversityAnalysis",
    "OverallAnalysis",
]


def __getattr__(name: str):
    module_name = _ANALYSIS_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    value = getattr(importlib.import_module(module_name, __name__), name)
    globals()[name] = value
    return value
